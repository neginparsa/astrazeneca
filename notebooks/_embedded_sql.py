# Databricks notebook source
# Seed + dashboard SQL embedded for Databricks Git folders missing sql/ directory.

import importlib.util
import os
from pathlib import Path

EMBEDDED_SQL = {
    "seed_astrazeneca.sql": "-- AstraZeneca synthetic lakehouse seed (catalog: main)\n-- Used by notebooks/11_seed_lakehouse_data.sql and .py\n\nCREATE SCHEMA IF NOT EXISTS {catalog}.bronze;\nCREATE SCHEMA IF NOT EXISTS {catalog}.silver;\nCREATE SCHEMA IF NOT EXISTS {catalog}.gold;\nCREATE SCHEMA IF NOT EXISTS {catalog}.ml;\n\nCREATE OR REPLACE TABLE {catalog}.gold.az_product_dim AS\nSELECT * FROM VALUES\n  ('AZ-TAG-40',  'Tagrisso',   'Oncology',       'osimertinib',           14500.00),\n  ('AZ-IMF-500', 'Imfinzi',    'Oncology',       'durvalumab',            8900.00),\n  ('AZ-LYN-150', 'Lynparza',   'Oncology',       'olaparib',              7800.00),\n  ('AZ-CAL-100', 'Calquence',  'Oncology',       'acalabrutinib',         9200.00),\n  ('AZ-FAR-10',  'Farxiga',    'CVRM',           'dapagliflozin',         520.00),\n  ('AZ-BRI-90',  'Brilinta',   'CVRM',           'ticagrelor',            380.00),\n  ('AZ-FAS-30',  'Fasenra',    'Respiratory',    'benralizumab',          4200.00),\n  ('AZ-TEZ-210', 'Tezspire',   'Respiratory',    'tezepelumab',           5100.00),\n  ('AZ-SYM-160', 'Symbicort',  'Respiratory',    'budesonide-formoterol', 285.00),\n  ('AZ-ULT-300', 'Ultomiris',  'Rare Disease',   'ravulizumab',           12500.00)\nAS t(therapy_code, brand_name, therapeutic_area, molecule, avg_wac_per_fill);\n\nCREATE OR REPLACE TABLE {catalog}.bronze.claims_raw AS\nWITH patients AS (\n  SELECT CONCAT('AZ-PAT-', LPAD(CAST(id AS STRING), 5, '0')) AS patient_id\n  FROM (SELECT EXPLODE(SEQUENCE(1, 1500)) AS id)\n),\nfills AS (SELECT EXPLODE(SEQUENCE(0, 11)) AS fill_num),\nproducts AS (SELECT * FROM {catalog}.gold.az_product_dim)\nSELECT\n  UUID() AS claim_id, p.patient_id,\n  CAST(1000000000 + (HASH(p.patient_id) % 900000000) AS STRING) AS provider_npi,\n  CONCAT('50242-', LPAD(CAST(1000 + (HASH(pr.therapy_code) % 8999) AS STRING), 4, '0')) AS ndc,\n  DATE_ADD(DATE('2024-06-01'), CAST(ABS(HASH(p.patient_id, f.fill_num)) % 570 AS INT)) AS fill_date,\n  CASE\n    WHEN pr.therapeutic_area = 'Oncology' THEN 28\n    WHEN pr.therapeutic_area = 'Rare Disease' THEN 14\n    WHEN pr.brand_name = 'Farxiga' THEN 30\n    ELSE CASE WHEN ABS(HASH(f.fill_num)) % 3 = 0 THEN 90 ELSE 30 END\n  END AS days_supply,\n  CAST(pr.avg_wac_per_fill * (0.85 + (ABS(HASH(p.patient_id, f.fill_num)) % 30) / 100.0) AS DECIMAL(12, 2)) AS paid_amount,\n  pr.therapy_code, pr.brand_name, pr.therapeutic_area, pr.molecule,\n  'az/specialty_pharmacy/claims' AS source_file, CURRENT_TIMESTAMP() AS ingested_at\nFROM patients p CROSS JOIN fills f\nJOIN products pr ON ABS(HASH(p.patient_id, f.fill_num)) % 10 = ABS(HASH(pr.therapy_code)) % 10;\n\nCREATE OR REPLACE TABLE {catalog}.bronze.specialty_rx_raw AS\nWITH patients AS (\n  SELECT CONCAT('AZ-PAT-', LPAD(CAST(id AS STRING), 5, '0')) AS patient_id\n  FROM (SELECT EXPLODE(SEQUENCE(1, 1500)) AS id)\n),\nships AS (SELECT EXPLODE(SEQUENCE(0, 5)) AS ship_num)\nSELECT UUID() AS rx_id, p.patient_id, pr.therapy_code, pr.brand_name,\n  DATE_ADD(CURRENT_DATE(), -CAST(ABS(HASH(p.patient_id, s.ship_num)) % 200 AS INT)) AS ship_date,\n  1 + (ABS(HASH(s.ship_num)) % 2) AS quantity,\n  CASE ABS(HASH(p.patient_id, s.ship_num)) % 5\n    WHEN 0 THEN 'SHIPPED' WHEN 1 THEN 'AZ_ME_HUB_PROCESSING' WHEN 2 THEN 'DELAYED'\n    WHEN 3 THEN 'PRIOR_AUTH_HOLD' ELSE 'SHIPPED' END AS hub_status,\n  'az/ace_hub/specialty_rx' AS source_file, CURRENT_TIMESTAMP() AS ingested_at\nFROM patients p CROSS JOIN ships s\nJOIN {catalog}.gold.az_product_dim pr ON ABS(HASH(p.patient_id)) % 10 = ABS(HASH(pr.therapy_code)) % 10\nWHERE ABS(HASH(p.patient_id, s.ship_num)) % 4 != 0;\n\nCREATE OR REPLACE TABLE {catalog}.bronze.prior_auth_raw AS\nWITH patients AS (\n  SELECT CONCAT('AZ-PAT-', LPAD(CAST(id AS STRING), 5, '0')) AS patient_id\n  FROM (SELECT EXPLODE(SEQUENCE(1, 1500)) AS id) WHERE id % 2 = 0\n)\nSELECT UUID() AS auth_id, patient_id, pr.therapy_code, pr.brand_name,\n  CASE ABS(HASH(patient_id)) % 6\n    WHEN 0 THEN 'DENIED' WHEN 1 THEN 'APPROVED' WHEN 2 THEN 'PENDING'\n    WHEN 3 THEN 'APPROVED' WHEN 4 THEN 'APPEAL_APPROVED' ELSE 'DENIED' END AS status,\n  TIMESTAMP('2024-08-01') + (ABS(HASH(patient_id)) % 400) * INTERVAL 1 DAY AS submitted_at,\n  TIMESTAMP('2024-08-10') + (ABS(HASH(patient_id)) % 400) * INTERVAL 1 DAY AS decided_at,\n  'az/market_access/prior_auth' AS source_file, CURRENT_TIMESTAMP() AS ingested_at\nFROM patients JOIN {catalog}.gold.az_product_dim pr ON ABS(HASH(patient_id)) % 10 = ABS(HASH(pr.therapy_code)) % 10;\n\nCREATE OR REPLACE TABLE {catalog}.bronze.crm_raw AS\nWITH patients AS (\n  SELECT CONCAT('AZ-PAT-', LPAD(CAST(id AS STRING), 5, '0')) AS patient_id\n  FROM (SELECT EXPLODE(SEQUENCE(1, 1500)) AS id) WHERE id % 3 = 0\n),\ntouch AS (SELECT EXPLODE(SEQUENCE(0, 3)) AS touch_num)\nSELECT UUID() AS interaction_id, p.patient_id,\n  CASE ABS(HASH(p.patient_id, t.touch_num)) % 4 WHEN 0 THEN 'NURSE_EDUCATOR' WHEN 1 THEN 'HUB_PHONE' WHEN 2 THEN 'SMS' ELSE 'EMAIL' END AS channel,\n  CASE ABS(HASH(p.patient_id, t.touch_num)) % 5\n    WHEN 0 THEN 'ADHERENCE_COUNSELING' WHEN 1 THEN 'REFILL_REMINDER' WHEN 2 THEN 'NO_ANSWER'\n    WHEN 3 THEN 'PA_SUPPORT' ELSE 'SIDE_EFFECT_TRIAGE' END AS outcome,\n  CURRENT_TIMESTAMP() - (ABS(HASH(p.patient_id, t.touch_num)) % 90) * INTERVAL 1 DAY AS interaction_at,\n  'az/patient_support/crm' AS source_file, CURRENT_TIMESTAMP() AS ingested_at\nFROM patients p CROSS JOIN touch t;\n\nCREATE OR REPLACE TABLE {catalog}.bronze.inventory_raw AS\nSELECT CONCAT('SKU-', therapy_code) AS sku, therapy_code, brand_name,\n  CASE ABS(HASH(therapy_code)) % 4 WHEN 0 THEN 'AZ-MCE-WILMINGTON' WHEN 1 THEN 'AZ-3PL-MEMPHIS' WHEN 2 THEN 'AZ-3PL-PHOENIX' ELSE 'AZ-CMO-IRELAND' END AS site_id,\n  100 + (ABS(HASH(therapy_code)) % 900) AS on_hand, CURRENT_DATE() AS as_of_date,\n  'az/supply_chain/inventory' AS source_file, CURRENT_TIMESTAMP() AS ingested_at\nFROM {catalog}.gold.az_product_dim;\n\nCREATE OR REPLACE TABLE {catalog}.silver.claims_enriched AS\nSELECT claim_id, SHA2(CAST(patient_id AS STRING), 256) AS patient_token,\n  therapy_code, brand_name, therapeutic_area, molecule, fill_date, days_supply, paid_amount,\n  SHA2(CONCAT('npi:', provider_npi), 256) AS provider_token, CURRENT_TIMESTAMP() AS updated_at\nFROM {catalog}.bronze.claims_raw;\n\nCREATE OR REPLACE TABLE {catalog}.silver.patient_events AS\nSELECT SHA2(CONCAT(claim_id, CAST(fill_date AS STRING), 'CLAIM'), 256) AS event_id,\n  SHA2(CAST(patient_id AS STRING), 256) AS patient_token, 'CLAIM_FILL' AS event_type,\n  therapy_code, brand_name, therapeutic_area, fill_date AS event_date,\n  CAST(fill_date AS TIMESTAMP) AS event_ts,\n  MAP('brand', brand_name, 'molecule', molecule, 'paid', CAST(paid_amount AS STRING)) AS detail,\n  'az_specialty_pharmacy' AS source_system, CURRENT_TIMESTAMP() AS updated_at\nFROM {catalog}.bronze.claims_raw;\n\nCREATE OR REPLACE TABLE {catalog}.gold.patient_journey AS\nWITH ranked AS (\n  SELECT patient_token, therapy_code, brand_name, therapeutic_area, fill_date AS last_fill_date,\n    ROW_NUMBER() OVER (PARTITION BY patient_token, therapy_code ORDER BY fill_date DESC) AS rn\n  FROM {catalog}.silver.claims_enriched\n)\nSELECT patient_token, therapy_code, brand_name, therapeutic_area,\n  DATE_SUB(last_fill_date, 400) AS journey_start, last_fill_date,\n  DATEDIFF(CURRENT_DATE(), last_fill_date) AS days_since_last_fill,\n  CAST(ABS(HASH(patient_token)) % 4 AS INT) AS pa_denial_count,\n  CAST(ABS(HASH(patient_token)) % 6 AS INT) AS hub_interactions_30d,\n  DATEDIFF(CURRENT_DATE(), last_fill_date) <= CASE WHEN therapeutic_area = 'Oncology' THEN 35 WHEN therapeutic_area = 'Rare Disease' THEN 21 ELSE 45 END AS on_therapy,\n  CURRENT_TIMESTAMP() AS journey_updated_at\nFROM ranked WHERE rn = 1;\n\nCREATE OR REPLACE TABLE {catalog}.gold.daily_therapy_metrics AS\nSELECT fill_date AS metric_date, therapy_code, brand_name, therapeutic_area,\n  COUNT(DISTINCT patient_token) AS active_patients,\n  CAST(SUM(CASE WHEN ABS(HASH(patient_token, fill_date)) % 25 = 0 THEN 1 ELSE 0 END) AS BIGINT) AS new_starts,\n  CAST(SUM(CASE WHEN ABS(HASH(patient_token, fill_date)) % 30 = 0 THEN 1 ELSE 0 END) AS BIGINT) AS discontinuations,\n  AVG(days_supply) AS avg_days_supply, SUM(paid_amount) AS total_paid_amount, CURRENT_TIMESTAMP() AS computed_at\nFROM {catalog}.silver.claims_enriched\nGROUP BY fill_date, therapy_code, brand_name, therapeutic_area;\n\nCREATE OR REPLACE TABLE {catalog}.gold.daily_franchise_metrics AS\nSELECT metric_date, therapeutic_area, SUM(active_patients) AS active_patients,\n  SUM(new_starts) AS new_starts, SUM(discontinuations) AS discontinuations,\n  SUM(total_paid_amount) AS total_paid_amount, COUNT(DISTINCT brand_name) AS brands_active\nFROM {catalog}.gold.daily_therapy_metrics GROUP BY metric_date, therapeutic_area;\n\nCREATE OR REPLACE TABLE {catalog}.ml.discontinuation_features AS\nSELECT patient_token, therapy_code, brand_name, therapeutic_area, CURRENT_DATE() AS as_of_date,\n  days_since_last_fill, CAST(4 + ABS(HASH(patient_token)) % 10 AS INT) AS fills_last_90d,\n  pa_denial_count AS pa_denials_last_180d, hub_interactions_30d AS crm_outreach_last_30d,\n  28.0 + (ABS(HASH(patient_token)) % 8) AS avg_days_supply_90d,\n  CAST(NOT on_therapy AS INT) AS label_discontinued_within_30d\nFROM {catalog}.gold.patient_journey;\n",
    "dashboard_views.sql": '-- Dashboard-friendly views (catalog: main). Run after QUICKSTART or gold notebooks.\n\nCREATE OR REPLACE VIEW main.gold.v_dashboard_kpis AS\nSELECT\n  SUM(active_patients) AS total_active_patients,\n  ROUND(SUM(total_paid_amount), 2) AS total_paid_amount,\n  COUNT(DISTINCT therapy_code) AS therapy_count,\n  ROUND(AVG(avg_days_supply), 1) AS avg_days_supply,\n  MAX(metric_date) AS latest_metric_date\nFROM main.gold.daily_therapy_metrics\nWHERE metric_date >= date_sub(current_date(), 120);\n\nCREATE OR REPLACE VIEW main.gold.v_dashboard_daily_trend AS\nSELECT\n  metric_date,\n  therapy_code,\n  active_patients,\n  total_paid_amount,\n  avg_days_supply,\n  new_starts,\n  discontinuations\nFROM main.gold.daily_therapy_metrics\nWHERE metric_date >= date_sub(current_date(), 120)\nORDER BY metric_date, therapy_code;\n\nCREATE OR REPLACE VIEW main.gold.v_dashboard_therapy_summary AS\nSELECT\n  therapy_code,\n  SUM(active_patients) AS patient_fills,\n  ROUND(SUM(total_paid_amount), 2) AS paid_amount,\n  ROUND(AVG(avg_days_supply), 1) AS avg_days_supply\nFROM main.gold.daily_therapy_metrics\nWHERE metric_date >= date_sub(current_date(), 120)\nGROUP BY therapy_code;\n\nCREATE OR REPLACE VIEW main.gold.v_dashboard_journey_status AS\nSELECT\n  therapy_code,\n  on_therapy,\n  COUNT(*) AS patient_count,\n  ROUND(AVG(days_since_last_fill), 1) AS avg_days_since_fill,\n  SUM(pa_denial_count) AS pa_denials\nFROM main.gold.patient_journey\nGROUP BY therapy_code, on_therapy;\n\nCREATE OR REPLACE VIEW main.gold.v_dashboard_at_risk AS\nSELECT\n  patient_token,\n  therapy_code,\n  last_fill_date,\n  days_since_last_fill,\n  pa_denial_count,\n  hub_interactions_30d,\n  on_therapy\nFROM main.gold.patient_journey\nWHERE NOT on_therapy OR days_since_last_fill > 30\nORDER BY days_since_last_fill DESC\nLIMIT 200;\n',
}


def _repo_paths(filename: str) -> list[Path]:
    paths: list[Path] = []
    try:
        nb = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        ws_nb = Path("/Workspace" + nb)
        if "/notebooks" in nb:
            paths.append(Path("/Workspace" + nb.rsplit("/notebooks", 1)[0]) / "sql" / filename)
        cur = ws_nb.parent
        for _ in range(6):
            candidate = cur / "sql" / filename
            if candidate not in paths:
                paths.append(candidate)
            cur = cur.parent
    except Exception:
        pass
    cwd = Path(os.getcwd())
    for rel in (
        cwd / "sql" / filename,
        cwd.parent / "sql" / filename,
        Path("../../sql") / filename,
        Path("../sql") / filename,
        Path("sql") / filename,
    ):
        if rel not in paths:
            paths.append(rel)
    return paths


def load_sql_file(filename: str) -> str:
    for p in _repo_paths(filename):
        if p.suffix == ".sql" and p.exists():
            print(f"Loaded {p}")
            return p.read_text(encoding="utf-8")
    if filename in EMBEDDED_SQL:
        print(f"Using embedded SQL for {filename} (sql/ folder not on workspace).")
        return EMBEDDED_SQL[filename]
    raise FileNotFoundError(
        "sql/" + filename + " not found. Use notebooks/11_seed_lakehouse_data.sql on SQL warehouse, "
        "or sync Git: git fetch origin && git reset --hard origin/main"
    )


def run_sql_script(sql_text: str, catalog_name: str, label: str) -> None:
    sql = sql_text.replace("{catalog}", catalog_name).replace("main.", f"{catalog_name}.")
    statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]
    for i, stmt in enumerate(statements, 1):
        spark.sql(stmt)
        head = stmt.split("\n")[0][:70]
        print(f"[{label} {i}/{len(statements)}] OK: {head}...")
