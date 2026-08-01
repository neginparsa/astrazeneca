-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 11 — AstraZeneca bulk seed (SQL warehouse)
-- MAGIC ~1,500 patients · ~18,000 claims · **all SQL inline** (no external files).

-- COMMAND ----------

USE CATALOG main;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS main.silver;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS main.gold;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS main.ml;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.gold.az_product_dim AS
SELECT * FROM VALUES
  ('AZ-TAG-40',  'Tagrisso',   'Oncology',       'osimertinib',           14500.00),
  ('AZ-IMF-500', 'Imfinzi',    'Oncology',       'durvalumab',            8900.00),
  ('AZ-LYN-150', 'Lynparza',   'Oncology',       'olaparib',              7800.00),
  ('AZ-CAL-100', 'Calquence',  'Oncology',       'acalabrutinib',         9200.00),
  ('AZ-FAR-10',  'Farxiga',    'CVRM',           'dapagliflozin',         520.00),
  ('AZ-BRI-90',  'Brilinta',   'CVRM',           'ticagrelor',            380.00),
  ('AZ-FAS-30',  'Fasenra',    'Respiratory',    'benralizumab',          4200.00),
  ('AZ-TEZ-210', 'Tezspire',   'Respiratory',    'tezepelumab',           5100.00),
  ('AZ-SYM-160', 'Symbicort',  'Respiratory',    'budesonide-formoterol', 285.00),
  ('AZ-ULT-300', 'Ultomiris',  'Rare Disease',   'ravulizumab',           12500.00)
AS t(therapy_code, brand_name, therapeutic_area, molecule, avg_wac_per_fill);

-- COMMAND ----------

CREATE OR REPLACE TABLE main.bronze.claims_raw AS
WITH patients AS (
  SELECT CONCAT('AZ-PAT-', LPAD(CAST(id AS STRING), 5, '0')) AS patient_id
  FROM (SELECT EXPLODE(SEQUENCE(1, 1500)) AS id)
),
fills AS (SELECT EXPLODE(SEQUENCE(0, 11)) AS fill_num),
products AS (SELECT * FROM main.gold.az_product_dim)
SELECT
  UUID() AS claim_id, p.patient_id,
  CAST(1000000000 + (HASH(p.patient_id) % 900000000) AS STRING) AS provider_npi,
  CONCAT('50242-', LPAD(CAST(1000 + (HASH(pr.therapy_code) % 8999) AS STRING), 4, '0')) AS ndc,
  DATE_ADD(DATE('2024-06-01'), CAST(ABS(HASH(p.patient_id, f.fill_num)) % 570 AS INT)) AS fill_date,
  CASE
    WHEN pr.therapeutic_area = 'Oncology' THEN 28
    WHEN pr.therapeutic_area = 'Rare Disease' THEN 14
    WHEN pr.brand_name = 'Farxiga' THEN 30
    ELSE CASE WHEN ABS(HASH(f.fill_num)) % 3 = 0 THEN 90 ELSE 30 END
  END AS days_supply,
  CAST(pr.avg_wac_per_fill * (0.85 + (ABS(HASH(p.patient_id, f.fill_num)) % 30) / 100.0) AS DECIMAL(12, 2)) AS paid_amount,
  pr.therapy_code, pr.brand_name, pr.therapeutic_area, pr.molecule,
  'az/specialty_pharmacy/claims' AS source_file, CURRENT_TIMESTAMP() AS ingested_at
FROM patients p CROSS JOIN fills f
JOIN products pr ON ABS(HASH(p.patient_id, f.fill_num)) % 10 = ABS(HASH(pr.therapy_code)) % 10;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.bronze.specialty_rx_raw AS
WITH patients AS (
  SELECT CONCAT('AZ-PAT-', LPAD(CAST(id AS STRING), 5, '0')) AS patient_id
  FROM (SELECT EXPLODE(SEQUENCE(1, 1500)) AS id)
),
ships AS (SELECT EXPLODE(SEQUENCE(0, 5)) AS ship_num)
SELECT UUID() AS rx_id, p.patient_id, pr.therapy_code, pr.brand_name,
  DATE_ADD(CURRENT_DATE(), -CAST(ABS(HASH(p.patient_id, s.ship_num)) % 200 AS INT)) AS ship_date,
  1 + (ABS(HASH(s.ship_num)) % 2) AS quantity,
  CASE ABS(HASH(p.patient_id, s.ship_num)) % 5
    WHEN 0 THEN 'SHIPPED' WHEN 1 THEN 'AZ_ME_HUB_PROCESSING' WHEN 2 THEN 'DELAYED'
    WHEN 3 THEN 'PRIOR_AUTH_HOLD' ELSE 'SHIPPED' END AS hub_status,
  'az/ace_hub/specialty_rx' AS source_file, CURRENT_TIMESTAMP() AS ingested_at
FROM patients p CROSS JOIN ships s
JOIN main.gold.az_product_dim pr ON ABS(HASH(p.patient_id)) % 10 = ABS(HASH(pr.therapy_code)) % 10
WHERE ABS(HASH(p.patient_id, s.ship_num)) % 4 != 0;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.bronze.prior_auth_raw AS
WITH patients AS (
  SELECT CONCAT('AZ-PAT-', LPAD(CAST(id AS STRING), 5, '0')) AS patient_id
  FROM (SELECT EXPLODE(SEQUENCE(1, 1500)) AS id) WHERE id % 2 = 0
)
SELECT UUID() AS auth_id, patient_id, pr.therapy_code, pr.brand_name,
  CASE ABS(HASH(patient_id)) % 6
    WHEN 0 THEN 'DENIED' WHEN 1 THEN 'APPROVED' WHEN 2 THEN 'PENDING'
    WHEN 3 THEN 'APPROVED' WHEN 4 THEN 'APPEAL_APPROVED' ELSE 'DENIED' END AS status,
  TIMESTAMP('2024-08-01') + (ABS(HASH(patient_id)) % 400) * INTERVAL 1 DAY AS submitted_at,
  TIMESTAMP('2024-08-10') + (ABS(HASH(patient_id)) % 400) * INTERVAL 1 DAY AS decided_at,
  'az/market_access/prior_auth' AS source_file, CURRENT_TIMESTAMP() AS ingested_at
FROM patients JOIN main.gold.az_product_dim pr ON ABS(HASH(patient_id)) % 10 = ABS(HASH(pr.therapy_code)) % 10;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.bronze.crm_raw AS
WITH patients AS (
  SELECT CONCAT('AZ-PAT-', LPAD(CAST(id AS STRING), 5, '0')) AS patient_id
  FROM (SELECT EXPLODE(SEQUENCE(1, 1500)) AS id) WHERE id % 3 = 0
),
touch AS (SELECT EXPLODE(SEQUENCE(0, 3)) AS touch_num)
SELECT UUID() AS interaction_id, p.patient_id,
  CASE ABS(HASH(p.patient_id, t.touch_num)) % 4 WHEN 0 THEN 'NURSE_EDUCATOR' WHEN 1 THEN 'HUB_PHONE' WHEN 2 THEN 'SMS' ELSE 'EMAIL' END AS channel,
  CASE ABS(HASH(p.patient_id, t.touch_num)) % 5
    WHEN 0 THEN 'ADHERENCE_COUNSELING' WHEN 1 THEN 'REFILL_REMINDER' WHEN 2 THEN 'NO_ANSWER'
    WHEN 3 THEN 'PA_SUPPORT' ELSE 'SIDE_EFFECT_TRIAGE' END AS outcome,
  CURRENT_TIMESTAMP() - (ABS(HASH(p.patient_id, t.touch_num)) % 90) * INTERVAL 1 DAY AS interaction_at,
  'az/patient_support/crm' AS source_file, CURRENT_TIMESTAMP() AS ingested_at
FROM patients p CROSS JOIN touch t;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.bronze.inventory_raw AS
SELECT CONCAT('SKU-', therapy_code) AS sku, therapy_code, brand_name,
  CASE ABS(HASH(therapy_code)) % 4 WHEN 0 THEN 'AZ-MCE-WILMINGTON' WHEN 1 THEN 'AZ-3PL-MEMPHIS' WHEN 2 THEN 'AZ-3PL-PHOENIX' ELSE 'AZ-CMO-IRELAND' END AS site_id,
  100 + (ABS(HASH(therapy_code)) % 900) AS on_hand, CURRENT_DATE() AS as_of_date,
  'az/supply_chain/inventory' AS source_file, CURRENT_TIMESTAMP() AS ingested_at
FROM main.gold.az_product_dim;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.silver.claims_enriched AS
SELECT claim_id, SHA2(CAST(patient_id AS STRING), 256) AS patient_token,
  therapy_code, brand_name, therapeutic_area, molecule, fill_date, days_supply, paid_amount,
  SHA2(CONCAT('npi:', provider_npi), 256) AS provider_token, CURRENT_TIMESTAMP() AS updated_at
FROM main.bronze.claims_raw;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.silver.patient_events AS
SELECT SHA2(CONCAT(claim_id, CAST(fill_date AS STRING), 'CLAIM'), 256) AS event_id,
  SHA2(CAST(patient_id AS STRING), 256) AS patient_token, 'CLAIM_FILL' AS event_type,
  therapy_code, brand_name, therapeutic_area, fill_date AS event_date,
  CAST(fill_date AS TIMESTAMP) AS event_ts,
  MAP('brand', brand_name, 'molecule', molecule, 'paid', CAST(paid_amount AS STRING)) AS detail,
  'az_specialty_pharmacy' AS source_system, CURRENT_TIMESTAMP() AS updated_at
FROM main.bronze.claims_raw;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.gold.patient_journey AS
WITH ranked AS (
  SELECT patient_token, therapy_code, brand_name, therapeutic_area, fill_date AS last_fill_date,
    ROW_NUMBER() OVER (PARTITION BY patient_token, therapy_code ORDER BY fill_date DESC) AS rn
  FROM main.silver.claims_enriched
)
SELECT patient_token, therapy_code, brand_name, therapeutic_area,
  DATE_SUB(last_fill_date, 400) AS journey_start, last_fill_date,
  DATEDIFF(CURRENT_DATE(), last_fill_date) AS days_since_last_fill,
  CAST(ABS(HASH(patient_token)) % 4 AS INT) AS pa_denial_count,
  CAST(ABS(HASH(patient_token)) % 6 AS INT) AS hub_interactions_30d,
  DATEDIFF(CURRENT_DATE(), last_fill_date) <= CASE WHEN therapeutic_area = 'Oncology' THEN 35 WHEN therapeutic_area = 'Rare Disease' THEN 21 ELSE 45 END AS on_therapy,
  CURRENT_TIMESTAMP() AS journey_updated_at
FROM ranked WHERE rn = 1;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.gold.daily_therapy_metrics AS
SELECT fill_date AS metric_date, therapy_code, brand_name, therapeutic_area,
  COUNT(DISTINCT patient_token) AS active_patients,
  CAST(SUM(CASE WHEN ABS(HASH(patient_token, fill_date)) % 25 = 0 THEN 1 ELSE 0 END) AS BIGINT) AS new_starts,
  CAST(SUM(CASE WHEN ABS(HASH(patient_token, fill_date)) % 30 = 0 THEN 1 ELSE 0 END) AS BIGINT) AS discontinuations,
  AVG(days_supply) AS avg_days_supply, SUM(paid_amount) AS total_paid_amount, CURRENT_TIMESTAMP() AS computed_at
FROM main.silver.claims_enriched
GROUP BY fill_date, therapy_code, brand_name, therapeutic_area;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.gold.daily_franchise_metrics AS
SELECT metric_date, therapeutic_area, SUM(active_patients) AS active_patients,
  SUM(new_starts) AS new_starts, SUM(discontinuations) AS discontinuations,
  SUM(total_paid_amount) AS total_paid_amount, COUNT(DISTINCT brand_name) AS brands_active
FROM main.gold.daily_therapy_metrics GROUP BY metric_date, therapeutic_area;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.ml.discontinuation_features AS
SELECT patient_token, therapy_code, brand_name, therapeutic_area, CURRENT_DATE() AS as_of_date,
  days_since_last_fill, CAST(4 + ABS(HASH(patient_token)) % 10 AS INT) AS fills_last_90d,
  pa_denial_count AS pa_denials_last_180d, hub_interactions_30d AS crm_outreach_last_30d,
  28.0 + (ABS(HASH(patient_token)) % 8) AS avg_days_supply_90d,
  CAST(NOT on_therapy AS INT) AS label_discontinued_within_30d
FROM main.gold.patient_journey;

-- COMMAND ----------

SELECT 'bronze.claims_raw' AS tbl, COUNT(*) AS rows FROM main.bronze.claims_raw
UNION ALL SELECT 'gold.daily_therapy_metrics', COUNT(*) FROM main.gold.daily_therapy_metrics
UNION ALL SELECT 'gold.patient_journey', COUNT(*) FROM main.gold.patient_journey;

-- COMMAND ----------

SELECT brand_name, therapeutic_area, SUM(active_patients) AS fills, ROUND(SUM(total_paid_amount), 0) AS paid
FROM main.gold.daily_therapy_metrics
GROUP BY brand_name, therapeutic_area
ORDER BY paid DESC;
