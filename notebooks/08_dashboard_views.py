# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Dashboard views
# MAGIC Run after **QUICKSTART** so AI/BI dashboards have data to query.
# MAGIC
# MAGIC SQL is embedded below (works in Git folders without file-path issues).

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog")

# COMMAND ----------

import os
from pathlib import Path

catalog = dbutils.widgets.get("catalog").strip() or "main"
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

def _repo_sql_paths() -> list[Path]:
    paths: list[Path] = []
    try:
        nb_path = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        # e.g. /Users/you/astra/notebooks/08_dashboard_views
        repo_root = Path("/Workspace" + nb_path.rsplit("/notebooks", 1)[0])
        paths.append(repo_root / "sql" / "dashboard_views.sql")
    except Exception:
        pass
    paths.extend(
        [
            Path("../../sql/dashboard_views.sql"),
            Path("../sql/dashboard_views.sql"),
            Path("sql/dashboard_views.sql"),
        ]
    )
    return paths


def load_sql() -> str:
    for p in _repo_sql_paths():
        if p.exists():
            print(f"Loaded SQL from {p}")
            return p.read_text(encoding="utf-8")
    print("Using embedded SQL (file not found on workspace path).")
    return EMBEDDED_SQL


EMBEDDED_SQL = """
CREATE OR REPLACE VIEW {catalog}.gold.v_dashboard_kpis AS
SELECT
  SUM(active_patients) AS total_active_patients,
  ROUND(SUM(total_paid_amount), 2) AS total_paid_amount,
  COUNT(DISTINCT therapy_code) AS therapy_count,
  ROUND(AVG(avg_days_supply), 1) AS avg_days_supply,
  MAX(metric_date) AS latest_metric_date
FROM {catalog}.gold.daily_therapy_metrics
WHERE metric_date >= date_sub(current_date(), 120);

CREATE OR REPLACE VIEW {catalog}.gold.v_dashboard_daily_trend AS
SELECT
  metric_date,
  therapy_code,
  active_patients,
  total_paid_amount,
  avg_days_supply,
  new_starts,
  discontinuations
FROM {catalog}.gold.daily_therapy_metrics
WHERE metric_date >= date_sub(current_date(), 120);

CREATE OR REPLACE VIEW {catalog}.gold.v_dashboard_therapy_summary AS
SELECT
  therapy_code,
  SUM(active_patients) AS patient_fills,
  ROUND(SUM(total_paid_amount), 2) AS paid_amount,
  ROUND(AVG(avg_days_supply), 1) AS avg_days_supply
FROM {catalog}.gold.daily_therapy_metrics
WHERE metric_date >= date_sub(current_date(), 120)
GROUP BY therapy_code;

CREATE OR REPLACE VIEW {catalog}.gold.v_dashboard_journey_status AS
SELECT
  therapy_code,
  on_therapy,
  COUNT(*) AS patient_count,
  ROUND(AVG(days_since_last_fill), 1) AS avg_days_since_fill,
  SUM(pa_denial_count) AS pa_denials
FROM {catalog}.gold.patient_journey
GROUP BY therapy_code, on_therapy;

CREATE OR REPLACE VIEW {catalog}.gold.v_dashboard_at_risk AS
SELECT
  patient_token,
  therapy_code,
  last_fill_date,
  days_since_last_fill,
  pa_denial_count,
  hub_interactions_30d,
  on_therapy
FROM {catalog}.gold.patient_journey
WHERE NOT on_therapy OR days_since_last_fill > 30
ORDER BY days_since_last_fill DESC
LIMIT 200;
"""

# COMMAND ----------

raw = load_sql()
if "{catalog}" in raw:
    sql = raw.format(catalog=catalog)
else:
    sql = raw.replace("main.", f"{catalog}.")

statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]

for stmt in statements:
    spark.sql(stmt)
    first_line = stmt.split("\n")[0][:90]
    print(f"OK: {first_line}...")

print(f"\nDashboard views ready in `{catalog}.gold`. Import dashboards/*.lvdash.json in AI/BI.")
