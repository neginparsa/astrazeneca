# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Fix empty dashboards (validate + repair)
# MAGIC Run this if AI/BI dashboards show **no data**.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog").strip() or "main"
spark.sql(f"USE CATALOG {catalog}")

TABLES = [
    f"{catalog}.gold.daily_therapy_metrics",
    f"{catalog}.gold.patient_journey",
    f"{catalog}.silver.claims_enriched",
]

print("=== Row counts ===")
empty = []
for t in TABLES:
    try:
        n = spark.table(t).count()
        print(f"{t}: {n} rows")
        if n == 0:
            empty.append(t)
    except Exception as e:
        print(f"{t}: MISSING ({e})")
        empty.append(t)

# COMMAND ----------

# MAGIC %md
# MAGIC ## If Gold tables are empty → re-run QUICKSTART
# MAGIC Open **`QUICKSTART_setup_and_run`** and **Run all**, then run this notebook again.

# COMMAND ----------

if empty:
    dbutils.notebook.exit(
        f"STOP: Empty or missing tables: {empty}. Run QUICKSTART_setup_and_run first."
    )

# COMMAND ----------

# Recreate dashboard views (no date filter — show all Gold data)
views_sql = f"""
CREATE OR REPLACE VIEW {catalog}.gold.v_dashboard_kpis AS
SELECT
  SUM(active_patients) AS total_active_patients,
  ROUND(SUM(total_paid_amount), 2) AS total_paid_amount,
  COUNT(DISTINCT therapy_code) AS therapy_count,
  ROUND(AVG(avg_days_supply), 1) AS avg_days_supply,
  MAX(metric_date) AS latest_metric_date
FROM {catalog}.gold.daily_therapy_metrics;

CREATE OR REPLACE VIEW {catalog}.gold.v_dashboard_daily_trend AS
SELECT * FROM {catalog}.gold.daily_therapy_metrics;

CREATE OR REPLACE VIEW {catalog}.gold.v_dashboard_therapy_summary AS
SELECT
  therapy_code,
  SUM(active_patients) AS patient_fills,
  ROUND(SUM(total_paid_amount), 2) AS paid_amount,
  ROUND(AVG(avg_days_supply), 1) AS avg_days_supply
FROM {catalog}.gold.daily_therapy_metrics
GROUP BY therapy_code;

CREATE OR REPLACE VIEW {catalog}.gold.v_dashboard_journey_status AS
SELECT
  therapy_code,
  CAST(on_therapy AS STRING) AS on_therapy,
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
  CAST(on_therapy AS STRING) AS on_therapy
FROM {catalog}.gold.patient_journey
ORDER BY days_since_last_fill DESC
LIMIT 200
"""

for stmt in [s.strip() for s in views_sql.split(";") if s.strip()]:
    spark.sql(stmt)
    print("Created:", stmt.split("\n")[0][:70])

# COMMAND ----------

print("=== Sample data dashboards use ===")
spark.sql(f"SELECT * FROM {catalog}.gold.v_dashboard_kpis").show(truncate=False)
spark.sql(f"SELECT * FROM {catalog}.gold.v_dashboard_daily_trend LIMIT 5").show(truncate=False)
spark.sql(f"SELECT * FROM {catalog}.gold.v_dashboard_journey_status").show(truncate=False)

# COMMAND ----------

print("""
NEXT STEPS:
1. SQL warehouse must be RUNNING (Dashboards → pick warehouse → Start if stopped).
2. Re-import dashboards from dashboards/*.lvdash.json (Git pull first) OR click Refresh on each chart.
3. In dashboard Data panel, open a dataset → Run query — you should see rows.
4. Catalog in SQL must be '{catalog}' — edit dataset SQL if you used a different catalog.
""".format(catalog=catalog))
