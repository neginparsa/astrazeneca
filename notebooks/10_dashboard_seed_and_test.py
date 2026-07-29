# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Dashboard data fix (Python / Serverless only)
# MAGIC
# MAGIC **If you see:** `SQL warehouses only support executing SQL cells`  
# MAGIC **Use instead:** **`10_dashboard_seed.sql`** (SQL notebook — works on SQL warehouse).
# MAGIC
# MAGIC This Python notebook requires **Serverless** compute (not SQL warehouse).

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog").strip() or "main"
spark.sql(f"USE CATALOG {catalog}")
for s in ("bronze", "silver", "gold", "ml"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{s}")

print(f"Using catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Seed Gold tables (SQL — visible to SQL warehouse / dashboards)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.gold.daily_therapy_metrics (
  metric_date DATE,
  therapy_code STRING,
  active_patients BIGINT,
  new_starts BIGINT,
  discontinuations BIGINT,
  avg_days_supply DOUBLE,
  total_paid_amount DECIMAL(18,2),
  computed_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
INSERT OVERWRITE {catalog}.gold.daily_therapy_metrics
SELECT * FROM VALUES
  (DATE('2026-01-15'), 'MAGN-101', CAST(120 AS BIGINT), CAST(8 AS BIGINT), CAST(2 AS BIGINT), 30.0, CAST(450000.00 AS DECIMAL(18,2)), current_timestamp()),
  (DATE('2026-02-15'), 'MAGN-101', CAST(135 AS BIGINT), CAST(10 AS BIGINT), CAST(3 AS BIGINT), 30.0, CAST(510000.00 AS DECIMAL(18,2)), current_timestamp()),
  (DATE('2026-03-15'), 'MAGN-101', CAST(142 AS BIGINT), CAST(9 AS BIGINT), CAST(4 AS BIGINT), 30.0, CAST(525000.00 AS DECIMAL(18,2)), current_timestamp()),
  (DATE('2026-04-15'), 'MAGN-204', CAST(88 AS BIGINT), CAST(6 AS BIGINT), CAST(1 AS BIGINT), 28.0, CAST(320000.00 AS DECIMAL(18,2)), current_timestamp()),
  (DATE('2026-05-15'), 'MAGN-204', CAST(95 AS BIGINT), CAST(7 AS BIGINT), CAST(2 AS BIGINT), 28.0, CAST(355000.00 AS DECIMAL(18,2)), current_timestamp()),
  (DATE('2026-06-15'), 'MAGN-330', CAST(64 AS BIGINT), CAST(5 AS BIGINT), CAST(1 AS BIGINT), 90.0, CAST(890000.00 AS DECIMAL(18,2)), current_timestamp()),
  (DATE('2026-07-01'), 'MAGN-330', CAST(70 AS BIGINT), CAST(4 AS BIGINT), CAST(0 AS BIGINT), 90.0, CAST(920000.00 AS DECIMAL(18,2)), current_timestamp())
AS t(metric_date, therapy_code, active_patients, new_starts, discontinuations, avg_days_supply, total_paid_amount, computed_at)
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.gold.patient_journey (
  patient_token STRING,
  therapy_code STRING,
  journey_start DATE,
  last_fill_date DATE,
  days_since_last_fill INT,
  pa_denial_count INT,
  hub_interactions_30d INT,
  on_therapy BOOLEAN,
  journey_updated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
INSERT OVERWRITE {catalog}.gold.patient_journey
SELECT * FROM VALUES
  ('tok_001', 'MAGN-101', DATE('2025-08-01'), DATE('2026-06-20'), 39, 0, 2, true, current_timestamp()),
  ('tok_002', 'MAGN-101', DATE('2025-09-01'), DATE('2026-05-01'), 89, 1, 0, false, current_timestamp()),
  ('tok_003', 'MAGN-204', DATE('2025-10-01'), DATE('2026-07-10'), 19, 0, 1, true, current_timestamp()),
  ('tok_004', 'MAGN-204', DATE('2025-11-01'), DATE('2026-04-01'), 119, 2, 3, false, current_timestamp()),
  ('tok_005', 'MAGN-330', DATE('2025-12-01'), DATE('2026-07-15'), 14, 0, 0, true, current_timestamp()),
  ('tok_006', 'MAGN-330', DATE('2026-01-01'), DATE('2026-03-01'), 150, 1, 1, false, current_timestamp())
AS t(patient_token, therapy_code, journey_start, last_fill_date, days_since_last_fill, pa_denial_count, hub_interactions_30d, on_therapy, journey_updated_at)
""")

print("Seed complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verify row counts

# COMMAND ----------

metrics_n = spark.sql(f"SELECT COUNT(*) AS n FROM {catalog}.gold.daily_therapy_metrics").collect()[0]["n"]
journey_n = spark.sql(f"SELECT COUNT(*) AS n FROM {catalog}.gold.patient_journey").collect()[0]["n"]
print(f"daily_therapy_metrics: {metrics_n} rows")
print(f"patient_journey: {journey_n} rows")

if metrics_n == 0 or journey_n == 0:
    raise RuntimeError("Seed failed — tables still empty.")

spark.sql(f"SELECT * FROM {catalog}.gold.daily_therapy_metrics ORDER BY metric_date").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dashboard SQL (copy into AI/BI dataset if import fails)
# MAGIC
# MAGIC **Executive KPIs:**
# MAGIC ```sql
# MAGIC SELECT SUM(active_patients) AS total_active_patients,
# MAGIC        SUM(total_paid_amount) AS total_paid_amount
# MAGIC FROM main.gold.daily_therapy_metrics
# MAGIC ```
# MAGIC
# MAGIC **Trend:**
# MAGIC ```sql
# MAGIC SELECT metric_date, therapy_code, active_patients, total_paid_amount
# MAGIC FROM main.gold.daily_therapy_metrics
# MAGIC ORDER BY metric_date
# MAGIC ```

# COMMAND ----------

spark.sql(f"""
SELECT SUM(active_patients) AS total_active_patients,
       SUM(total_paid_amount) AS total_paid_amount
FROM {catalog}.gold.daily_therapy_metrics
""").show()

# COMMAND ----------

print(f"""
SUCCESS — data is in `{catalog}.gold`.

NEXT (do all 3):
1. SQL Warehouses → START your warehouse (must say Running).
2. Dashboards → delete old imports → Import dashboards/magnolia_smoke_test.lvdash.json first.
3. If smoke test shows a number, import therapy_executive.lvdash.json again.

If smoke test is still empty: Dashboard → Data → pick dataset → Run query.
Paste any error message here.
""")
