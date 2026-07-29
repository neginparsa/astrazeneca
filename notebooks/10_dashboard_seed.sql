-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 10 — Dashboard seed (SQL only)
-- MAGIC
-- MAGIC Run on **Serverless** OR attach a **SQL warehouse** — this notebook uses **SQL cells only**.
-- MAGIC
-- MAGIC Loads demo data into `main.gold` for AI/BI dashboards.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Set catalog (change if yours is not `main`)

-- COMMAND ----------

USE CATALOG main;
CREATE SCHEMA IF NOT EXISTS main.gold;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.gold.daily_therapy_metrics (
  metric_date DATE,
  therapy_code STRING,
  active_patients BIGINT,
  new_starts BIGINT,
  discontinuations BIGINT,
  avg_days_supply DOUBLE,
  total_paid_amount DECIMAL(18, 2),
  computed_at TIMESTAMP
) USING DELTA;

-- COMMAND ----------

INSERT OVERWRITE main.gold.daily_therapy_metrics
SELECT * FROM VALUES
  (DATE('2026-01-15'), 'MAGN-101', CAST(120 AS BIGINT), CAST(8 AS BIGINT), CAST(2 AS BIGINT), 30.0, CAST(450000.00 AS DECIMAL(18, 2)), current_timestamp()),
  (DATE('2026-02-15'), 'MAGN-101', CAST(135 AS BIGINT), CAST(10 AS BIGINT), CAST(3 AS BIGINT), 30.0, CAST(510000.00 AS DECIMAL(18, 2)), current_timestamp()),
  (DATE('2026-03-15'), 'MAGN-101', CAST(142 AS BIGINT), CAST(9 AS BIGINT), CAST(4 AS BIGINT), 30.0, CAST(525000.00 AS DECIMAL(18, 2)), current_timestamp()),
  (DATE('2026-04-15'), 'MAGN-204', CAST(88 AS BIGINT), CAST(6 AS BIGINT), CAST(1 AS BIGINT), 28.0, CAST(320000.00 AS DECIMAL(18, 2)), current_timestamp()),
  (DATE('2026-05-15'), 'MAGN-204', CAST(95 AS BIGINT), CAST(7 AS BIGINT), CAST(2 AS BIGINT), 28.0, CAST(355000.00 AS DECIMAL(18, 2)), current_timestamp()),
  (DATE('2026-06-15'), 'MAGN-330', CAST(64 AS BIGINT), CAST(5 AS BIGINT), CAST(1 AS BIGINT), 90.0, CAST(890000.00 AS DECIMAL(18, 2)), current_timestamp()),
  (DATE('2026-07-01'), 'MAGN-330', CAST(70 AS BIGINT), CAST(4 AS BIGINT), CAST(0 AS BIGINT), 90.0, CAST(920000.00 AS DECIMAL(18, 2)), current_timestamp())
AS t(metric_date, therapy_code, active_patients, new_starts, discontinuations, avg_days_supply, total_paid_amount, computed_at);

-- COMMAND ----------

CREATE OR REPLACE TABLE main.gold.patient_journey (
  patient_token STRING,
  therapy_code STRING,
  journey_start DATE,
  last_fill_date DATE,
  days_since_last_fill INT,
  pa_denial_count INT,
  hub_interactions_30d INT,
  on_therapy BOOLEAN,
  journey_updated_at TIMESTAMP
) USING DELTA;

-- COMMAND ----------

INSERT OVERWRITE main.gold.patient_journey
SELECT * FROM VALUES
  ('tok_001', 'MAGN-101', DATE('2025-08-01'), DATE('2026-06-20'), 39, 0, 2, true, current_timestamp()),
  ('tok_002', 'MAGN-101', DATE('2025-09-01'), DATE('2026-05-01'), 89, 1, 0, false, current_timestamp()),
  ('tok_003', 'MAGN-204', DATE('2025-10-01'), DATE('2026-07-10'), 19, 0, 1, true, current_timestamp()),
  ('tok_004', 'MAGN-204', DATE('2025-11-01'), DATE('2026-04-01'), 119, 2, 3, false, current_timestamp()),
  ('tok_005', 'MAGN-330', DATE('2025-12-01'), DATE('2026-07-15'), 14, 0, 0, true, current_timestamp()),
  ('tok_006', 'MAGN-330', DATE('2026-01-01'), DATE('2026-03-01'), 150, 1, 1, false, current_timestamp())
AS t(patient_token, therapy_code, journey_start, last_fill_date, days_since_last_fill, pa_denial_count, hub_interactions_30d, on_therapy, journey_updated_at);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify (should show 7 and 6 rows)

-- COMMAND ----------

SELECT 'daily_therapy_metrics' AS tbl, COUNT(*) AS rows FROM main.gold.daily_therapy_metrics
UNION ALL
SELECT 'patient_journey', COUNT(*) FROM main.gold.patient_journey;

-- COMMAND ----------

SELECT * FROM main.gold.daily_therapy_metrics ORDER BY metric_date;

-- COMMAND ----------

SELECT SUM(active_patients) AS total_patients, SUM(total_paid_amount) AS total_paid
FROM main.gold.daily_therapy_metrics;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Done
-- MAGIC
-- MAGIC 1. SQL warehouse must be **Running**
-- MAGIC 2. Import **`dashboards/magnolia_smoke_test.lvdash.json`** — counters should show **716** and **7**
-- MAGIC 3. Then import **`therapy_executive.lvdash.json`**
