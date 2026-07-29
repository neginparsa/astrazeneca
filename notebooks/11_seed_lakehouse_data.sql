-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 11 — Bulk seed (SQL warehouse)
-- MAGIC ~500 patients · ~5,000 claims · full Bronze / Silver / Gold / ML  
-- MAGIC Catalog: **main** (edit below if different)

-- COMMAND ----------

USE CATALOG main;
CREATE SCHEMA IF NOT EXISTS main.bronze;
CREATE SCHEMA IF NOT EXISTS main.silver;
CREATE SCHEMA IF NOT EXISTS main.gold;
CREATE SCHEMA IF NOT EXISTS main.ml;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.bronze.claims_raw AS
WITH patients AS (
  SELECT CONCAT('PAT-', LPAD(CAST(id AS STRING), 5, '0')) AS patient_id
  FROM (SELECT EXPLODE(SEQUENCE(1, 500)) AS id)
),
fills AS (SELECT EXPLODE(SEQUENCE(0, 9)) AS fill_num),
therapies AS (
  SELECT therapy_code FROM VALUES
    ('MAGN-101'), ('MAGN-204'), ('MAGN-330'), ('MAGN-410'), ('MAGN-522')
  AS t(therapy_code)
)
SELECT
  UUID() AS claim_id,
  p.patient_id,
  CAST(1000000000 + (HASH(p.patient_id) % 900000000) AS STRING) AS provider_npi,
  CONCAT('00000-', LPAD(CAST(1000 + (HASH(p.patient_id, f.fill_num) % 8999) AS STRING), 4, '0')) AS ndc,
  DATE_ADD(DATE('2025-01-01'), CAST(ABS(HASH(p.patient_id, f.fill_num)) % 540 AS INT)) AS fill_date,
  CASE WHEN ABS(HASH(f.fill_num)) % 3 = 0 THEN 90 WHEN ABS(HASH(f.fill_num)) % 2 = 0 THEN 30 ELSE 28 END AS days_supply,
  CAST(500 + (ABS(HASH(p.patient_id, f.fill_num)) % 8000) AS DECIMAL(12, 2)) AS paid_amount,
  th.therapy_code,
  'seed/bulk_claims' AS source_file,
  CURRENT_TIMESTAMP() AS ingested_at
FROM patients p
CROSS JOIN fills f
JOIN therapies th ON ABS(HASH(p.patient_id)) % 5 = ABS(HASH(th.therapy_code)) % 5;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.bronze.specialty_rx_raw AS
WITH patients AS (
  SELECT CONCAT('PAT-', LPAD(CAST(id AS STRING), 5, '0')) AS patient_id
  FROM (SELECT EXPLODE(SEQUENCE(1, 500)) AS id)
),
ships AS (SELECT EXPLODE(SEQUENCE(0, 4)) AS ship_num)
SELECT UUID() AS rx_id, p.patient_id,
  CASE ABS(HASH(p.patient_id)) % 5 WHEN 0 THEN 'MAGN-101' WHEN 1 THEN 'MAGN-204' WHEN 2 THEN 'MAGN-330' WHEN 3 THEN 'MAGN-410' ELSE 'MAGN-522' END AS therapy_code,
  DATE_ADD(CURRENT_DATE(), -CAST(ABS(HASH(p.patient_id, s.ship_num)) % 180 AS INT)) AS ship_date,
  1 + (ABS(HASH(s.ship_num)) % 3) AS quantity,
  CASE ABS(HASH(p.patient_id, s.ship_num)) % 4 WHEN 0 THEN 'SHIPPED' WHEN 1 THEN 'DELAYED' WHEN 2 THEN 'PROCESSING' ELSE 'CANCELLED' END AS hub_status,
  'seed/specialty_rx' AS source_file, CURRENT_TIMESTAMP() AS ingested_at
FROM patients p CROSS JOIN ships s WHERE ABS(HASH(p.patient_id, s.ship_num)) % 3 != 0;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.bronze.prior_auth_raw AS
WITH patients AS (
  SELECT CONCAT('PAT-', LPAD(CAST(id AS STRING), 5, '0')) AS patient_id
  FROM (SELECT EXPLODE(SEQUENCE(1, 500)) AS id) WHERE id % 3 = 0
)
SELECT UUID() AS auth_id, patient_id,
  CASE ABS(HASH(patient_id)) % 5 WHEN 0 THEN 'MAGN-101' WHEN 1 THEN 'MAGN-204' WHEN 2 THEN 'MAGN-330' WHEN 3 THEN 'MAGN-410' ELSE 'MAGN-522' END AS therapy_code,
  CASE ABS(HASH(patient_id)) % 5 WHEN 0 THEN 'APPROVED' WHEN 1 THEN 'DENIED' WHEN 2 THEN 'PENDING' WHEN 3 THEN 'APPROVED' ELSE 'DENIED' END AS status,
  TIMESTAMP('2025-06-01') + (ABS(HASH(patient_id)) % 300) * INTERVAL 1 DAY AS submitted_at,
  TIMESTAMP('2025-06-15') + (ABS(HASH(patient_id)) % 300) * INTERVAL 1 DAY AS decided_at,
  'seed/prior_auth' AS source_file, CURRENT_TIMESTAMP() AS ingested_at
FROM patients;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.bronze.crm_raw AS
WITH patients AS (
  SELECT CONCAT('PAT-', LPAD(CAST(id AS STRING), 5, '0')) AS patient_id
  FROM (SELECT EXPLODE(SEQUENCE(1, 500)) AS id) WHERE id % 2 = 0
),
touch AS (SELECT EXPLODE(SEQUENCE(0, 2)) AS touch_num)
SELECT UUID() AS interaction_id, p.patient_id,
  CASE ABS(HASH(p.patient_id, t.touch_num)) % 3 WHEN 0 THEN 'PHONE' WHEN 1 THEN 'EMAIL' ELSE 'SMS' END AS channel,
  CASE ABS(HASH(p.patient_id, t.touch_num)) % 4 WHEN 0 THEN 'REACHED' WHEN 1 THEN 'NO_ANSWER' WHEN 2 THEN 'SCHEDULED' ELSE 'VOICEMAIL' END AS outcome,
  CURRENT_TIMESTAMP() - (ABS(HASH(p.patient_id, t.touch_num)) % 60) * INTERVAL 1 DAY AS interaction_at,
  'seed/crm' AS source_file, CURRENT_TIMESTAMP() AS ingested_at
FROM patients p CROSS JOIN touch t;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.bronze.inventory_raw AS
SELECT sku, therapy_code, site_id, on_hand, as_of_date, 'seed/inventory' AS source_file, CURRENT_TIMESTAMP() AS ingested_at
FROM VALUES
  ('SKU-MAGN-101', 'MAGN-101', 'DFW-01', 420, CURRENT_DATE()),
  ('SKU-MAGN-204', 'MAGN-204', 'ATL-02', 310, CURRENT_DATE()),
  ('SKU-MAGN-330', 'MAGN-330', 'PHX-03', 275, CURRENT_DATE()),
  ('SKU-MAGN-410', 'MAGN-410', 'DFW-01', 190, CURRENT_DATE()),
  ('SKU-MAGN-522', 'MAGN-522', 'BOS-04', 155, CURRENT_DATE())
AS t(sku, therapy_code, site_id, on_hand, as_of_date);

-- COMMAND ----------

CREATE OR REPLACE TABLE main.silver.claims_enriched AS
SELECT claim_id, SHA2(CAST(patient_id AS STRING), 256) AS patient_token, therapy_code,
  fill_date, days_supply, paid_amount, SHA2(CONCAT('npi:', provider_npi), 256) AS provider_token,
  CURRENT_TIMESTAMP() AS updated_at
FROM main.bronze.claims_raw;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.silver.patient_events AS
SELECT SHA2(CONCAT(claim_id, CAST(fill_date AS STRING), 'CLAIM'), 256) AS event_id,
  SHA2(CAST(patient_id AS STRING), 256) AS patient_token, 'CLAIM_FILL' AS event_type, therapy_code,
  fill_date AS event_date, CAST(fill_date AS TIMESTAMP) AS event_ts,
  MAP('days_supply', CAST(days_supply AS STRING), 'paid_amount', CAST(paid_amount AS STRING)) AS detail,
  'claims' AS source_system, CURRENT_TIMESTAMP() AS updated_at
FROM main.bronze.claims_raw;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.gold.patient_journey AS
WITH ranked AS (
  SELECT patient_token, therapy_code, fill_date AS last_fill_date,
    ROW_NUMBER() OVER (PARTITION BY patient_token, therapy_code ORDER BY fill_date DESC) AS rn
  FROM main.silver.claims_enriched
)
SELECT patient_token, therapy_code, DATE_SUB(last_fill_date, 365) AS journey_start, last_fill_date,
  DATEDIFF(CURRENT_DATE(), last_fill_date) AS days_since_last_fill,
  CAST(ABS(HASH(patient_token)) % 3 AS INT) AS pa_denial_count,
  CAST(ABS(HASH(patient_token)) % 5 AS INT) AS hub_interactions_30d,
  DATEDIFF(CURRENT_DATE(), last_fill_date) <= 45 AS on_therapy, CURRENT_TIMESTAMP() AS journey_updated_at
FROM ranked WHERE rn = 1;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.gold.daily_therapy_metrics AS
SELECT fill_date AS metric_date, therapy_code, COUNT(DISTINCT patient_token) AS active_patients,
  CAST(SUM(CASE WHEN ABS(HASH(patient_token, fill_date)) % 20 = 0 THEN 1 ELSE 0 END) AS BIGINT) AS new_starts,
  CAST(SUM(CASE WHEN ABS(HASH(patient_token, fill_date)) % 25 = 0 THEN 1 ELSE 0 END) AS BIGINT) AS discontinuations,
  AVG(days_supply) AS avg_days_supply, SUM(paid_amount) AS total_paid_amount, CURRENT_TIMESTAMP() AS computed_at
FROM main.silver.claims_enriched GROUP BY fill_date, therapy_code;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.ml.discontinuation_features AS
SELECT patient_token, therapy_code, CURRENT_DATE() AS as_of_date, days_since_last_fill,
  CAST(3 + ABS(HASH(patient_token)) % 8 AS INT) AS fills_last_90d,
  pa_denial_count AS pa_denials_last_180d, hub_interactions_30d AS crm_outreach_last_30d,
  28.0 + (ABS(HASH(patient_token)) % 5) AS avg_days_supply_90d,
  CAST(NOT on_therapy AS INT) AS label_discontinued_within_30d
FROM main.gold.patient_journey;

-- COMMAND ----------

SELECT 'bronze.claims_raw' AS tbl, COUNT(*) AS rows FROM main.bronze.claims_raw
UNION ALL SELECT 'silver.claims_enriched', COUNT(*) FROM main.silver.claims_enriched
UNION ALL SELECT 'gold.daily_therapy_metrics', COUNT(*) FROM main.gold.daily_therapy_metrics
UNION ALL SELECT 'gold.patient_journey', COUNT(*) FROM main.gold.patient_journey
UNION ALL SELECT 'ml.discontinuation_features', COUNT(*) FROM main.ml.discontinuation_features;

-- COMMAND ----------

SELECT * FROM main.gold.daily_therapy_metrics ORDER BY metric_date DESC LIMIT 10;
