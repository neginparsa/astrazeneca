CREATE TABLE IF NOT EXISTS ${catalog}.silver.patient_token_map (
  patient_id STRING,
  patient_token STRING,
  tokenized_at TIMESTAMP
) USING DELTA
COMMENT 'Maps source patient_id to irreversible token for downstream sharing';

CREATE TABLE IF NOT EXISTS ${catalog}.silver.patient_events (
  event_id STRING,
  patient_token STRING,
  event_type STRING,
  therapy_code STRING,
  event_date DATE,
  event_ts TIMESTAMP,
  detail MAP<STRING, STRING>,
  source_system STRING,
  updated_at TIMESTAMP
) USING DELTA
CLUSTER BY (patient_token, event_date)
COMMENT 'Unified timeline: fills, PA, CRM, specialty shipments';

CREATE TABLE IF NOT EXISTS ${catalog}.silver.claims_enriched (
  claim_id STRING,
  patient_token STRING,
  therapy_code STRING,
  fill_date DATE,
  days_supply INT,
  paid_amount DECIMAL(12, 2),
  provider_token STRING,
  updated_at TIMESTAMP
) USING DELTA
CLUSTER BY (patient_token, fill_date);
