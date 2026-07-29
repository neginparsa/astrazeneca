CREATE TABLE IF NOT EXISTS ${catalog}.gold.patient_journey (
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
CLUSTER BY (patient_token, therapy_code)
COMMENT 'Core patient journey snapshot for commercial and access teams';

CREATE TABLE IF NOT EXISTS ${catalog}.gold.daily_therapy_metrics (
  metric_date DATE,
  therapy_code STRING,
  active_patients BIGINT,
  new_starts BIGINT,
  discontinuations BIGINT,
  avg_days_supply DOUBLE,
  total_paid_amount DECIMAL(18, 2),
  computed_at TIMESTAMP
) USING DELTA
CLUSTER BY (metric_date, therapy_code)
COMMENT 'Single trusted daily rollup replacing fragmented multi-day reporting';

CREATE TABLE IF NOT EXISTS ${catalog}.ml.discontinuation_features (
  patient_token STRING,
  therapy_code STRING,
  as_of_date DATE,
  days_since_last_fill INT,
  fills_last_90d INT,
  pa_denials_last_180d INT,
  crm_outreach_last_30d INT,
  avg_days_supply_90d DOUBLE,
  label_discontinued_within_30d INT
) USING DELTA
CLUSTER BY (patient_token, as_of_date);
