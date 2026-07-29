-- Unity Catalog objects for Magnolia Pharma Lakehouse
-- Replace ${catalog} when running or use spark conf substitution in notebook 00.

CREATE SCHEMA IF NOT EXISTS ${catalog}.bronze
  COMMENT 'Raw ingested sources (Auto Loader / batch landing)';

CREATE SCHEMA IF NOT EXISTS ${catalog}.silver
  COMMENT 'Conformed, tokenized, deduplicated entities and events';

CREATE SCHEMA IF NOT EXISTS ${catalog}.gold
  COMMENT 'Trusted metrics and patient journey for BI and ML features';

CREATE SCHEMA IF NOT EXISTS ${catalog}.ml
  COMMENT 'MLflow-managed models and feature tables';

-- Bronze: one table per landing zone
CREATE TABLE IF NOT EXISTS ${catalog}.bronze.claims_raw (
  claim_id STRING,
  patient_id STRING,
  provider_npi STRING,
  ndc STRING,
  fill_date DATE,
  days_supply INT,
  paid_amount DECIMAL(12, 2),
  source_file STRING,
  ingested_at TIMESTAMP
) USING DELTA
COMMENT 'Claims feed as landed from payer files';

CREATE TABLE IF NOT EXISTS ${catalog}.bronze.specialty_rx_raw (
  rx_id STRING,
  patient_id STRING,
  therapy_code STRING,
  ship_date DATE,
  quantity INT,
  hub_status STRING,
  source_file STRING,
  ingested_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.bronze.prior_auth_raw (
  auth_id STRING,
  patient_id STRING,
  therapy_code STRING,
  status STRING,
  submitted_at TIMESTAMP,
  decided_at TIMESTAMP,
  source_file STRING,
  ingested_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.bronze.crm_raw (
  interaction_id STRING,
  patient_id STRING,
  channel STRING,
  outcome STRING,
  interaction_at TIMESTAMP,
  source_file STRING,
  ingested_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.bronze.inventory_raw (
  sku STRING,
  therapy_code STRING,
  site_id STRING,
  on_hand INT,
  as_of_date DATE,
  source_file STRING,
  ingested_at TIMESTAMP
) USING DELTA;
