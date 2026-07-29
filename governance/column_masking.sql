-- Column masks and row filters (Databricks Unity Catalog).
-- Requires appropriate UC entitlement and catalog owner privileges.

CREATE OR REPLACE FUNCTION ${catalog}.gold.sha256_mask(col STRING)
  RETURN CASE
    WHEN is_member('${entra_groups.data_science}') THEN col
    ELSE '***'
  END;

ALTER TABLE ${catalog}.gold.patient_journey
  ALTER COLUMN patient_token SET MASK ${catalog}.gold.sha256_mask;

CREATE OR REPLACE FUNCTION ${catalog}.gold.commercial_row_filter()
  RETURN IF(is_member('${entra_groups.commercial_analysts}'), on_therapy = true, true);

ALTER TABLE ${catalog}.gold.patient_journey SET ROW FILTER ${catalog}.gold.commercial_row_filter ON (patient_token);
