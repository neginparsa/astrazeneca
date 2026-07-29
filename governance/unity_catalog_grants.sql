-- Example Unity Catalog grants (map groups to your Entra ID security groups).
-- Run as metastore admin / catalog owner after tables exist.

-- Platform engineers: full build access
GRANT USE CATALOG ON CATALOG ${catalog} TO `${entra_groups.platform_engineers}`;
GRANT ALL PRIVILEGES ON SCHEMA ${catalog}.bronze TO `${entra_groups.platform_engineers}`;
GRANT ALL PRIVILEGES ON SCHEMA ${catalog}.silver TO `${entra_groups.platform_engineers}`;
GRANT ALL PRIVILEGES ON SCHEMA ${catalog}.gold TO `${entra_groups.platform_engineers}`;

-- Commercial: gold read only (no raw bronze)
GRANT USE CATALOG ON CATALOG ${catalog} TO `${entra_groups.commercial_analysts}`;
GRANT USE SCHEMA ON SCHEMA ${catalog}.gold TO `${entra_groups.commercial_analysts}`;
GRANT SELECT ON SCHEMA ${catalog}.gold TO `${entra_groups.commercial_analysts}`;

-- Market access: gold + selected silver aggregates (no patient_id columns)
GRANT USE CATALOG ON CATALOG ${catalog} TO `${entra_groups.market_access}`;
GRANT USE SCHEMA ON SCHEMA ${catalog}.gold TO `${entra_groups.market_access}`;
GRANT SELECT ON TABLE ${catalog}.gold.daily_therapy_metrics TO `${entra_groups.market_access}`;

-- Data science: ML schema + feature table, masked gold journey
GRANT USE CATALOG ON CATALOG ${catalog} TO `${entra_groups.data_science}`;
GRANT USE SCHEMA ON SCHEMA ${catalog}.ml TO `${entra_groups.data_science}`;
GRANT SELECT ON TABLE ${catalog}.ml.discontinuation_features TO `${entra_groups.data_science}`;
