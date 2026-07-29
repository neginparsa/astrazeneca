# Azure deployment checklist

## 1. Storage (ADLS Gen2)

1. Create storage account + container `lakehouse`.
2. Create folders: `landing/{claims,specialty_rx,prior_auth,crm,inventory}` and `_checkpoints/`.
3. In Databricks **Catalog** → **External locations**, grant access to `abfss://lakehouse@...`.

## 2. Unity Catalog

1. Enable UC metastore on the workspace (Azure admin).
2. Create catalog `magnolia_pharma` (or name in `config/env.yaml`).
3. Run notebook `00_setup_unity_catalog`.

## 3. Entra ID (RBAC)

Map Azure AD groups to UC groups listed in `config/env.example.yaml`, then run `governance/unity_catalog_grants.sql` (substitute `${catalog}` and group names).

## 4. Ingest sample data

Locally:

```bash
python3 scripts/generate_sample_data.py
```

Upload `local-data/landing/**` to ADLS landing paths, then run `01_bronze_autoloader`.

## 5. Job schedule

Optional: `databricks bundle deploy -t dev` then trigger `magnolia_daily_refresh` (requires Databricks CLI auth).

## 6. Portfolio evidence

Capture screenshots or logs for:

- Unity Catalog lineage (Bronze → Silver → Gold)
- Streaming query progress for Auto Loader
- `06_photon_bakeoff` timing comparison
- MLflow model registry entry for `therapy_discontinuation_risk`
