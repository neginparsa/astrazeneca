# Databricks Free Edition — Magnolia project

Use this checklist only. Full Azure setup is **not** required for Free Edition.

## Sign up

1. Go to [Databricks Free Edition signup](https://www.databricks.com/try-databricks).
2. Sign in with Google or Microsoft (easiest).
3. Open your workspace when it is ready.

## Import the project (pick one)

### A — Upload just the quickstart (fastest)

1. In Cursor/your Mac, open **`notebooks/QUICKSTART_setup_and_run.py`** in a text editor.
2. Databricks → **Workspace** → your user folder → **Create** → **Notebook**.
3. Name it `QUICKSTART_setup_and_run`.
4. Copy **all** notebook contents from the `.py` file into the notebook (or **File** → **Import** and select the `.py` file).

You do **not** need the whole repo for the first run.

### B — Full repo (Git)

1. Push this folder to GitHub (private repo is fine).
2. Databricks → **Workspace** → **Add** → **Repo** → paste clone URL.

## Run the pipeline

1. Open **`QUICKSTART_setup_and_run`**.
2. At the top, set compute to **Serverless** (default on Free Edition).
3. Confirm the widget **catalog** = **`main`** (do not change unless you know you have another catalog).
4. **Run all** cells (Runtime → Run all).
5. When it finishes, open **Catalog** (left sidebar) → **`main`**:
   - `bronze.claims_raw`
   - `silver.claims_enriched`
   - `gold.daily_therapy_metrics`
   - `gold.patient_journey`
   - `ml.discontinuation_features`

Expected runtime: about **5–15 minutes** on first run.

## What works on Free Edition

| Feature | In this project |
|--------|------------------|
| Unity Catalog (`main`) | Yes |
| Delta Bronze / Silver / Gold | Yes — QUICKSTART |
| Serverless notebooks | Yes — required |
| MLflow training in notebook | Usually yes (last section; OK if it skips) |
| Auto Loader → ADLS | No — skip notebooks `01`, `02` |
| Custom catalog `magnolia_pharma` | Often no — use **`main`** |
| DBFS landing paths | No — QUICKSTART writes Delta directly |
| Photon bake-off / Azure Entra | No — need paid Azure workspace later |

## Optional next notebooks (same workspace)

After QUICKSTART succeeds:

| Notebook | Free Edition? |
|----------|----------------|
| `03_gold_patient_journey` | Re-runs gold logic (optional) |
| `04_performance_tuning` | Partial — some `ALTER CLUSTER BY` may fail; safe to skip |
| `05_ml_discontinuation_risk` | Try if MLflow worked in QUICKSTART |
| `06_photon_bakeoff` | Skip on Free Edition |
| `01`, `02` | Skip until you have Azure ADLS |

## Troubleshooting

| Error | Fix |
|-------|-----|
| `CREATE CATALOG` / permission | Set catalog widget to **`main`** only. |
| `Cannot create schema` | Run as account owner; refresh and retry. |
| Serverless not available | Confirm you are on **Free Edition**, not an expired trial. |
| Daily quota exceeded | Free Edition pauses compute until the next day — wait and retry. |
| `pip install` fails | QUICKSTART no longer requires pip; re-import latest notebook from repo. |
| Import repo / no config file | Inlined config is used automatically. |

## When you outgrow Free Edition

Move to **Azure Databricks trial** and follow **Path 2** in [SETUP_FROM_SCRATCH.md](SETUP_FROM_SCRATCH.md) for ADLS, Auto Loader, streaming, and governance.
