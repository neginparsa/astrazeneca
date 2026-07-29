# Setup from scratch (start here)

You do **not** need Azure, ADLS, or CLI tools to run the project the first time. Follow **Path 1** below (about 30–45 minutes). Move to **Path 2** when you want the full Azure Lakehouse story from your resume.

---

## Path 1 — Run the lakehouse today (Databricks Free Edition)

Best if you have **no Azure account** yet and want working Bronze → Silver → Gold + MLflow.

### Step 1: Create a Databricks account

1. Open [Databricks Free Edition](https://www.databricks.com/try-databricks).
2. Sign up (email or Google). Pick any cloud region shown (AWS is fine for learning).
3. Wait for your workspace URL (looks like `https://dbc-xxxxx.cloud.databricks.com`).

### Step 2: Put this project in the workspace

**Option A — Git (recommended)**

1. Push this folder to a **private** GitHub repo (or use GitLab/Bitbucket).
2. In Databricks: **Workspace** → right-click your user folder → **Add** → **Repo**.
3. Clone URL → paste your repo URL → **Create Repo**.

**Option B — Upload without Git**

1. Zip the project folder on your Mac (exclude `.venv`).
2. Databricks → **Workspace** → **Import**.
3. Import the zip under `/Users/you/magnolia-pharma` (any folder name is fine).

### Step 3: Run the one-shot notebook

1. Open **`notebooks/QUICKSTART_setup_and_run`** (`.py` file in Repos).
2. Attach **Serverless** compute (dropdown at top) — no cluster setup needed.
3. **Run all cells** top to bottom (~5–10 min first time).
4. At the end you should see:
   - Tables under `main.bronze`, `main.silver`, `main.gold`, `main.ml`
   - Sample rows from `main.gold.daily_therapy_metrics`
   - An MLflow run (if MLflow is enabled in your workspace)

Config used: `config/env.yaml` (`mode: demo`, DBFS landing paths).

### Step 4: Run the rest of the curriculum (optional)

After QUICKSTART succeeds, run notebooks `00`–`06` in order for Auto Loader, streaming MERGE, Photon bake-off, etc. On Free Edition, **Auto Loader to ADLS** may be limited; keep `mode: demo` or use DBFS paths in `config/env.yaml`.

### Troubleshooting Path 1

| Problem | Fix |
|--------|-----|
| `CREATE CATALOG` fails | In notebook widget, set catalog to **`main`** (default on Free Edition). |
| Cannot find `config/env.yaml` | Set repo path: `%cd` to repo root, or set env var `MAGNOLIA_CONFIG` to full path. |
| `pyyaml` missing | First cell runs `%pip install pyyaml`; retry after install. |
| MLflow register fails | Skip last MLflow cell; training still runs locally in the notebook. |

---

## Path 2 — Full Azure Databricks (matches resume / portfolio)

Use this for **ADLS Gen2**, **Unity Catalog metastore on Azure**, **Entra ID**, and **Photon** bake-offs.

### Overview

```text
Azure subscription
  → Resource group
  → Storage account (ADLS Gen2) + container lakehouse
  → Azure Databricks workspace (Premium)
  → Unity Catalog metastore linked to storage
  → External location + credentials
  → Import repo + switch config to env.azure.yaml
  → Upload landing CSVs + run notebooks 00–06
```

### Step 1: Azure subscription

1. [Create a free Azure account](https://azure.microsoft.com/free/) if needed.
2. Note your **subscription ID** (Portal → Subscriptions).

### Step 2: Install tools on your Mac (one time)

From the project root:

```bash
chmod +x scripts/setup/install_prerequisites.sh
./scripts/setup/install_prerequisites.sh
```

Log in:

```bash
az login
az account set --subscription "<YOUR_SUBSCRIPTION_ID>"
```

Install Databricks CLI auth (after workspace exists):

```bash
databricks auth login --host https://adb-XXXX.azuredatabricks.net
```

### Step 3: Create Azure resources (automated)

Edit variables at the top of the script, then run:

```bash
chmod +x scripts/azure/create_magnolia_infra.sh
./scripts/azure/create_magnolia_infra.sh
```

The script creates:

- Resource group `rg-magnolia-pharma`
- Storage account + `lakehouse` container + landing folders
- Azure Databricks workspace (Premium SKU)

It prints **storage account name** and **workspace URL**. Copy them into `config/env.azure.yaml`.

### Step 4: Unity Catalog on Azure (Portal)

1. Open your **Azure Databricks** workspace.
2. **Catalog** → **Setup metastore** (if prompted) — follow Azure wizard to link ADLS.
3. **Catalog** → **External data** → **External locations** → add:
   - URL: `abfss://lakehouse@<storage>.dfs.core.windows.net/`
   - Credential: workspace access connector / managed identity (wizard defaults usually work).

Grant your user **CREATE CATALOG** or use an existing catalog name in config.

### Step 5: Configure the project

```bash
cp config/env.azure.yaml.template config/env.azure.yaml
# Edit catalog, storage_account, landing_base, checkpoint_base
cp config/env.azure.yaml config/env.yaml
```

Generate and upload sample data:

```bash
python3 scripts/generate_sample_data.py
./scripts/azure/upload_landing_data.sh
```

### Step 6: Run notebooks in Databricks

Same as Path 1: import repo, then **`00_setup_unity_catalog`** → **`01`** … **`06`**.

Apply governance when ready: `governance/unity_catalog_grants.sql` (replace `${catalog}` and group names).

---

## What to do right now

If you are starting from zero **today**:

1. Do **Path 1, Steps 1–3** only.
2. Reply with your workspace type (Free vs Azure) and any error message from the QUICKSTART notebook — we can fix config line-by-line.

---

## Local machine (this repo on your Mac)

| Task | Command |
|------|---------|
| Regenerate sample CSVs | `python3 scripts/generate_sample_data.py` |
| Check local tools | `./scripts/setup/check_prerequisites.sh` |

You do **not** need Spark installed locally; all processing runs in Databricks.
