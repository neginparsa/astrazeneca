# AI/BI Dashboards — Magnolia Pharma

Two Databricks **AI/BI** (Lakeview) dashboards for the Gold lakehouse. Import after running **QUICKSTART** and **`08_dashboard_views`**.

## Dashboards

| File | Purpose |
|------|---------|
| `therapy_executive.lvdash.json` | KPI counters, daily trends, revenue by therapy |
| `patient_journey_analytics.lvdash.json` | On-therapy mix, fill gaps, at-risk patient table |

**Palette:** deep blue `#003865`, pharma accent `#830051`, success `#00843D`, alert `#F0AB00`.

## Setup in Databricks Free Edition

1. Run **`notebooks/QUICKSTART_setup_and_run`** (creates Gold tables).
2. Run **`notebooks/09_fix_dashboard_data`** if charts are empty (validates row counts + rebuilds views).
3. **Start your SQL warehouse** (Dashboards need a running warehouse).
4. **Import** each `.lvdash.json` (or **Replace dashboard** if re-importing).
5. **Publish** to share (optional).

### Import path (UI)

**Dashboards** → **Create** → **Import** → choose `therapy_executive.lvdash.json`  
Repeat for `patient_journey_analytics.lvdash.json`.

Or from a Git folder: open the `.lvdash.json` file → **Create dashboard from file**.

## Data sources

All queries use catalog **`main`**, schema **`gold`**:

- `v_dashboard_kpis`
- `v_dashboard_daily_trend`
- `v_dashboard_therapy_summary`
- `v_dashboard_journey_status`
- `v_dashboard_at_risk`
- `patient_journey` (base table)

If your catalog is not `main`, edit SQL in the JSON files or re-run notebook `08` with the correct catalog widget.

## Git folder sync

After **Git → Pull** in folder `astra`, dashboards appear under `dashboards/`. Import once per workspace; edits in UI can be exported back to Git.
