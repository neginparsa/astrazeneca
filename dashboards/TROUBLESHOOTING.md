# Dashboard empty? — Manual fix (Free Edition)

## A. Run notebook 10 (required)

1. Open **`notebooks/10_dashboard_seed_and_test`**
2. **Serverless** → **Run all**
3. Confirm output shows **7 rows** in `daily_therapy_metrics` and **6 rows** in `patient_journey`

This loads **fixed demo data** into `main.gold` using SQL.

## B. Start SQL warehouse (required)

1. Left sidebar → **SQL Warehouses**
2. Click your warehouse → **Start**
3. Wait until **Running** (green)

Dashboards **will not show data** if the warehouse is stopped.

## C. Smoke test dashboard

1. **Dashboards** → **Import** → `magnolia_smoke_test.lvdash.json`
2. Pick the **running** warehouse
3. You should see counters: **716** (patients) and **7** (rows)

If smoke test works → import `therapy_executive.lvdash.json` again.

## D. Build one chart manually (if import still empty)

1. **Dashboards** → **Create dashboard**
2. **Add data** → **New dataset from SQL**:

```sql
SELECT metric_date, therapy_code, active_patients
FROM main.gold.daily_therapy_metrics
ORDER BY metric_date
```

3. **Add visualization** → Bar chart
   - X: `metric_date`
   - Y: `active_patients`
   - Color: `therapy_code`

If manual chart works but JSON import does not → use manual build for portfolio screenshots.

## E. Catalog not `main`?

If notebook 10 used another catalog, replace `main` in dashboard SQL with your catalog name (widget on notebook 10).
