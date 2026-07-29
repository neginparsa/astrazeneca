# Dashboard empty? — Manual fix (Free Edition)

## A. Run notebook 10 (required)

**Use the SQL notebook** (works on SQL warehouse):

1. Open **`notebooks/10_dashboard_seed.sql`**
2. Attach your **SQL warehouse** (or Serverless)
3. **Run all**

**Do not** run `10_dashboard_seed_and_test.py` on a SQL warehouse — Python cells fail there.

For Python notebooks (`QUICKSTART`, etc.) use **Serverless** only.

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
