# Databricks notebook source
# MAGIC %md
# MAGIC # 11 — AstraZeneca bulk seed (Serverless)
# MAGIC
# MAGIC Runs **`sql/seed_astrazeneca.sql`** via `spark.sql` — avoids Spark Connect `createDataFrame` errors.
# MAGIC
# MAGIC ~1,500 patients · ~18,000 claims · Tagrisso, Imfinzi, Farxiga, etc.
# MAGIC
# MAGIC **Prefer SQL warehouse?** Use **`11_seed_lakehouse_data.sql`** instead (same data).

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")

# COMMAND ----------

import os
from pathlib import Path

catalog = dbutils.widgets.get("catalog").strip() or "main"
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

def _repo_paths() -> list[Path]:
    paths: list[Path] = []
    try:
        nb = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        root = Path("/Workspace" + nb.rsplit("/notebooks", 1)[0])
        paths.append(root / "sql" / "seed_astrazeneca.sql")
    except Exception:
        pass
    paths.extend(
        [
            Path("../../sql/seed_astrazeneca.sql"),
            Path("../sql/seed_astrazeneca.sql"),
            Path("sql/seed_astrazeneca.sql"),
        ]
    )
    return paths


def load_seed_sql() -> str:
    for p in _repo_paths():
        if p.exists():
            print(f"Loaded {p}")
            return p.read_text(encoding="utf-8")
    raise FileNotFoundError(
        "sql/seed_astrazeneca.sql not found. Git pull the repo or run 11_seed_lakehouse_data.sql"
    )


def run_seed(sql_text: str, catalog_name: str) -> None:
    sql = sql_text.replace("{catalog}", catalog_name)
    statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]
    for i, stmt in enumerate(statements, 1):
        spark.sql(stmt)
        head = stmt.split("\n")[0][:70]
        print(f"[{i}/{len(statements)}] OK: {head}...")


run_seed(load_seed_sql(), catalog)

# COMMAND ----------

spark.sql(f"""
SELECT 'bronze.claims_raw' AS tbl, COUNT(*) AS rows FROM {catalog}.bronze.claims_raw
UNION ALL SELECT 'gold.daily_therapy_metrics', COUNT(*) FROM {catalog}.gold.daily_therapy_metrics
UNION ALL SELECT 'gold.patient_journey', COUNT(*) FROM {catalog}.gold.patient_journey
""").show()

# COMMAND ----------

spark.sql(f"""
SELECT brand_name, therapeutic_area, SUM(active_patients) AS fills, ROUND(SUM(total_paid_amount), 0) AS paid
FROM {catalog}.gold.daily_therapy_metrics
GROUP BY brand_name, therapeutic_area
ORDER BY paid DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC SQL warehouse **Running** → import **`dashboards/astrazeneca_*.lvdash.json`**
