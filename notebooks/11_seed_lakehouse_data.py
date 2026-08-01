# Databricks notebook source
# MAGIC %md
# MAGIC # 11 — AstraZeneca bulk seed (Serverless)\n\nOr use **11_seed_lakehouse_data.sql** on SQL warehouse.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")

# COMMAND ----------


import importlib.util
from pathlib import Path


def _load_embedded_module():
    candidates = [Path("_embedded_sql.py"), Path(__file__ if "__file__" in dir() else "_embedded_sql.py")]
    try:
        nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        candidates.insert(0, Path("/Workspace" + nb).parent / "_embedded_sql.py")
        if "/notebooks" in nb:
            candidates.insert(0, Path("/Workspace" + nb.rsplit("/notebooks", 1)[0]) / "notebooks" / "_embedded_sql.py")
    except Exception:
        pass
    for p in candidates:
        if p.exists():
            spec = importlib.util.spec_from_file_location("_embedded_sql", str(p))
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                return mod
    raise FileNotFoundError(
        "notebooks/_embedded_sql.py not found. Git → Pull, or run 11_seed_lakehouse_data.sql on SQL warehouse."
    )


_sql = _load_embedded_module()
load_sql_file = _sql.load_sql_file
run_sql_script = _sql.run_sql_script


catalog = dbutils.widgets.get("catalog").strip() or "main"
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

run_sql_script(load_sql_file("seed_astrazeneca.sql"), catalog, "seed")

# COMMAND ----------

spark.sql(f"""
SELECT 'bronze.claims_raw' AS tbl, COUNT(*) AS rows FROM {catalog}.bronze.claims_raw
UNION ALL SELECT 'gold.daily_therapy_metrics', COUNT(*) FROM {catalog}.gold.daily_therapy_metrics
""").show()

