# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Dashboard views
# MAGIC Run after **QUICKSTART** so AI/BI dashboards have data to query.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog").strip() or "main"
spark.sql(f"USE CATALOG {catalog}")

sql_path = "../../sql/dashboard_views.sql"
try:
    sql = open(sql_path).read()
except FileNotFoundError:
    sql = open("sql/dashboard_views.sql").read()

# Replace catalog if user changed widget from main
sql = sql.replace("main.", f"{catalog}.")

for stmt in [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]:
    spark.sql(stmt)
    print(f"OK: {stmt.split(chr(10))[0][:80]}...")

print("Dashboard views ready. Import dashboards/*.lvdash.json in AI/BI.")
