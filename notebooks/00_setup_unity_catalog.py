# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Unity Catalog setup
# MAGIC Creates catalog schemas and Bronze/Silver/Gold/ML tables for Magnolia Pharma.

# COMMAND ----------

# MAGIC %pip install pyyaml -q

# COMMAND ----------

import os
from pathlib import Path

import yaml

# Adjust path if repo is checked out under /Workspace/Repos/
config_path = os.environ.get("MAGNOLIA_CONFIG", "../../config/env.yaml")
if not Path(config_path).exists():
    config_path = "../../config/env.example.yaml"

with open(config_path, encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

catalog = cfg["catalog"]
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
except Exception as e:
    print(f"Using existing catalog (CREATE CATALOG not allowed): {e}")
spark.sql(f"USE CATALOG {catalog}")

for schema in ("bronze", "silver", "gold", "ml"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

def run_sql_file(rel_path: str) -> None:
    sql = Path(rel_path).read_text(encoding="utf-8")
    sql = sql.replace("${catalog}", catalog)
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        spark.sql(stmt)

repo_root = Path("/Workspace/Repos")  # update if different
for name in ("bronze", "silver", "gold"):
    run_sql_file(f"../../schemas/{name}.sql")

print(f"Setup complete for catalog `{catalog}`")
