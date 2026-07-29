# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Photon vs standard Spark bake-off
# MAGIC
# MAGIC Benchmarks the **Gold daily metrics** aggregation (same query your lakehouse uses).
# MAGIC
# MAGIC | Environment | What to do |
# MAGIC |-------------|------------|
# MAGIC | **Free Edition (Serverless)** | Run this notebook once — records timing on Serverless. True Photon vs non-Photon comparison **requires Azure paid clusters** (skip for portfolio demo). |
# MAGIC | **Azure Databricks** | Run once on a **Photon** job cluster, once on **standard** — paste both timings below. |
# MAGIC
# MAGIC **Prerequisite:** Run **`11_seed_lakehouse_data`** first so `silver.claims_enriched` has data.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")

# COMMAND ----------

import time
from pyspark.sql import functions as F

catalog = dbutils.widgets.get("catalog").strip() or "main"
table = f"{catalog}.silver.claims_enriched"

try:
    row_count = spark.table(table).count()
except Exception as e:
    raise RuntimeError(
        f"Table {table} not found or empty. Run notebooks/11_seed_lakehouse_data first."
    ) from e

print(f"Using {table} — {row_count:,} rows")

# COMMAND ----------

claims = spark.table(table)

def run_benchmark():
    spark.sparkContext.setJobDescription("magnolia_gold_daily_metrics_bakeoff")
    start = time.time()
    (
        claims.groupBy("therapy_code")
        .agg(
            F.countDistinct("patient_token").alias("patients"),
            F.sum("paid_amount").alias("paid"),
            F.avg("days_supply").alias("avg_supply"),
        )
        .collect()
    )
    return time.time() - start

# Warm-up (JIT / cache planning)
_ = run_benchmark()

duration_sec = run_benchmark()
photon_enabled = spark.conf.get("spark.databricks.photon.enabled", "unknown")

# COMMAND ----------

print("=" * 50)
print("MAGNOLIA GOLD BENCHMARK RESULTS")
print("=" * 50)
print(f"Catalog / table : {table}")
print(f"Row count       : {row_count:,}")
print(f"Photon enabled  : {photon_enabled}")
print(f"Wall time (sec) : {duration_sec:.2f}")
print("=" * 50)

if photon_enabled == "true":
    print("Serverless/Photon path detected. Save this time for your write-up.")
else:
    print("Free Edition: Serverless only — note this time as your baseline.")
    print("For resume story (~3h → ~40m), run again on Azure Photon cluster later.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Portfolio template (fill in after Azure run)
# MAGIC
# MAGIC | Run | Compute | Seconds |
# MAGIC |-----|---------|--------|
# MAGIC | Baseline (monolithic batch) | Standard cluster | *e.g. 10,800* |
# MAGIC | Optimized (MERGE + clustering) | Photon | *paste above* |
