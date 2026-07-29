# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Photon vs standard Spark bake-off
# MAGIC Run the same Gold aggregation twice (Photon-enabled cluster vs non-Photon) and compare `spark.conf` timings.

# COMMAND ----------

import os
import time
from pathlib import Path

import yaml
from pyspark.sql import functions as F

config_path = os.environ.get("MAGNOLIA_CONFIG", "../../config/env.yaml")
if not Path(config_path).exists():
    config_path = "../../config/env.example.yaml"
with open(config_path, encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

catalog = cfg["catalog"]
claims = spark.table(f"{catalog}.silver.claims_enriched")

# COMMAND ----------

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


duration_sec = run_benchmark()
photon_enabled = spark.conf.get("spark.databricks.photon.enabled", "unknown")
print(f"Photon enabled: {photon_enabled}")
print(f"Wall time seconds: {duration_sec:.2f}")
print("Repeat on a Photon cluster and a standard cluster; paste results into README.")
