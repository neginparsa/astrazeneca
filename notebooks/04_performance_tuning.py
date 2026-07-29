# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Performance tuning
# MAGIC Liquid clustering, broadcast joins, and incremental MERGE patterns for the patient-journey pipeline.

# COMMAND ----------

import os
from pathlib import Path

import yaml
from pyspark.sql import functions as F

config_path = os.environ.get("MAGNOLIA_CONFIG", "../../config/env.yaml")
if not Path(config_path).exists():
    config_path = "../../config/env.example.yaml"
with open(config_path, encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

catalog = cfg["catalog"]
journey_table = f"{catalog}.gold.patient_journey"
cluster_cols = cfg["patient_journey"]["liquid_cluster_by"]

# COMMAND ----------

# Liquid clustering (DBR 13.3+). Re-run after major data shape changes.
spark.sql(
    f"ALTER TABLE {journey_table} CLUSTER BY ({', '.join(cluster_cols)})"
)

# COMMAND ----------

# Broadcast small dimension (inventory / therapy reference) when joining to large fact
inventory = F.broadcast(spark.table(f"{catalog}.bronze.inventory_raw"))
events = spark.table(f"{catalog}.silver.patient_events")

optimized = (
    events.join(inventory, events.therapy_code == inventory.therapy_code, "left")
    .select(events["*"], inventory["on_hand"])
    .limit(1000)
)

display(optimized)

# COMMAND ----------

# MAGIC %md
# MAGIC Document runtime before/after in your portfolio write-up:
# MAGIC - Monolithic batch join (~3h reference)
# MAGIC - Incremental MERGE + clustering + broadcast (~40m target)
