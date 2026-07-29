# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze ingestion (Auto Loader)
# MAGIC Ingest ADLS Gen2 landing CSVs into Bronze Delta with schema evolution and checkpoints.

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
landing = cfg["landing_base"]
checkpoint = f"{cfg['checkpoint_base']}/bronze"

# COMMAND ----------

sources = {
    "claims": ("claims", f"{catalog}.bronze.claims_raw"),
    "specialty_rx": ("specialty_rx", f"{catalog}.bronze.specialty_rx_raw"),
    "prior_auth": ("prior_auth", f"{catalog}.bronze.prior_auth_raw"),
    "crm": ("crm", f"{catalog}.bronze.crm_raw"),
    "inventory": ("inventory", f"{catalog}.bronze.inventory_raw"),
}

for name, (folder, table) in sources.items():
    path = f"{landing}/{folder}"
    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoint}/{name}/schema")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued")
        .load(path)
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingested_at", F.current_timestamp())
        .writeStream.format("delta")
        .option("checkpointLocation", f"{checkpoint}/{name}/data")
        .trigger(processingTime=cfg["streaming"]["trigger"])
        .toTable(table)
    )
    print(f"Started Auto Loader stream -> {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC Upload sample files from `local-data/landing/` to ADLS paths above, or run `scripts/generate_sample_data.py` locally first.
