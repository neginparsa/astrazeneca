# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver: Structured Streaming + incremental MERGE
# MAGIC Builds tokenized `patient_events` and enriched claims from Bronze streams.

# COMMAND ----------

import os
import sys
from pathlib import Path

import yaml
from pyspark.sql import functions as F

sys.path.append(str(Path("../../src").resolve()))
from magnolia.spark_utils import stable_event_id, tokenize_patient, tokenize_provider

config_path = os.environ.get("MAGNOLIA_CONFIG", "../../config/env.yaml")
if not Path(config_path).exists():
    config_path = "../../config/env.example.yaml"
with open(config_path, encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

catalog = cfg["catalog"]
events_table = f"{catalog}.silver.patient_events"
claims_table = f"{catalog}.silver.claims_enriched"

# COMMAND ----------

def foreach_merge_events(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    silver = (
        batch_df.withColumn("patient_token", tokenize_patient())
        .withColumn(
            "event_id",
            stable_event_id(F.col("claim_id"), F.col("fill_date"), F.lit("CLAIM")),
        )
        .select(
            F.col("event_id"),
            F.col("patient_token"),
            F.lit("CLAIM_FILL").alias("event_type"),
            F.lit(None).cast("string").alias("therapy_code"),
            F.col("fill_date").alias("event_date"),
            F.to_timestamp(F.col("fill_date")).alias("event_ts"),
            F.create_map(
                F.lit("days_supply"),
                F.col("days_supply").cast("string"),
                F.lit("paid_amount"),
                F.col("paid_amount").cast("string"),
            ).alias("detail"),
            F.lit("claims").alias("source_system"),
            F.current_timestamp().alias("updated_at"),
        )
    )
    silver.createOrReplaceTempView("_events")
    spark.sql(
        f"""
        MERGE INTO {events_table} t
        USING _events s ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


claims_stream = (
    spark.readStream.table(f"{catalog}.bronze.claims_raw")
    .writeStream.foreachBatch(foreach_merge_events)
    .option("checkpointLocation", f"{cfg['checkpoint_base']}/silver/events")
    .trigger(processingTime=cfg["streaming"]["trigger"])
    .start()
)

# COMMAND ----------

def foreach_merge_claims(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    enriched = (
        batch_df.withColumn("patient_token", tokenize_patient())
        .withColumn("provider_token", tokenize_provider())
        .withColumn("therapy_code", F.lit("UNKNOWN"))
        .withColumn("updated_at", F.current_timestamp())
        .select(
            "claim_id",
            "patient_token",
            "therapy_code",
            "fill_date",
            "days_supply",
            "paid_amount",
            "provider_token",
            "updated_at",
        )
    )
    enriched.createOrReplaceTempView("_claims")
    spark.sql(
        f"""
        MERGE INTO {claims_table} t
        USING _claims s ON t.claim_id = s.claim_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


(
    spark.readStream.table(f"{catalog}.bronze.claims_raw")
    .writeStream.foreachBatch(foreach_merge_claims)
    .option("checkpointLocation", f"{cfg['checkpoint_base']}/silver/claims")
    .trigger(processingTime=cfg["streaming"]["trigger"])
    .start()
)

print("Silver streaming MERGE jobs started")
