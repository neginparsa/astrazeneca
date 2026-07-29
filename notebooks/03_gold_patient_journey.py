# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: patient journey & daily therapy metrics
# MAGIC Produces governed Gold tables — the single trusted source for daily reporting.

# COMMAND ----------

import os
from pathlib import Path

import yaml
from pyspark.sql import functions as F
from pyspark.sql.window import Window

config_path = os.environ.get("MAGNOLIA_CONFIG", "../../config/env.yaml")
if not Path(config_path).exists():
    config_path = "../../config/env.example.yaml"
with open(config_path, encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

catalog = cfg["catalog"]
events = spark.table(f"{catalog}.silver.patient_events")
claims = spark.table(f"{catalog}.silver.claims_enriched")

# COMMAND ----------

fill_events = events.filter(F.col("event_type") == "CLAIM_FILL")
w = Window.partitionBy("patient_token", "therapy_code").orderBy(F.col("event_date").desc())

journey = (
    fill_events.withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        F.col("patient_token"),
        F.coalesce(F.col("therapy_code"), F.lit("UNKNOWN")).alias("therapy_code"),
        F.col("event_date").alias("last_fill_date"),
        F.datediff(F.current_date(), F.col("event_date")).alias("days_since_last_fill"),
    )
    .withColumn("journey_start", F.date_sub(F.col("last_fill_date"), 365))
    .withColumn("pa_denial_count", F.lit(0))
    .withColumn("hub_interactions_30d", F.lit(0))
    .withColumn("on_therapy", F.col("days_since_last_fill") <= F.lit(45))
    .withColumn("journey_updated_at", F.current_timestamp())
    .drop("rn")
)

(
    journey.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.patient_journey")
)

# COMMAND ----------

metrics = (
    claims.groupBy(F.col("therapy_code"), F.col("fill_date").alias("metric_date"))
    .agg(
        F.countDistinct("patient_token").alias("active_patients"),
        F.sum("paid_amount").alias("total_paid_amount"),
        F.avg("days_supply").alias("avg_days_supply"),
    )
    .withColumn("new_starts", F.lit(0))
    .withColumn("discontinuations", F.lit(0))
    .withColumn("computed_at", F.current_timestamp())
)

(
    metrics.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.daily_therapy_metrics")
)

display(spark.table(f"{catalog}.gold.daily_therapy_metrics").limit(20))
