# Databricks notebook source
# MAGIC %md
# MAGIC # 11 — Bulk seed (Serverless / Python)
# MAGIC ~500 patients · ~5,000 claims · Bronze / Silver / Gold / ML  
# MAGIC Use **Serverless** compute (not SQL warehouse).

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("patients", "500", "Number of patients")
dbutils.widgets.text("fills_per_patient", "10", "Fills per patient")

# COMMAND ----------

import uuid
import random
from datetime import date, timedelta

from pyspark.sql import Row, functions as F

catalog = dbutils.widgets.get("catalog").strip() or "main"
n_patients = int(dbutils.widgets.get("patients"))
n_fills = int(dbutils.widgets.get("fills_per_patient"))
therapies = ["MAGN-101", "MAGN-204", "MAGN-330", "MAGN-410", "MAGN-522"]

random.seed(42)
spark.sql(f"USE CATALOG {catalog}")
for s in ("bronze", "silver", "gold", "ml"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{s}")

# COMMAND ----------

start = date(2025, 1, 1)
claim_rows = []
for i in range(1, n_patients + 1):
    pid = f"PAT-{i:05d}"
    therapy = therapies[i % len(therapies)]
    for _ in range(n_fills):
        fill = start + timedelta(days=random.randint(0, 540))
        claim_rows.append(
            Row(
                claim_id=str(uuid.uuid4()),
                patient_id=pid,
                provider_npi=str(1000000000 + random.randint(0, 899999999)),
                ndc=f"00000-{random.randint(1000, 9999)}",
                fill_date=fill,
                days_supply=random.choice([28, 30, 90]),
                paid_amount=round(random.uniform(500, 8500), 2),
                therapy_code=therapy,
                source_file="seed/bulk",
            )
        )

claims = (
    spark.createDataFrame(claim_rows)
    .withColumn("ingested_at", F.current_timestamp())
)
claims.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.bronze.claims_raw")
print(f"bronze.claims_raw: {claims.count()} rows")

# COMMAND ----------

token = F.sha2(F.col("patient_id").cast("string"), 256)
silver = (
    spark.table(f"{catalog}.bronze.claims_raw")
    .withColumn("patient_token", token)
    .withColumn("provider_token", F.sha2(F.concat(F.lit("npi:"), F.col("provider_npi").cast("string")), 256))
    .withColumn("updated_at", F.current_timestamp())
    .select("claim_id", "patient_token", "therapy_code", "fill_date", "days_supply", "paid_amount", "provider_token", "updated_at")
)
silver.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.silver.claims_enriched")

events = (
    spark.table(f"{catalog}.bronze.claims_raw")
    .withColumn("patient_token", token)
    .withColumn("event_id", F.sha2(F.concat_ws("|", F.col("claim_id"), F.col("fill_date").cast("string"), F.lit("CLAIM")), 256))
    .select(
        F.col("event_id"), F.col("patient_token"), F.lit("CLAIM_FILL").alias("event_type"),
        F.col("therapy_code"), F.col("fill_date").alias("event_date"),
        F.to_timestamp(F.col("fill_date")).alias("event_ts"),
        F.create_map(F.lit("days_supply"), F.col("days_supply").cast("string")).alias("detail"),
        F.lit("claims").alias("source_system"), F.current_timestamp().alias("updated_at"),
    )
)
events.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.silver.patient_events")
print(f"silver.claims_enriched: {silver.count()} rows")

# COMMAND ----------

from pyspark.sql.window import Window

w = Window.partitionBy("patient_token", "therapy_code").orderBy(F.col("fill_date").desc())
journey = (
    silver.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
    .select(
        F.col("patient_token"), F.col("therapy_code"),
        F.date_sub(F.col("fill_date"), 365).alias("journey_start"),
        F.col("fill_date").alias("last_fill_date"),
        F.datediff(F.current_date(), F.col("fill_date")).alias("days_since_last_fill"),
        (F.abs(F.hash(F.col("patient_token"))) % 3).alias("pa_denial_count"),
        (F.abs(F.hash(F.col("patient_token"))) % 5).alias("hub_interactions_30d"),
        (F.datediff(F.current_date(), F.col("fill_date")) <= 45).alias("on_therapy"),
        F.current_timestamp().alias("journey_updated_at"),
    )
)
journey.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.gold.patient_journey")

metrics = (
    silver.groupBy(F.col("therapy_code"), F.col("fill_date").alias("metric_date"))
    .agg(
        F.countDistinct("patient_token").alias("active_patients"),
        F.sum("paid_amount").alias("total_paid_amount"),
        F.avg("days_supply").alias("avg_days_supply"),
    )
    .withColumn("new_starts", (F.col("active_patients") * 0.05).cast("bigint"))
    .withColumn("discontinuations", (F.col("active_patients") * 0.02).cast("bigint"))
    .withColumn("computed_at", F.current_timestamp())
)
metrics.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.gold.daily_therapy_metrics")

features = journey.select(
    F.col("patient_token"), F.col("therapy_code"), F.current_date().alias("as_of_date"),
    F.col("days_since_last_fill"), F.lit(5).alias("fills_last_90d"),
    F.col("pa_denial_count").alias("pa_denials_last_180d"),
    F.col("hub_interactions_30d").alias("crm_outreach_last_30d"),
    F.lit(30.0).alias("avg_days_supply_90d"),
    (~F.col("on_therapy")).cast("int").alias("label_discontinued_within_30d"),
)
features.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.ml.discontinuation_features")

# COMMAND ----------

for t in [
    f"{catalog}.gold.daily_therapy_metrics",
    f"{catalog}.gold.patient_journey",
    f"{catalog}.ml.discontinuation_features",
]:
    print(f"{t}: {spark.table(t).count()} rows")

spark.table(f"{catalog}.gold.daily_therapy_metrics").orderBy(F.col("metric_date").desc()).limit(10).show()
