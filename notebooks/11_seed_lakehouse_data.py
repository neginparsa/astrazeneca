# Databricks notebook source
# MAGIC %md
# MAGIC # 11 — AstraZeneca bulk seed (Serverless / Python)
# MAGIC
# MAGIC Same data as **`11_seed_lakehouse_data.sql`**: ~1,500 patients, ~18,000 claims, AZ brands.
# MAGIC
# MAGIC Use **Serverless** (not SQL warehouse).  
# MAGIC Drops existing `main.*` seed tables first to avoid Delta schema merge errors.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("patients", "1500", "Number of patients")
dbutils.widgets.text("fills_per_patient", "12", "Fills per patient")

# COMMAND ----------

import uuid
import random
from datetime import date, timedelta

from pyspark.sql import Row, functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

catalog = dbutils.widgets.get("catalog").strip() or "main"
n_patients = int(dbutils.widgets.get("patients"))
n_fills = int(dbutils.widgets.get("fills_per_patient"))

PRODUCTS = [
    ("AZ-TAG-40", "Tagrisso", "Oncology", "osimertinib", 14500.0),
    ("AZ-IMF-500", "Imfinzi", "Oncology", "durvalumab", 8900.0),
    ("AZ-LYN-150", "Lynparza", "Oncology", "olaparib", 7800.0),
    ("AZ-CAL-100", "Calquence", "Oncology", "acalabrutinib", 9200.0),
    ("AZ-FAR-10", "Farxiga", "CVRM", "dapagliflozin", 520.0),
    ("AZ-BRI-90", "Brilinta", "CVRM", "ticagrelor", 380.0),
    ("AZ-FAS-30", "Fasenra", "Respiratory", "benralizumab", 4200.0),
    ("AZ-TEZ-210", "Tezspire", "Respiratory", "tezepelumab", 5100.0),
    ("AZ-SYM-160", "Symbicort", "Respiratory", "budesonide-formoterol", 285.0),
    ("AZ-ULT-300", "Ultomiris", "Rare Disease", "ravulizumab", 12500.0),
]

random.seed(42)
spark.sql(f"USE CATALOG {catalog}")
for s in ("bronze", "silver", "gold", "ml"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{s}")

# COMMAND ----------

def write_delta(df, table: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )


def drop_seed_tables() -> None:
    tables = [
        f"{catalog}.ml.discontinuation_features",
        f"{catalog}.gold.daily_franchise_metrics",
        f"{catalog}.gold.daily_therapy_metrics",
        f"{catalog}.gold.patient_journey",
        f"{catalog}.gold.az_product_dim",
        f"{catalog}.silver.patient_events",
        f"{catalog}.silver.claims_enriched",
        f"{catalog}.bronze.inventory_raw",
        f"{catalog}.bronze.crm_raw",
        f"{catalog}.bronze.prior_auth_raw",
        f"{catalog}.bronze.specialty_rx_raw",
        f"{catalog}.bronze.claims_raw",
    ]
    for t in tables:
        spark.sql(f"DROP TABLE IF EXISTS {t}")

drop_seed_tables()

# COMMAND ----------

product_rows = [
    Row(
        therapy_code=p[0],
        brand_name=p[1],
        therapeutic_area=p[2],
        molecule=p[3],
        avg_wac_per_fill=round(p[4], 2),
    )
    for p in PRODUCTS
]
products_df = spark.createDataFrame(product_rows)
write_delta(products_df, f"{catalog}.gold.az_product_dim")

# COMMAND ----------

start = date(2024, 6, 1)
claim_rows = []
for i in range(1, n_patients + 1):
    pid = f"AZ-PAT-{i:05d}"
    product = PRODUCTS[i % len(PRODUCTS)]
    therapy_code, brand_name, area, molecule, wac = product
    for f in range(n_fills):
        fill = start + timedelta(days=random.randint(0, 570))
        if area == "Oncology":
            days_supply = 28
        elif area == "Rare Disease":
            days_supply = 14
        elif brand_name == "Farxiga":
            days_supply = 30
        else:
            days_supply = random.choice([28, 30, 90])
        claim_rows.append(
            Row(
                claim_id=str(uuid.uuid4()),
                patient_id=pid,
                provider_npi=str(1000000000 + random.randint(0, 899999999)),
                ndc=f"50242-{random.randint(1000, 9999)}",
                fill_date=fill,
                days_supply=int(days_supply),
                paid_amount=round(wac * random.uniform(0.85, 1.15), 2),
                therapy_code=therapy_code,
                brand_name=brand_name,
                therapeutic_area=area,
                molecule=molecule,
                source_file="az/specialty_pharmacy/claims",
            )
        )

claims_schema = StructType(
    [
        StructField("claim_id", StringType(), False),
        StructField("patient_id", StringType(), False),
        StructField("provider_npi", StringType(), False),
        StructField("ndc", StringType(), False),
        StructField("fill_date", DateType(), False),
        StructField("days_supply", IntegerType(), False),
        StructField("paid_amount", DecimalType(12, 2), False),
        StructField("therapy_code", StringType(), False),
        StructField("brand_name", StringType(), False),
        StructField("therapeutic_area", StringType(), False),
        StructField("molecule", StringType(), False),
        StructField("source_file", StringType(), False),
    ]
)

claims = (
    spark.createDataFrame(claim_rows, schema=claims_schema)
    .withColumn("ingested_at", F.current_timestamp().cast(TimestampType()))
)
write_delta(claims, f"{catalog}.bronze.claims_raw")
print(f"bronze.claims_raw: {claims.count():,} rows")

# COMMAND ----------

token = F.sha2(F.col("patient_id").cast("string"), 256)

silver = (
    spark.table(f"{catalog}.bronze.claims_raw")
    .withColumn("patient_token", token)
    .withColumn("provider_token", F.sha2(F.concat(F.lit("npi:"), F.col("provider_npi").cast("string")), 256))
    .withColumn("updated_at", F.current_timestamp())
    .select(
        "claim_id",
        "patient_token",
        "therapy_code",
        "brand_name",
        "therapeutic_area",
        "molecule",
        "fill_date",
        F.col("days_supply").cast("int"),
        F.col("paid_amount").cast("decimal(12,2)"),
        "provider_token",
        "updated_at",
    )
)
write_delta(silver, f"{catalog}.silver.claims_enriched")

events = (
    spark.table(f"{catalog}.bronze.claims_raw")
    .withColumn("patient_token", token)
    .withColumn(
        "event_id",
        F.sha2(F.concat_ws("|", F.col("claim_id"), F.col("fill_date").cast("string"), F.lit("CLAIM")), 256),
    )
    .select(
        F.col("event_id"),
        F.col("patient_token"),
        F.lit("CLAIM_FILL").alias("event_type"),
        F.col("therapy_code"),
        F.col("brand_name"),
        F.col("therapeutic_area"),
        F.col("fill_date").alias("event_date"),
        F.to_timestamp(F.col("fill_date")).alias("event_ts"),
        F.create_map(
            F.lit("brand"),
            F.col("brand_name"),
            F.lit("paid"),
            F.col("paid_amount").cast("string"),
        ).alias("detail"),
        F.lit("az_specialty_pharmacy").alias("source_system"),
        F.current_timestamp().alias("updated_at"),
    )
)
write_delta(events, f"{catalog}.silver.patient_events")
print(f"silver.claims_enriched: {silver.count():,} rows")

# COMMAND ----------

from pyspark.sql.window import Window

w = Window.partitionBy("patient_token", "therapy_code").orderBy(F.col("fill_date").desc())
journey = (
    silver.withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        F.col("patient_token"),
        F.col("therapy_code"),
        F.col("brand_name"),
        F.col("therapeutic_area"),
        F.date_sub(F.col("fill_date"), 400).alias("journey_start"),
        F.col("fill_date").alias("last_fill_date"),
        F.datediff(F.current_date(), F.col("fill_date")).cast("int").alias("days_since_last_fill"),
        (F.abs(F.hash(F.col("patient_token"))) % 4).cast("int").alias("pa_denial_count"),
        (F.abs(F.hash(F.col("patient_token"))) % 6).cast("int").alias("hub_interactions_30d"),
        (
            F.datediff(F.current_date(), F.col("fill_date"))
            <= F.when(F.col("therapeutic_area") == "Oncology", 35)
            .when(F.col("therapeutic_area") == "Rare Disease", 21)
            .otherwise(45)
        ).alias("on_therapy"),
        F.current_timestamp().alias("journey_updated_at"),
    )
)
write_delta(journey, f"{catalog}.gold.patient_journey")

metrics = (
    silver.groupBy("therapy_code", "brand_name", "therapeutic_area", F.col("fill_date").alias("metric_date"))
    .agg(
        F.countDistinct("patient_token").alias("active_patients"),
        F.sum("paid_amount").alias("total_paid_amount"),
        F.avg("days_supply").alias("avg_days_supply"),
    )
    .withColumn("new_starts", (F.col("active_patients") * 0.04).cast("bigint"))
    .withColumn("discontinuations", (F.col("active_patients") * 0.02).cast("bigint"))
    .withColumn("computed_at", F.current_timestamp())
)
write_delta(metrics, f"{catalog}.gold.daily_therapy_metrics")

franchise = (
    metrics.groupBy("metric_date", "therapeutic_area")
    .agg(
        F.sum("active_patients").alias("active_patients"),
        F.sum("new_starts").alias("new_starts"),
        F.sum("discontinuations").alias("discontinuations"),
        F.sum("total_paid_amount").alias("total_paid_amount"),
        F.countDistinct("brand_name").alias("brands_active"),
    )
)
write_delta(franchise, f"{catalog}.gold.daily_franchise_metrics")

features = journey.select(
    F.col("patient_token"),
    F.col("therapy_code"),
    F.col("brand_name"),
    F.col("therapeutic_area"),
    F.current_date().alias("as_of_date"),
    F.col("days_since_last_fill"),
    F.lit(5).alias("fills_last_90d"),
    F.col("pa_denial_count").alias("pa_denials_last_180d"),
    F.col("hub_interactions_30d").alias("crm_outreach_last_30d"),
    F.lit(30.0).alias("avg_days_supply_90d"),
    (~F.col("on_therapy")).cast("int").alias("label_discontinued_within_30d"),
)
write_delta(features, f"{catalog}.ml.discontinuation_features")

# COMMAND ----------

for t in [
    f"{catalog}.bronze.claims_raw",
    f"{catalog}.gold.daily_therapy_metrics",
    f"{catalog}.gold.daily_franchise_metrics",
    f"{catalog}.gold.patient_journey",
]:
    print(f"{t}: {spark.table(t).count():,} rows")

spark.table(f"{catalog}.gold.daily_therapy_metrics").groupBy("brand_name").sum("total_paid_amount").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Import **`dashboards/astrazeneca_*.lvdash.json`** (SQL warehouse running).
