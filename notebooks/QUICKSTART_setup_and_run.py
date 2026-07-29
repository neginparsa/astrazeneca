# Databricks notebook source
# MAGIC %md
# MAGIC # QUICKSTART — Magnolia Pharma Lakehouse (Free Edition)
# MAGIC
# MAGIC **Run this notebook first on Free Edition.** Serverless only · catalog **`main`** · no Azure · no DBFS.
# MAGIC
# MAGIC Creates Bronze → Silver → Gold + optional MLflow.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog (use main on Free Edition)")

# COMMAND ----------

import os
import uuid
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import Row, functions as F

# Free Edition: avoid pip installs (limited outbound network). Config is inlined below.
DEFAULT_CFG = {
    "mode": "demo",
    "catalog": "main",
    "landing_base": "",  # unused on Free Edition — data goes straight to Delta tables
}

def load_cfg() -> dict:
    catalog = dbutils.widgets.get("catalog").strip() or "main"
    custom = os.environ.get("MAGNOLIA_CONFIG", "")
    paths = []
    if custom:
        paths.append(Path(custom))
    for rel in ("../../config/env.yaml", "../config/env.yaml", "config/env.yaml"):
        paths.append(Path(rel))
    for p in paths:
        if p.exists():
            try:
                import yaml

                with open(p, encoding="utf-8") as f:
                    cfg = yaml.safe_load(f)
                cfg["catalog"] = catalog
                print(f"Loaded config from {p}")
                return cfg
            except Exception as e:
                print(f"Could not parse {p}: {e}")
    cfg = dict(DEFAULT_CFG)
    cfg["catalog"] = catalog
    print("Using inlined Free Edition config (no env.yaml required).")
    return cfg


cfg = load_cfg()
catalog = cfg["catalog"]
print(f"Catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Catalog & schemas

# COMMAND ----------

def ensure_catalog(name: str) -> None:
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {name}")
    except Exception as e:
        print(f"CREATE CATALOG skipped ({e}); using existing catalog `{name}`")
    spark.sql(f"USE CATALOG {name}")


ensure_catalog(catalog)
for schema in ("bronze", "silver", "gold", "ml"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.bronze.claims_raw (
  claim_id STRING, patient_id STRING, provider_npi STRING, ndc STRING,
  fill_date DATE, days_supply INT, paid_amount DECIMAL(12,2),
  source_file STRING, ingested_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS {catalog}.bronze.specialty_rx_raw (
  rx_id STRING, patient_id STRING, therapy_code STRING, ship_date DATE,
  quantity INT, hub_status STRING, source_file STRING, ingested_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS {catalog}.silver.patient_events (
  event_id STRING, patient_token STRING, event_type STRING, therapy_code STRING,
  event_date DATE, event_ts TIMESTAMP, detail MAP<STRING, STRING>,
  source_system STRING, updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS {catalog}.silver.claims_enriched (
  claim_id STRING, patient_token STRING, therapy_code STRING, fill_date DATE,
  days_supply INT, paid_amount DECIMAL(12,2), provider_token STRING, updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS {catalog}.gold.patient_journey (
  patient_token STRING, therapy_code STRING, journey_start DATE, last_fill_date DATE,
  days_since_last_fill INT, pa_denial_count INT, hub_interactions_30d INT,
  on_therapy BOOLEAN, journey_updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS {catalog}.gold.daily_therapy_metrics (
  metric_date DATE, therapy_code STRING, active_patients BIGINT, new_starts BIGINT,
  discontinuations BIGINT, avg_days_supply DOUBLE, total_paid_amount DECIMAL(18,2),
  computed_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS {catalog}.ml.discontinuation_features (
  patient_token STRING, therapy_code STRING, as_of_date DATE, days_since_last_fill INT,
  fills_last_90d INT, pa_denials_last_180d INT, crm_outreach_last_30d INT,
  avg_days_supply_90d DOUBLE, label_discontinued_within_30d INT
) USING DELTA;
"""

for stmt in [s.strip() for s in DDL.format(catalog=catalog).split(";") if s.strip()]:
    spark.sql(stmt)

print("Tables ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Synthetic data → Bronze (Delta tables; no DBFS on Free Edition)

# COMMAND ----------

import random

random.seed(42)
therapies = ["MAGN-101", "MAGN-204", "MAGN-330"]
patients = [f"PAT-{i:06d}" for i in range(1, 201)]
start = date.today() - timedelta(days=120)
claim_rows = []
for pid in patients:
    for _ in range(random.randint(2, 6)):
        fill = start + timedelta(days=random.randint(0, 120))
        claim_rows.append(
            Row(
                claim_id=str(uuid.uuid4()),
                patient_id=pid,
                provider_npi=str(random.randint(1000000000, 1999999999)),
                ndc=f"00000-{random.randint(1000, 9999)}",
                fill_date=fill,
                days_supply=random.choice([28, 30, 90]),
                paid_amount=round(random.uniform(500, 8500), 2),
            )
        )

claims_df = spark.createDataFrame(claim_rows)
claims_df.limit(5).show()

# COMMAND ----------

bronze_claims = (
    claims_df.withColumn("source_file", F.lit("quickstart/synthetic"))
    .withColumn("ingested_at", F.current_timestamp())
)
bronze_claims.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.bronze.claims_raw")
print(f"Bronze claims loaded: {bronze_claims.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver (tokenized)

# COMMAND ----------

token = F.sha2(F.col("patient_id").cast("string"), 256)
provider_token = F.sha2(F.concat(F.lit("npi:"), F.col("provider_npi").cast("string")), 256)

silver_claims = (
    spark.table(f"{catalog}.bronze.claims_raw")
    .withColumn("patient_token", token)
    .withColumn("provider_token", provider_token)
    .withColumn("therapy_code", F.lit("MAGN-101"))
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
silver_claims.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.silver.claims_enriched")

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
        F.lit("MAGN-101").alias("therapy_code"),
        F.col("fill_date").alias("event_date"),
        F.to_timestamp(F.col("fill_date")).alias("event_ts"),
        F.create_map(F.lit("days_supply"), F.col("days_supply").cast("string")).alias("detail"),
        F.lit("claims").alias("source_system"),
        F.current_timestamp().alias("updated_at"),
    )
)
events.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.silver.patient_events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold metrics & patient journey

# COMMAND ----------

from pyspark.sql.window import Window

claims = spark.table(f"{catalog}.silver.claims_enriched")
w = Window.partitionBy("patient_token", "therapy_code").orderBy(F.col("fill_date").desc())

journey = (
    claims.withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "patient_token",
        "therapy_code",
        F.date_sub(F.col("fill_date"), 365).alias("journey_start"),
        F.col("fill_date").alias("last_fill_date"),
        F.datediff(F.current_date(), F.col("fill_date")).alias("days_since_last_fill"),
        F.lit(0).alias("pa_denial_count"),
        F.lit(0).alias("hub_interactions_30d"),
        (F.datediff(F.current_date(), F.col("fill_date")) <= 45).alias("on_therapy"),
        F.current_timestamp().alias("journey_updated_at"),
    )
)
journey.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.gold.patient_journey")

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
metrics.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.gold.daily_therapy_metrics")

spark.table(f"{catalog}.gold.daily_therapy_metrics").orderBy(F.col("metric_date").desc()).limit(10).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. MLflow — discontinuation risk (optional)

# COMMAND ----------

features = journey.select(
    F.col("patient_token"),
    F.col("therapy_code"),
    F.current_date().alias("as_of_date"),
    F.col("days_since_last_fill"),
    F.lit(3).alias("fills_last_90d"),
    F.col("pa_denial_count").alias("pa_denials_last_180d"),
    F.col("hub_interactions_30d").alias("crm_outreach_last_30d"),
    F.lit(30.0).alias("avg_days_supply_90d"),
    (~F.col("on_therapy")).cast("int").alias("label_discontinued_within_30d"),
)
features.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.ml.discontinuation_features")

try:
    import mlflow
    import mlflow.sklearn
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.metrics import roc_auc_score
    from sklearn.model_selection import train_test_split

    pdf = features.toPandas()
    X = pdf[
        [
            "days_since_last_fill",
            "fills_last_90d",
            "pa_denials_last_180d",
            "crm_outreach_last_30d",
            "avg_days_supply_90d",
        ]
    ]
    y = pdf["label_discontinued_within_30d"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    user = spark.sql("SELECT current_user()").collect()[0][0]
    mlflow.set_experiment(f"/Users/{user}/magnolia_discontinuation")
    with mlflow.start_run(run_name="quickstart_v1"):
        model = GradientBoostingClassifier(random_state=42)
        model.fit(X_train, y_train)
        auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
        mlflow.log_metric("roc_auc", float(auc))
        mlflow.sklearn.log_model(model, artifact_path="model")
        print(f"MLflow run complete — ROC-AUC={auc:.3f}")
except Exception as e:
    print(f"MLflow step skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Browse **Catalog** → **`main`** → schemas `bronze`, `silver`, `gold`, `ml`.
# MAGIC
# MAGIC On Free Edition skip notebooks **`01`** and **`02`**. Optional: **`05`** for more MLflow.
