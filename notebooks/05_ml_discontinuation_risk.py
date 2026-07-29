# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — MLflow: therapy discontinuation risk
# MAGIC Trains a classifier on Gold/Silver features for proactive patient outreach.

# COMMAND ----------

import os
from pathlib import Path

import mlflow
import mlflow.sklearn
import yaml
from pyspark.sql import functions as F
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

config_path = os.environ.get("MAGNOLIA_CONFIG", "../../config/env.yaml")
if not Path(config_path).exists():
    config_path = "../../config/env.example.yaml"
with open(config_path, encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

catalog = cfg["catalog"]
journey = spark.table(f"{catalog}.gold.patient_journey")

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

(
    features.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.ml.discontinuation_features")
)

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

# COMMAND ----------

mlflow.set_experiment(f"/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/magnolia_discontinuation")

with mlflow.start_run(run_name="therapy_discontinuation_v1"):
    model = GradientBoostingClassifier(random_state=42)
    model.fit(X_train, y_train)
    preds = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, preds)
    mlflow.log_metric("roc_auc", auc)
    mlflow.sklearn.log_model(
        model,
        artifact_path="model",
        registered_model_name=f"{catalog}.ml.therapy_discontinuation_risk",
    )
    print(f"Registered model — ROC-AUC={auc:.3f}")
