# Databricks notebook source
# MAGIC %md
# MAGIC # QUICKSTART — AstraZeneca Lakehouse\n\n**Run all** on Serverless. No sql/ folder required.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog")

# COMMAND ----------


import importlib.util
from pathlib import Path


def _load_embedded_module():
    candidates = [Path("_embedded_sql.py"), Path(__file__ if "__file__" in dir() else "_embedded_sql.py")]
    try:
        nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        candidates.insert(0, Path("/Workspace" + nb).parent / "_embedded_sql.py")
        if "/notebooks" in nb:
            candidates.insert(0, Path("/Workspace" + nb.rsplit("/notebooks", 1)[0]) / "notebooks" / "_embedded_sql.py")
    except Exception:
        pass
    for p in candidates:
        if p.exists():
            spec = importlib.util.spec_from_file_location("_embedded_sql", str(p))
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                return mod
    raise FileNotFoundError(
        "notebooks/_embedded_sql.py not found. Git → Pull, or run 11_seed_lakehouse_data.sql on SQL warehouse."
    )


_sql = _load_embedded_module()
load_sql_file = _sql.load_sql_file
run_sql_script = _sql.run_sql_script


catalog = dbutils.widgets.get("catalog").strip() or "main"
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
except Exception as e:
    print(f"CREATE CATALOG skipped ({e})")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

run_sql_script(load_sql_file("seed_astrazeneca.sql"), catalog, "seed")

# COMMAND ----------

spark.sql(f"""
SELECT 'bronze.claims_raw' AS tbl, COUNT(*) AS rows FROM {catalog}.bronze.claims_raw
UNION ALL SELECT 'gold.daily_therapy_metrics', COUNT(*) FROM {catalog}.gold.daily_therapy_metrics
UNION ALL SELECT 'gold.patient_journey', COUNT(*) FROM {catalog}.gold.patient_journey
""").show()

# COMMAND ----------

run_sql_script(load_sql_file("dashboard_views.sql"), catalog, "views")

# COMMAND ----------

spark.sql(f"SELECT * FROM {catalog}.gold.v_dashboard_kpis").show(truncate=False)

# COMMAND ----------

features = spark.table(f"{catalog}.ml.discontinuation_features")
try:
    import mlflow, mlflow.sklearn
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.metrics import roc_auc_score
    from sklearn.model_selection import train_test_split
    pdf = features.toPandas()
    X = pdf[["days_since_last_fill", "fills_last_90d", "pa_denials_last_180d", "crm_outreach_last_30d", "avg_days_supply_90d"]]
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
        print(f"MLflow ROC-AUC={auc:.3f}")
except Exception as e:
    print(f"MLflow skipped: {e}")

