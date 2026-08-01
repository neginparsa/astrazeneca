# Databricks notebook source
# MAGIC %md
# MAGIC # QUICKSTART — AstraZeneca Lakehouse (Free Edition)
# MAGIC
# MAGIC **Run this notebook first.** One click loads everything:
# MAGIC
# MAGIC 1. Unity Catalog schemas (`bronze`, `silver`, `gold`, `ml`)
# MAGIC 2. AstraZeneca synthetic seed (~1,500 patients · ~18,000 claims)
# MAGIC 3. Dashboard SQL views for AI/BI
# MAGIC 4. Optional MLflow discontinuation model
# MAGIC
# MAGIC **Compute:** Serverless only · catalog **`main`**

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog (use main on Free Edition)")

# COMMAND ----------

import os
from pathlib import Path

catalog = dbutils.widgets.get("catalog").strip() or "main"

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
except Exception as e:
    print(f"CREATE CATALOG skipped ({e}); using `{catalog}`")
spark.sql(f"USE CATALOG {catalog}")
print(f"Catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Seed lakehouse data (`sql/seed_astrazeneca.sql`)

# COMMAND ----------


def _repo_paths(filename: str) -> list[Path]:
    paths: list[Path] = []
    try:
        nb = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        root = Path("/Workspace" + nb.rsplit("/notebooks", 1)[0])
        paths.append(root / "sql" / filename)
    except Exception:
        pass
    for rel in (f"../../sql/{filename}", f"../sql/{filename}", f"sql/{filename}"):
        paths.append(Path(rel))
    return paths


def load_sql_file(filename: str) -> str:
    for p in _repo_paths(filename):
        if p.exists():
            print(f"Loaded {p}")
            return p.read_text(encoding="utf-8")
    raise FileNotFoundError(
        f"sql/{filename} not found. Git → Pull the repo, then re-run this notebook."
    )


def run_sql_script(sql_text: str, catalog_name: str, label: str) -> None:
    sql = sql_text.replace("{catalog}", catalog_name).replace("main.", f"{catalog_name}.")
    statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]
    for i, stmt in enumerate(statements, 1):
        spark.sql(stmt)
        head = stmt.split("\n")[0][:70]
        print(f"[{label} {i}/{len(statements)}] OK: {head}...")


run_sql_script(load_sql_file("seed_astrazeneca.sql"), catalog, "seed")

# COMMAND ----------

spark.sql(f"""
SELECT 'bronze.claims_raw' AS tbl, COUNT(*) AS rows FROM {catalog}.bronze.claims_raw
UNION ALL SELECT 'gold.daily_therapy_metrics', COUNT(*) FROM {catalog}.gold.daily_therapy_metrics
UNION ALL SELECT 'gold.patient_journey', COUNT(*) FROM {catalog}.gold.patient_journey
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Dashboard views (`sql/dashboard_views.sql`)

# COMMAND ----------

run_sql_script(load_sql_file("dashboard_views.sql"), catalog, "views")

# COMMAND ----------

spark.sql(f"SELECT * FROM {catalog}.gold.v_dashboard_kpis").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. MLflow — discontinuation risk (optional)

# COMMAND ----------

features = spark.table(f"{catalog}.ml.discontinuation_features")

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
# MAGIC | Next step | Action |
# MAGIC |-----------|--------|
# MAGIC | Browse data | **Catalog** → **`main`** → `bronze` / `silver` / `gold` / `ml` |
# MAGIC | Dashboards | Start **SQL warehouse** → import `dashboards/astrazeneca_*.lvdash.json` |
# MAGIC | Deep dive | Notebooks `00`–`06` for Auto Loader, streaming, Photon |
# MAGIC