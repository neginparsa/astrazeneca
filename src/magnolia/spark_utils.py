"""Shared helpers for Magnolia Pharma lakehouse notebooks."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def load_config(path: str | Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def fq(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def tokenize_patient(patient_id_col: str = "patient_id") -> F.Column:
    """One-way token for PHI-safe Silver/Gold (not reversible without map table)."""

    return F.sha2(F.col(patient_id_col).cast("string"), 256)


def tokenize_provider(npi_col: str = "provider_npi") -> F.Column:
    return F.sha2(F.concat(F.lit("npi:"), F.col(npi_col).cast("string")), 256)


def merge_delta(
    spark: SparkSession,
    target_table: str,
    source_df: DataFrame,
    merge_condition: str,
    update_columns: dict[str, str] | None = None,
) -> None:
    """Incremental MERGE wrapper used by Silver streaming jobs."""

    source_df.createOrReplaceTempView("_merge_source")
    set_clause = ", ".join(f"t.{k} = s.{k}" for k in (update_columns or {})) or "t.updated_at = s.updated_at"
    insert_cols = ", ".join(source_df.columns)
    insert_vals = ", ".join(f"s.{c}" for c in source_df.columns)

    spark.sql(
        f"""
        MERGE INTO {target_table} AS t
        USING _merge_source AS s
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )


def stable_event_id(*cols: F.Column) -> F.Column:
    payload = F.concat_ws("|", *[c.cast("string") for c in cols])
    return F.sha2(payload, 256)
