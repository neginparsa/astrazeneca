#!/usr/bin/env python3
"""Regenerate notebooks/_embedded_sql.py from sql/*.sql."""
from pathlib import Path

root = Path(__file__).resolve().parents[1]
seed = (root / "sql/seed_astrazeneca.sql").read_text()
dash = (root / "sql/dashboard_views.sql").read_text()
(root / "notebooks/_embedded_sql.py").write_text(
    "# Databricks notebook source\n"
    "EMBEDDED_SQL = {\n"
    f"    \"seed_astrazeneca.sql\": {seed!r},\n"
    f"    \"dashboard_views.sql\": {dash!r},\n"
    "}\n"
)
print("Wrote notebooks/_embedded_sql.py")
