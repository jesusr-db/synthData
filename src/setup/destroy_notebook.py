# Databricks notebook source
# COMMAND ----------
import sys
import yaml
from pathlib import Path

# Load params — injected as widgets or read from conf/params.yml
try:
    catalog_name = dbutils.widgets.get("catalog_name")
except Exception:
    params = yaml.safe_load(Path("/Workspace/conf/params.yml").read_text())
    catalog_name = params["catalog_name"]

print(f"[INFO] Destroy: catalog={catalog_name}")

# COMMAND ----------
# Step 1: Drop UC Metric Views
METRIC_VIEWS = [
    "unit_performance_daily",
    "sos_compliance_summary",
    "loyalty_cohort_metrics",
    "inventory_waste_summary",
]

for view_name in METRIC_VIEWS:
    spark.sql(f"DROP VIEW IF EXISTS {catalog_name}.metrics.{view_name}")
    print(f"[INFO] Dropped view: {catalog_name}.metrics.{view_name}")

# COMMAND ----------
# Step 2: Drop metrics schema
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.metrics CASCADE")
print(f"[INFO] Dropped schema: {catalog_name}.metrics")

# COMMAND ----------
# Step 3: Drop staging tables
STAGING_TABLES = [
    "order_events",
    "inventory_events",
    "guest_events",
    "loyalty_events",
    "workforce_events",
]

for table in STAGING_TABLES:
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.staging.{table}")
    print(f"[INFO] Dropped table: {catalog_name}.staging.{table}")

# COMMAND ----------
# Step 4: Drop staging schema
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.staging CASCADE")
print(f"[INFO] Dropped schema: {catalog_name}.staging")

# COMMAND ----------
# Step 5: Drop reference tables
REF_TABLES = [
    "unit",
    "franchisee",
    "financial_period",
    "supplier",
    "menu_item",
    "recipe_ingredient",
    "weather_conditions",
    "local_events",
]

for table in REF_TABLES:
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.ref.{table}")
    print(f"[INFO] Dropped table: {catalog_name}.ref.{table}")

# COMMAND ----------
# Step 6: Drop ref schema
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.ref CASCADE")
print(f"[INFO] Dropped schema: {catalog_name}.ref")

# COMMAND ----------
# Note: Gold/Silver schemas are managed by the DLT pipeline and dropped via
# `databricks bundle destroy`. This job only tears down what DAB cannot manage.
print("[INFO] Destroy complete. Run `databricks bundle destroy` to remove DAB-managed resources.")
