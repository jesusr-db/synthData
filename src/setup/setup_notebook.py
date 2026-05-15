# Databricks notebook source
# COMMAND ----------
import sys
import yaml
from pathlib import Path

# Load params — injected as widgets or read from conf/params.yml
try:
    catalog_name = dbutils.widgets.get("catalog_name")
    num_units = int(dbutils.widgets.get("num_units"))
except Exception:
    params = yaml.safe_load(Path("/Workspace/conf/params.yml").read_text())
    catalog_name = params["catalog_name"]
    num_units = params["num_units"]

print(f"[INFO] Setup: catalog={catalog_name}, num_units={num_units}")

# COMMAND ----------
# Step 1: Create catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
print(f"[INFO] Catalog ready: {catalog_name}")

# COMMAND ----------
# Step 2: Create schemas
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.staging")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.ref")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.metrics")
print(f"[INFO] Schemas ready: staging, ref, metrics")

# COMMAND ----------
# Step 3: Create staging tables with schema evolution enabled
# Minimal schema — Auto Loader / generator will evolve columns on append
STAGING_TABLES = [
    "order_events",
    "inventory_events",
    "guest_events",
    "loyalty_events",
    "workforce_events",
]

for table in STAGING_TABLES:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.staging.{table} (
            event_type STRING,
            event_id   BIGINT,
            unit_id    BIGINT,
            event_ts   TIMESTAMP
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.columnMapping.mode' = 'name',
            'delta.minReaderVersion'   = '2',
            'delta.minWriterVersion'   = '5'
        )
    """)
    print(f"[INFO] Staging table ready: {catalog_name}.staging.{table}")

# COMMAND ----------
# Step 4: Seed reference tables
from src.generator.reference.seeder import seed_all

seed_all(spark, catalog_name, num_units=num_units)
print(f"[INFO] Reference tables seeded")

# COMMAND ----------
# Step 5: Create UC Metric Views on Gold tables
# Gold tables live in {catalog_name}.silver (DLT target schema)
GOLD_TO_VIEW = {
    "unit_performance_daily":  "unit_performance_daily",
    "sos_compliance_summary":  "sos_compliance_summary",
    "loyalty_cohort_metrics":  "loyalty_cohort_metrics",
    "inventory_waste_summary": "inventory_waste_summary",
}

for gold_table, view_name in GOLD_TO_VIEW.items():
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog_name}.metrics.{view_name}
        AS SELECT * FROM {catalog_name}.silver.{gold_table}
    """)
    print(f"[INFO] Metric view ready: {catalog_name}.metrics.{view_name}")

print("[INFO] Setup complete")
