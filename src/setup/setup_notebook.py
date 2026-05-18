# Databricks notebook source
# COMMAND ----------
import sys
import yaml
from pathlib import Path

# Add bundle root to sys.path so `src.*` imports resolve
_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_notebook_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

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
# Step 1: Verify catalog exists (managed externally — declared in databricks.yml)
catalogs = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
if catalog_name not in catalogs:
    raise ValueError(f"Catalog '{catalog_name}' does not exist. Create it before running setup.")
print(f"[INFO] Catalog verified: {catalog_name}")

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
# Gold tables are created by the DLT pipeline — skip views that don't have a backing table yet.
GOLD_TO_VIEW = {
    "unit_performance_daily":  "unit_performance_daily",
    "sos_compliance_summary":  "sos_compliance_summary",
    "loyalty_cohort_metrics":  "loyalty_cohort_metrics",
    "inventory_waste_summary": "inventory_waste_summary",
}

existing_silver = {
    r.tableName for r in spark.sql(f"SHOW TABLES IN {catalog_name}.silver").collect()
} if spark.catalog.databaseExists(f"{catalog_name}.silver") else set()

for gold_table, view_name in GOLD_TO_VIEW.items():
    if gold_table not in existing_silver:
        print(f"[SKIP] Gold table {catalog_name}.silver.{gold_table} not yet created by DLT pipeline — skipping metric view")
        continue
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog_name}.metrics.{view_name}
        AS SELECT * FROM {catalog_name}.silver.{gold_table}
    """)
    print(f"[INFO] Metric view ready: {catalog_name}.metrics.{view_name}")

print("[INFO] Setup complete")
