# Databricks notebook source
# COMMAND ----------
import sys

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_nb_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

# COMMAND ----------
try:
    catalog_name = dbutils.widgets.get("catalog_name")
except Exception:
    catalog_name = "jmrdemo"

try:
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    schema_prefix = "synth_"

print(f"[INFO] Destroy: catalog={catalog_name}, schema_prefix={schema_prefix}")

# COMMAND ----------
# Step 1: Drop UC Metric Views
METRIC_VIEWS = [
    "unit_performance_daily",
    "sos_compliance_summary",
    "loyalty_cohort_metrics",
    "inventory_waste_summary",
]

for view_name in METRIC_VIEWS:
    spark.sql(f"DROP VIEW IF EXISTS {catalog_name}.{schema_prefix}metrics.{view_name}")
    print(f"[INFO] Dropped view: {catalog_name}.{schema_prefix}metrics.{view_name}")

# COMMAND ----------
# Step 2: Drop metrics schema
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_prefix}metrics CASCADE")
print(f"[INFO] Dropped schema: {catalog_name}.{schema_prefix}metrics")

# COMMAND ----------
# Step 3: Drop reference tables
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
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_prefix}ref.{table}")
    print(f"[INFO] Dropped table: {catalog_name}.{schema_prefix}ref.{table}")

# COMMAND ----------
# Step 4: Drop ref schema
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_prefix}ref CASCADE")
print(f"[INFO] Dropped schema: {catalog_name}.{schema_prefix}ref")

# COMMAND ----------
# Note: staging schema is intentionally preserved so historical data survives destroy/redeploy cycles.
# Gold/Silver schemas are managed by the DLT pipeline and dropped via `databricks bundle destroy`.
print(f"[INFO] Destroy complete. {schema_prefix}staging schema preserved. Run `databricks bundle destroy` to remove DAB-managed resources.")
