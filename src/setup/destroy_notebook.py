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
# Step 0a: Drop column masks from staging tables BEFORE dropping ref schema/functions.
# Masks on staging.guest_events reference synth_ref.mask_email/mask_phone. If these
# functions are dropped while the masks remain, any query on guest_events fails with
# UC_DEPENDENCY_DOES_NOT_EXIST — including DLT streaming reads and backfill queries.
for col in ["email", "phone"]:
    try:
        spark.sql(
            f"ALTER TABLE {catalog_name}.{schema_prefix}staging.guest_events "
            f"ALTER COLUMN {col} DROP MASK"
        )
        print(f"[INFO] Dropped mask on staging.guest_events.{col}")
    except Exception as e:
        print(f"[WARN] Drop mask on guest_events.{col} skipped: {e}")

for col in ["email", "phone"]:
    try:
        spark.sql(
            f"ALTER TABLE {catalog_name}.{schema_prefix}silver.guest_profile "
            f"ALTER COLUMN {col} DROP MASK"
        )
        print(f"[INFO] Dropped mask on silver.guest_profile.{col}")
    except Exception as e:
        print(f"[WARN] Drop mask on guest_profile.{col} skipped: {e}")

# COMMAND ----------
# Step 0d: Delete Lakehouse Monitors — non-fatal
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import NotFound
    w = WorkspaceClient()
    for table in ["order_events", "inventory_events", "loyalty_events"]:
        full_name = f"{catalog_name}.{schema_prefix}staging.{table}"
        try:
            w.quality_monitors.delete(table_name=full_name)
            print(f"[INFO] Monitor deleted: {full_name}")
        except NotFound:
            print(f"[INFO] Monitor not found (ok): {full_name}")
        except Exception as e:
            print(f"[WARN] Monitor delete skipped for {full_name}: {e}")
    guest_order_monitor = f"{catalog_name}.{schema_prefix}silver.guest_order"
    try:
        w.quality_monitors.delete(table_name=guest_order_monitor)
        print(f"[INFO] Monitor deleted: {guest_order_monitor}")
    except NotFound:
        print(f"[INFO] Monitor not found (ok): {guest_order_monitor}")
    except Exception as e:
        print(f"[WARN] Monitor delete skipped for {guest_order_monitor}: {e}")
except Exception as e:
    print(f"[WARN] Monitor cleanup step skipped entirely: {e}")

# COMMAND ----------
# Step 0b: Drop UC functions (governance pack)
FUNCTIONS = ["mask_email", "mask_phone", "tier_to_multiplier", "filter_by_franchisee"]
for fn in FUNCTIONS:
    try:
        spark.sql(f"DROP FUNCTION IF EXISTS {catalog_name}.{schema_prefix}ref.{fn}")
        print(f"[INFO] Dropped function: {catalog_name}.{schema_prefix}ref.{fn}")
    except Exception as e:
        print(f"[WARN] Drop function {fn} skipped: {e}")

# COMMAND ----------
# Step 0c: Drop UC volume (governance pack)
try:
    spark.sql(f"DROP VOLUME IF EXISTS {catalog_name}.{schema_prefix}ref.assets")
    print(f"[INFO] Dropped volume: {catalog_name}.{schema_prefix}ref.assets")
except Exception as e:
    print(f"[WARN] Drop volume assets skipped: {e}")

# COMMAND ----------
# Step 1: Drop UC Metric Views
METRIC_VIEWS = [
    "order_performance",
    "loyalty_performance",
    "inventory_waste",
    "staff_hours",
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
