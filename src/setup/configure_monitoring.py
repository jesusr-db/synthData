# Databricks notebook source
# COMMAND ----------
import sys

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_notebook_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

try:
    catalog_name = dbutils.widgets.get("catalog_name")
except Exception:
    catalog_name = "jmrdemo"

try:
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    schema_prefix = "synth_"

print(f"[INFO] configure_monitoring: catalog={catalog_name}, schema_prefix={schema_prefix}")
c = catalog_name
p = schema_prefix

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {c}.{p}metrics")
print(f"[OK] schema {c}.{p}metrics ready")

# COMMAND ----------
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import (
    MonitorSnapshot,
    MonitorTimeSeries,
    MonitorCronSchedule,
    MonitorDataClassificationConfig,
)

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
token = ctx.apiToken().get()
host = spark.conf.get("spark.databricks.workspaceUrl")
w = WorkspaceClient(host=f"https://{host}", token=token)

MONITOR_SCHEDULE = MonitorCronSchedule(
    quartz_cron_expression="0 0 0/12 * * ?",
    timezone_id="UTC",
)

# (table_full_name, assets_dir_suffix, monitor_type_kwargs)
MONITORS = [
    (
        f"{c}.{p}staging.order_events",
        f"{p}order_events",
        {"snapshot": MonitorSnapshot()},
    ),
    (
        f"{c}.{p}staging.inventory_events",
        f"{p}inventory_events",
        {"snapshot": MonitorSnapshot()},
    ),
    (
        f"{c}.{p}staging.loyalty_events",
        f"{p}loyalty_events",
        {"snapshot": MonitorSnapshot()},
    ),
    (
        f"{c}.{p}silver.guest_order",
        f"{p}guest_order",
        {"time_series": MonitorTimeSeries(timestamp_col="placed_at", granularities=["1 day"])},
    ),
]

output_schema = f"{c}.{p}metrics"

for full_name, assets_suffix, monitor_kwargs in MONITORS:
    try:
        spark.sql(f"GRANT SELECT ON TABLE {full_name} TO `account users`")
        print(f"[INFO] SELECT granted on {full_name} to account users")
    except Exception as e:
        print(f"[WARN] SELECT grant skipped for {full_name}: {e}")

    try:
        w.quality_monitors.get(table_name=full_name)  # probe — raises NotFound if absent
        try:
            w.quality_monitors.update(
                table_name=full_name,
                output_schema_name=output_schema,
                schedule=MONITOR_SCHEDULE,
                data_classification_config=MonitorDataClassificationConfig(enabled=True),
            )
            print(f"[INFO] Monitor updated: {full_name} (schedule=0 0 0/12 * * ?, classification=enabled)")
        except Exception as ue:
            print(f"[WARN] Monitor update skipped for {full_name}: {ue}")
    except NotFound:
        try:
            w.quality_monitors.create(
                table_name=full_name,
                assets_dir=f"/Shared/qsr_monitors/{assets_suffix}",
                output_schema_name=output_schema,
                schedule=MONITOR_SCHEDULE,
                data_classification_config=MonitorDataClassificationConfig(enabled=True),
                **monitor_kwargs,
            )
            print(f"[INFO] Monitor created: {full_name} (schedule=0 0 0/12 * * ?, classification=enabled)")
        except Exception as e:
            print(f"[WARN] Monitor skipped for {full_name}: {e}")

# COMMAND ----------
print("[INFO] configure_monitoring complete — 4 monitors (3 snapshot + 1 timeseries), 12h schedule, classification enabled")
