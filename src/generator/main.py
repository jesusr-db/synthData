# Databricks notebook source
# COMMAND ----------
import sys

# Add bundle root to sys.path so `src.*` imports resolve when run as a job
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_nb_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

# COMMAND ----------
# Load params — injected as job widgets (all params live in databricks.yml variables)
def _widget(name, default):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default

catalog_name       = _widget("catalog_name", "jmrdemo")
num_units          = int(_widget("num_units", "250"))
backfill_months    = int(_widget("backfill_months", "1"))
live_tick_seconds  = int(_widget("live_tick_seconds", "3600"))
base_orders        = int(_widget("base_orders_per_unit_per_hour", "18"))
mode               = _widget("mode", "live")
start_dt_override  = _widget("start_dt_override", "")
end_dt_override    = _widget("end_dt_override", "")
schema_prefix       = _widget("schema_prefix", "synth_")

# COMMAND ----------
from datetime import datetime, timedelta

from src.generator.entity_registry import EntityRegistry
from src.generator.runner import backfill_ticks
from src.generator.reference.seeder import seed_all

# Load registry from ref tables
registry = EntityRegistry.from_spark(spark, catalog_name, schema_prefix=schema_prefix)

# COMMAND ----------
from collections import defaultdict
from pyspark.sql import Row

DOMAIN_TABLE_MAP = {
    "guest_order":         f"{catalog_name}.{schema_prefix}staging.order_events",
    "order_item":          f"{catalog_name}.{schema_prefix}staging.order_events",
    "order_modifier":      f"{catalog_name}.{schema_prefix}staging.order_events",
    "payment":             f"{catalog_name}.{schema_prefix}staging.order_events",
    "status_event":        f"{catalog_name}.{schema_prefix}staging.order_events",
    "delivery_order":      f"{catalog_name}.{schema_prefix}staging.order_events",
    "on_hand_balance":     f"{catalog_name}.{schema_prefix}staging.inventory_events",
    "waste_log":           f"{catalog_name}.{schema_prefix}staging.inventory_events",
    "receiving_order":     f"{catalog_name}.{schema_prefix}staging.inventory_events",
    "replenishment_order": f"{catalog_name}.{schema_prefix}staging.inventory_events",
    "stock_transfer":      f"{catalog_name}.{schema_prefix}staging.inventory_events",
    "adjustment":          f"{catalog_name}.{schema_prefix}staging.inventory_events",
    "guest_profile":       f"{catalog_name}.{schema_prefix}staging.guest_events",
    "digital_account":     f"{catalog_name}.{schema_prefix}staging.guest_events",
    "loyalty_transaction": f"{catalog_name}.{schema_prefix}staging.loyalty_events",
    "reward_redemption":   f"{catalog_name}.{schema_prefix}staging.loyalty_events",
    "shift":               f"{catalog_name}.{schema_prefix}staging.workforce_events",
    "time_punch":          f"{catalog_name}.{schema_prefix}staging.workforce_events",
}


def write_batch(rows: list[dict]):
    """Route rows by event_type to the appropriate staging Delta table."""
    # Group by (table, event_type) — same event_type guarantees a uniform schema
    # per createDataFrame call (mixed types produce AXIS_LENGTH_MISMATCH errors)
    by_table_event: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        et = row.get("event_type")
        if et in DOMAIN_TABLE_MAP:
            by_table_event[(DOMAIN_TABLE_MAP[et], et)].append(row)
        else:
            print(f"[WARN] Unknown event_type '{et}' — skipped")
    for (table, _), event_rows in by_table_event.items():
        # PySpark cannot infer type for columns that are None in every row;
        # drop them here — mergeSchema fills the gap with NULL in Delta.
        all_keys = {k for r in event_rows for k in r}
        present_keys = {k for k in all_keys if any(r.get(k) is not None for r in event_rows)}
        cleaned = [{k: r.get(k) for k in present_keys} for r in event_rows]
        df = spark.createDataFrame(cleaned)
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)


# COMMAND ----------
def _latest_staging_ts():
    """Return the max event_ts across all staging tables, or None if tables are empty."""
    from datetime import timedelta
    STAGING_TABLES = [
        f"{catalog_name}.{schema_prefix}staging.order_events",
        f"{catalog_name}.{schema_prefix}staging.inventory_events",
        f"{catalog_name}.{schema_prefix}staging.guest_events",
        f"{catalog_name}.{schema_prefix}staging.loyalty_events",
        f"{catalog_name}.{schema_prefix}staging.workforce_events",
    ]
    max_ts = None
    for table in STAGING_TABLES:
        row = spark.sql(f"SELECT MAX(event_ts) AS ts FROM {table}").collect()[0]
        if row.ts is not None:
            ts = row.ts.replace(tzinfo=None) if hasattr(row.ts, 'tzinfo') else row.ts
            if max_ts is None or ts > max_ts:
                max_ts = ts
    if max_ts is None:
        return None
    # Advance to next full hour to avoid re-generating the last partial tick
    return (max_ts.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))


if mode == "backfill":
    if start_dt_override:
        start_dt = datetime.fromisoformat(start_dt_override)
        print(f"[INFO] Backfill start override: {start_dt}")
    else:
        start_dt = _latest_staging_ts()
        if start_dt is not None:
            print(f"[INFO] Rehydrating from latest staging timestamp: {start_dt}")
        else:
            print(f"[INFO] No existing data — backfilling {backfill_months} month(s), catalog={catalog_name}, schema_prefix={schema_prefix}")
    end_dt = datetime.fromisoformat(end_dt_override) if end_dt_override else None
    if end_dt:
        print(f"[INFO] Backfill end override: {end_dt}")
    total_rows = 0
    for i, batch in enumerate(
        backfill_ticks(
            registry,
            backfill_months,
            tick_seconds=3600,
            base_orders_per_hour=base_orders,
            start_dt=start_dt,
            end_dt=end_dt,
        )
    ):
        write_batch(batch)
        total_rows += len(batch)
        if i % 100 == 0:
            print(f"[INFO] Backfill tick {i}, cumulative rows written: {total_rows}")
    print(f"[INFO] Backfill complete: {total_rows} total rows written")

else:
    # Live mode: generate the previous hour as 60 sub-ticks with correct per-minute timestamps.
    # Runs once per hour (scheduled via cron). live_tick_seconds controls sub-tick granularity.
    end_dt   = datetime.now().replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(hours=1)
    print(f"[INFO] Live tick: window=[{start_dt}, {end_dt}), sub_tick_seconds={live_tick_seconds}, catalog={catalog_name}, schema_prefix={schema_prefix}")
    total_rows = 0
    for batch in backfill_ticks(registry, backfill_months=1, tick_seconds=live_tick_seconds,
                                 base_orders_per_hour=base_orders, start_dt=start_dt, end_dt=end_dt):
        write_batch(batch)
        total_rows += len(batch)
    print(f"[INFO] Live tick complete: {total_rows} rows written for window [{start_dt}, {end_dt})")
