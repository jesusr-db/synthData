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
try:
    catalog_name = dbutils.widgets.get("catalog_name")
    num_units = int(dbutils.widgets.get("num_units"))
    backfill_months = int(dbutils.widgets.get("backfill_months"))
    live_tick_seconds = int(dbutils.widgets.get("live_tick_seconds"))
    base_orders = int(dbutils.widgets.get("base_orders_per_unit_per_hour"))
    mode = dbutils.widgets.get("mode")  # "backfill" or "live"
except Exception:
    # Defaults for interactive execution — always use job parameters in deployment
    catalog_name = "jmrdemo"
    num_units = 250
    backfill_months = 12
    live_tick_seconds = 60
    base_orders = 18
    mode = "live"

# COMMAND ----------
from src.generator.entity_registry import EntityRegistry
from src.generator.runner import backfill_ticks, live_tick, GeneratorConfig
from src.generator.reference.seeder import seed_all

# Load registry from ref tables
registry = EntityRegistry.from_spark(spark, catalog_name)

# COMMAND ----------
from collections import defaultdict
from pyspark.sql import Row

DOMAIN_TABLE_MAP = {
    "guest_order":         f"{catalog_name}.staging.order_events",
    "order_item":          f"{catalog_name}.staging.order_events",
    "order_modifier":      f"{catalog_name}.staging.order_events",
    "payment":             f"{catalog_name}.staging.order_events",
    "status_event":        f"{catalog_name}.staging.order_events",
    "delivery_order":      f"{catalog_name}.staging.order_events",
    "on_hand_balance":     f"{catalog_name}.staging.inventory_events",
    "waste_log":           f"{catalog_name}.staging.inventory_events",
    "receiving_order":     f"{catalog_name}.staging.inventory_events",
    "replenishment_order": f"{catalog_name}.staging.inventory_events",
    "stock_transfer":      f"{catalog_name}.staging.inventory_events",
    "adjustment":          f"{catalog_name}.staging.inventory_events",
    "guest_profile":       f"{catalog_name}.staging.guest_events",
    "digital_account":     f"{catalog_name}.staging.guest_events",
    "loyalty_transaction": f"{catalog_name}.staging.loyalty_events",
    "reward_redemption":   f"{catalog_name}.staging.loyalty_events",
    "shift":               f"{catalog_name}.staging.workforce_events",
    "time_punch":          f"{catalog_name}.staging.workforce_events",
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
if mode == "backfill":
    print(f"[INFO] Starting backfill: {backfill_months} months, catalog={catalog_name}")
    total_rows = 0
    for i, batch in enumerate(
        backfill_ticks(
            registry,
            backfill_months,
            tick_seconds=3600,
            base_orders_per_hour=base_orders,
        )
    ):
        write_batch(batch)
        total_rows += len(batch)
        if i % 100 == 0:
            print(f"[INFO] Backfill tick {i}, cumulative rows written: {total_rows}")
    print(f"[INFO] Backfill complete: {total_rows} total rows written")

else:
    # Live mode: run once (invoked every live_tick_seconds by the job schedule)
    print(f"[INFO] Live tick: tick_seconds={live_tick_seconds}, catalog={catalog_name}")
    rows = live_tick(registry, live_tick_seconds, base_orders)
    write_batch(rows)
    print(f"[INFO] Live tick complete: {len(rows)} rows written")
