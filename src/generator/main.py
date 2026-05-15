# Databricks notebook source
# COMMAND ----------
import sys
import yaml
from pathlib import Path

# Load params — injected as widgets or read from conf/params.yml
try:
    catalog_name = dbutils.widgets.get("catalog_name")
    num_units = int(dbutils.widgets.get("num_units"))
    backfill_months = int(dbutils.widgets.get("backfill_months"))
    live_tick_seconds = int(dbutils.widgets.get("live_tick_seconds"))
    base_orders = int(dbutils.widgets.get("base_orders_per_unit_per_hour"))
    mode = dbutils.widgets.get("mode")  # "backfill" or "live"
except Exception:
    params = yaml.safe_load(Path("/Workspace/conf/params.yml").read_text())
    catalog_name = params["catalog_name"]
    num_units = params["num_units"]
    backfill_months = params["backfill_months"]
    live_tick_seconds = params["live_tick_seconds"]
    base_orders = params["base_orders_per_unit_per_hour"]
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
    by_table: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        et = row.get("event_type")
        if et in DOMAIN_TABLE_MAP:
            by_table[DOMAIN_TABLE_MAP[et]].append(row)
        else:
            print(f"[WARN] Unknown event_type '{et}' — skipped")
    for table, table_rows in by_table.items():
        df = spark.createDataFrame([Row(**r) for r in table_rows])
        df.write.format("delta").mode("append").saveAsTable(table)


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
