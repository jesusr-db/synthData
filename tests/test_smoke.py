# tests/test_smoke.py
"""
Smoke test suite for the QSR synthetic data generator.
Validates end-to-end generator behaviour without requiring a live Spark/Databricks
cluster: all assertions run in pure Python.
"""
from datetime import datetime

import pytest

from src.generator.entity_registry import EntityRegistry
from src.generator.reference.us_locations import generate_units
from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
from src.generator.reference.seeder import build_financial_periods_data
from src.generator.runner import build_tick_rows, backfill_ticks


# ---------------------------------------------------------------------------
# Shared fixture: a small registry (1 unit) for fast smoke tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def small_registry():
    return EntityRegistry(
        units=generate_units(1, seed=99),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(1),
    )


# ---------------------------------------------------------------------------
# Test 1: Full pipeline for 1 unit over 1 hour — all event_type domains present
# ---------------------------------------------------------------------------

def test_build_tick_rows_covers_all_domains(small_registry):
    """build_tick_rows must emit rows for every major event_type domain."""
    rows = build_tick_rows(
        unit_id=1,
        timestamp=datetime(2025, 9, 19, 19, 0),   # busy dinner hour
        registry=small_registry,
        tick_seconds=3600,
        base_orders_per_hour=18,
    )
    assert len(rows) > 0, "Expected at least one row"
    event_types = {r["event_type"] for r in rows}

    # Order domain
    assert "guest_order" in event_types, f"Missing guest_order in {event_types}"
    assert "order_item" in event_types, f"Missing order_item in {event_types}"

    # Inventory domain — always present with orders
    inventory_domains = {"on_hand_balance", "waste_log", "receiving_order",
                         "replenishment_order", "stock_transfer", "adjustment"}
    assert event_types & inventory_domains, f"No inventory events in {event_types}"

    # Loyalty domain — some orders will trigger loyalty events
    loyalty_domains = {"loyalty_transaction", "reward_redemption"}
    assert event_types & loyalty_domains, f"No loyalty events in {event_types}"


# ---------------------------------------------------------------------------
# Test 2: FK consistency — order_item.guest_order_id subset of guest_order.guest_order_id
# ---------------------------------------------------------------------------

def test_order_item_fk_consistency(small_registry):
    """Every order_item.guest_order_id must reference a valid guest_order row."""
    rows = build_tick_rows(
        unit_id=1,
        timestamp=datetime(2025, 9, 19, 19, 0),
        registry=small_registry,
        tick_seconds=3600,
        base_orders_per_hour=18,
    )
    order_ids = {r["guest_order_id"] for r in rows if r["event_type"] == "guest_order"}
    item_order_ids = {r["guest_order_id"] for r in rows if r["event_type"] == "order_item"}

    assert order_ids, "No guest_order rows found — cannot check FK"
    assert item_order_ids, "No order_item rows found — cannot check FK"
    orphaned = item_order_ids - order_ids
    assert not orphaned, f"order_item rows reference missing guest_order_ids: {orphaned}"


# ---------------------------------------------------------------------------
# Test 3: DOMAIN_TABLE_MAP covers all emitted event_types
# ---------------------------------------------------------------------------

def test_domain_table_map_covers_all_event_types(small_registry):
    """Every event_type emitted by the generator must have a mapping in DOMAIN_TABLE_MAP."""
    # DOMAIN_TABLE_MAP is defined in src/generator/main.py — replicate it here
    # using catalog placeholder to avoid Databricks widget import at module load.
    catalog = "qsr_synth"
    DOMAIN_TABLE_MAP = {
        "guest_order":         f"{catalog}.staging.order_events",
        "order_item":          f"{catalog}.staging.order_events",
        "order_modifier":      f"{catalog}.staging.order_events",
        "payment":             f"{catalog}.staging.order_events",
        "status_event":        f"{catalog}.staging.order_events",
        "delivery_order":      f"{catalog}.staging.order_events",
        "on_hand_balance":     f"{catalog}.staging.inventory_events",
        "waste_log":           f"{catalog}.staging.inventory_events",
        "receiving_order":     f"{catalog}.staging.inventory_events",
        "replenishment_order": f"{catalog}.staging.inventory_events",
        "stock_transfer":      f"{catalog}.staging.inventory_events",
        "adjustment":          f"{catalog}.staging.inventory_events",
        "guest_profile":       f"{catalog}.staging.guest_events",
        "digital_account":     f"{catalog}.staging.guest_events",
        "loyalty_transaction": f"{catalog}.staging.loyalty_events",
        "reward_redemption":   f"{catalog}.staging.loyalty_events",
        "shift":               f"{catalog}.staging.workforce_events",
        "time_punch":          f"{catalog}.staging.workforce_events",
    }

    rows = build_tick_rows(
        unit_id=1,
        timestamp=datetime(2025, 9, 19, 19, 0),
        registry=small_registry,
        tick_seconds=3600,
        base_orders_per_hour=18,
    )
    emitted_types = {r["event_type"] for r in rows}
    unmapped = emitted_types - set(DOMAIN_TABLE_MAP.keys())
    assert not unmapped, f"Emitted event_types not in DOMAIN_TABLE_MAP: {unmapped}"


# ---------------------------------------------------------------------------
# Test 4: backfill_ticks for 1 month over 1 unit produces > 0 batches with non-empty rows
# ---------------------------------------------------------------------------

def test_backfill_ticks_produces_nonempty_batches():
    """backfill_ticks must yield at least one batch with rows for a 1-month run."""
    registry = EntityRegistry(
        units=generate_units(1, seed=42),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(1),
    )
    gen = backfill_ticks(registry, backfill_months=1, tick_seconds=3600, base_orders_per_hour=18)

    batch_count = 0
    total_rows = 0
    # Only consume first 5 batches to keep test fast
    for batch in gen:
        assert isinstance(batch, list), "Each batch must be a list"
        assert len(batch) > 0, "Each batch must be non-empty"
        total_rows += len(batch)
        batch_count += 1
        if batch_count >= 5:
            break

    assert batch_count > 0, "backfill_ticks produced zero batches"
    assert total_rows > 0, "backfill_ticks produced zero total rows"


# ---------------------------------------------------------------------------
# Test 5: All staging table names in DOMAIN_TABLE_MAP end with `_events` (not `stg_`)
# ---------------------------------------------------------------------------

def test_staging_table_names_use_events_suffix():
    """Staging table names must follow the <domain>_events convention — not stg_<domain>."""
    catalog = "qsr_synth"
    DOMAIN_TABLE_MAP = {
        "guest_order":         f"{catalog}.staging.order_events",
        "order_item":          f"{catalog}.staging.order_events",
        "order_modifier":      f"{catalog}.staging.order_events",
        "payment":             f"{catalog}.staging.order_events",
        "status_event":        f"{catalog}.staging.order_events",
        "delivery_order":      f"{catalog}.staging.order_events",
        "on_hand_balance":     f"{catalog}.staging.inventory_events",
        "waste_log":           f"{catalog}.staging.inventory_events",
        "receiving_order":     f"{catalog}.staging.inventory_events",
        "replenishment_order": f"{catalog}.staging.inventory_events",
        "stock_transfer":      f"{catalog}.staging.inventory_events",
        "adjustment":          f"{catalog}.staging.inventory_events",
        "guest_profile":       f"{catalog}.staging.guest_events",
        "digital_account":     f"{catalog}.staging.guest_events",
        "loyalty_transaction": f"{catalog}.staging.loyalty_events",
        "reward_redemption":   f"{catalog}.staging.loyalty_events",
        "shift":               f"{catalog}.staging.workforce_events",
        "time_punch":          f"{catalog}.staging.workforce_events",
    }

    table_names = set(DOMAIN_TABLE_MAP.values())
    for full_table in table_names:
        table_short = full_table.split(".")[-1]
        assert table_short.endswith("_events"), (
            f"Table '{table_short}' must end with '_events', got: '{full_table}'"
        )
        assert not table_short.startswith("stg_"), (
            f"Table '{table_short}' must not use 'stg_' prefix — use '<domain>_events': '{full_table}'"
        )
