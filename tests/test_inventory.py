# tests/test_inventory.py
from datetime import datetime
from src.generator.causal_context import build_context
from src.generator.entity_registry import EntityRegistry
from src.generator.reference.us_locations import generate_units
from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
from src.generator.reference.seeder import build_financial_periods_data
from src.generator.domains.inventory import (
    generate_inventory_events, generate_daily_receiving
)

def _reg():
    return EntityRegistry(
        units=generate_units(3),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(3),
    )

def test_inventory_events_for_orders():
    from src.generator.domains.orders import generate_orders_for_tick
    ctx = build_context(1, datetime(2025, 9, 19, 19, 0), 1.0)
    reg = _reg()
    order_rows = generate_orders_for_tick(ctx, reg, tick_seconds=3600)
    inv_rows = generate_inventory_events(ctx, reg, order_rows)
    assert isinstance(inv_rows, list)
    balance_rows = [r for r in inv_rows if r["event_type"] == "on_hand_balance"]
    assert len(balance_rows) > 0

def test_waste_events_have_required_fields():
    ctx = build_context(1, datetime(2025, 9, 19, 21, 0), 1.0)  # late night
    reg = _reg()
    rows = generate_inventory_events(ctx, reg, [])
    waste_rows = [r for r in rows if r["event_type"] == "waste_log"]
    for w in waste_rows:
        assert "stock_sku" in w
        assert "waste_quantity" in w
        assert w["waste_quantity"] > 0

def test_daily_receiving_produces_receiving_orders():
    reg = _reg()
    rows = generate_daily_receiving(unit_id=1, reg=reg, order_date="2025-09-19")
    assert len(rows) > 0
    assert all(r["event_type"] == "receiving_order" for r in rows)

def test_waste_categories_are_diverse():
    from src.generator.domains.orders import generate_orders_for_tick
    ctx = build_context(1, datetime(2025, 9, 19, 21, 0), 2.0)
    reg = _reg()
    order_rows = generate_orders_for_tick(ctx, reg, tick_seconds=3600)
    all_cats = []
    for _ in range(20):
        rows = generate_inventory_events(ctx, reg, order_rows)
        all_cats.extend(r["waste_category"] for r in rows if r["event_type"] == "waste_log")
    valid = {"overproduction", "spoilage", "theft", "expired", "damaged"}
    assert all(c in valid for c in all_cats), f"Invalid category: {set(all_cats) - valid}"
    assert len(set(all_cats)) >= 2, "Expected multiple distinct waste categories across 20 runs"
