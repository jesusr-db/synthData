# tests/test_orders.py
from datetime import datetime
from src.generator.causal_context import build_context
from src.generator.entity_registry import EntityRegistry
from src.generator.reference.us_locations import generate_units
from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
from src.generator.reference.seeder import build_financial_periods_data
from src.generator.domains.orders import generate_orders_for_tick

def _registry():
    return EntityRegistry(
        units=generate_units(5),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(3),
    )

def _ctx(unit_id=1):
    return build_context(unit_id, datetime(2025, 9, 19, 19, 0), 1.0)  # Friday dinner

def test_returns_list_of_dicts():
    rows = generate_orders_for_tick(_ctx(), _registry(), tick_seconds=60)
    assert isinstance(rows, list)

def test_order_has_required_fields():
    rows = generate_orders_for_tick(_ctx(), _registry(), tick_seconds=60)
    if rows:
        order = next(r for r in rows if r["event_type"] == "guest_order")
        required = {"guest_order_id", "unit_id", "channel", "order_status",
                    "subtotal", "tax_amount", "total_amount", "placed_at"}
        assert required.issubset(order.keys())

def test_order_items_reference_order():
    rows = generate_orders_for_tick(_ctx(), _registry(), tick_seconds=60)
    order_ids = {r["guest_order_id"] for r in rows if r["event_type"] == "guest_order"}
    item_order_ids = {r["guest_order_id"] for r in rows if r["event_type"] == "order_item"}
    assert item_order_ids.issubset(order_ids)

def test_payment_references_order():
    rows = generate_orders_for_tick(_ctx(), _registry(), tick_seconds=60)
    order_ids = {r["guest_order_id"] for r in rows if r["event_type"] == "guest_order"}
    payment_order_ids = {r["guest_order_id"] for r in rows if r["event_type"] == "payment"}
    # Cancelled orders may have no payment
    assert payment_order_ids.issubset(order_ids)

def test_total_amount_equals_subtotal_plus_tax():
    rows = generate_orders_for_tick(_ctx(), _registry(), tick_seconds=60)
    for r in rows:
        if r["event_type"] == "guest_order" and r["order_status"] != "cancelled":
            assert abs(r["total_amount"] - (r["subtotal"] + r["tax_amount"])) < 0.01
