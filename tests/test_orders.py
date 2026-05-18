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

def test_cancelled_orders_emit_items():
    ctx = build_context(1, datetime(2025, 9, 19, 19, 0), 2.0)
    reg = _registry()
    rows = generate_orders_for_tick(ctx, reg, tick_seconds=3600)
    cancelled_ids = {r["guest_order_id"] for r in rows
                     if r["event_type"] == "guest_order" and r["order_status"] == "cancelled"}
    if not cancelled_ids:
        return  # no cancelled orders this tick — non-deterministic, skip
    cancelled_items = [r for r in rows if r["event_type"] == "order_item"
                       and r["guest_order_id"] in cancelled_ids]
    assert len(cancelled_items) > 0, "Cancelled orders must emit order_item rows"
    assert all(r["item_status"] == "cancelled" for r in cancelled_items)

def test_fulfilled_items_have_valid_status():
    ctx = _ctx()
    reg = _registry()
    rows = generate_orders_for_tick(ctx, reg, tick_seconds=3600)
    fulfilled_ids = {r["guest_order_id"] for r in rows
                     if r["event_type"] == "guest_order" and r["order_status"] == "fulfilled"}
    items = [r for r in rows if r["event_type"] == "order_item" and r["guest_order_id"] in fulfilled_ids]
    valid = {"fulfilled", "refunded"}
    assert all(r["item_status"] in valid for r in items)

def test_waste_flag_set_on_some_items_at_late_night():
    ctx = build_context(1, datetime(2025, 9, 19, 21, 0), 2.0)
    reg = _registry()
    all_items = []
    for _ in range(15):
        rows = generate_orders_for_tick(ctx, reg, tick_seconds=3600)
        all_items.extend(r for r in rows if r["event_type"] == "order_item")
    if len(all_items) > 20:
        assert any(r["waste_flag"] for r in all_items), \
            "Expected some waste_flag=True at late-night high-volume"

def test_cancelled_items_have_higher_waste_rate_than_fulfilled():
    ctx = build_context(1, datetime(2025, 9, 19, 21, 0), 2.0)
    reg = _registry()
    fulfilled_items, cancelled_items = [], []
    for _ in range(30):
        rows = generate_orders_for_tick(ctx, reg, tick_seconds=3600)
        fulfilled_items.extend(r for r in rows if r["event_type"] == "order_item"
                                and r.get("item_status") == "fulfilled")
        cancelled_items.extend(r for r in rows if r["event_type"] == "order_item"
                                and r.get("item_status") == "cancelled")
    if not fulfilled_items or not cancelled_items:
        return
    fulfilled_rate = sum(1 for r in fulfilled_items if r["waste_flag"]) / len(fulfilled_items)
    cancelled_rate = sum(1 for r in cancelled_items if r["waste_flag"]) / len(cancelled_items)
    assert cancelled_rate > fulfilled_rate, \
        f"Expected cancelled waste rate ({cancelled_rate:.3f}) > fulfilled ({fulfilled_rate:.3f})"
