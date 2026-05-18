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

def test_some_orders_have_discounts():
    ctx = _ctx()
    reg = _registry()
    all_orders = []
    for _ in range(30):
        all_orders.extend(r for r in generate_orders_for_tick(ctx, reg, tick_seconds=3600)
                          if r["event_type"] == "guest_order" and r["order_status"] == "fulfilled")
    discounted = [o for o in all_orders if o.get("discount_amount", 0) > 0]
    assert len(discounted) > 0, "Expected some discounted orders across 30 ticks"

def test_discounted_order_math_is_correct():
    ctx = _ctx()
    reg = _registry()
    all_rows = []
    for _ in range(30):
        all_rows.extend(generate_orders_for_tick(ctx, reg, tick_seconds=3600))
    discounted_orders = [r for r in all_rows if r["event_type"] == "guest_order"
                         and r.get("discount_amount", 0) > 0]
    for o in discounted_orders:
        assert abs(o["total_amount"] - (o["subtotal"] + o["tax_amount"])) < 0.02, \
            f"total {o['total_amount']} != subtotal {o['subtotal']} + tax {o['tax_amount']}"

def test_line_net_amount_equals_gross_minus_discount():
    ctx = _ctx()
    reg = _registry()
    rows = []
    for _ in range(10):
        rows.extend(generate_orders_for_tick(ctx, reg, tick_seconds=3600))
    for item in rows:
        if item["event_type"] == "order_item":
            expected = round(item["line_gross_amount"] - item["line_discount_amount"], 2)
            assert abs(item["line_net_amount"] - expected) < 0.02, \
                f"line_net {item['line_net_amount']} != gross {item['line_gross_amount']} - disc {item['line_discount_amount']}"

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

def test_units_have_market_price_index():
    units = generate_units(10)
    for u in units:
        assert "market_price_index" in u, "Each unit must have a market_price_index"
        assert 0.85 <= u["market_price_index"] <= 1.25, \
            f"market_price_index {u['market_price_index']} out of range [0.85, 1.25]"

def test_aov_varies_across_units():
    """Units with high market_price_index should produce higher average order values."""
    from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
    from src.generator.reference.seeder import build_financial_periods_data

    # Build two registries: one with all units forced to low price index, one high
    low_units = [{"unit_id": i, "unit_name": f"U{i}", "city": "A", "state": "TX",
                  "lat": 30.0, "lon": -97.0, "metro_area": "Austin-Round Rock, TX",
                  "district_id": 1, "region_id": 1, "franchisee_id": None,
                  "format": "carryout_delivery", "unit_volume_bias": 1.0,
                  "is_franchise": False, "status": "active",
                  "market_price_index": 0.85} for i in range(1, 4)]
    high_units = [{"unit_id": i, "unit_name": f"U{i}", "city": "B", "state": "NY",
                   "lat": 40.7, "lon": -74.0, "metro_area": "New York-Newark-Jersey City, NY",
                   "district_id": 1, "region_id": 1, "franchisee_id": None,
                   "format": "carryout_delivery", "unit_volume_bias": 1.0,
                   "is_franchise": False, "status": "active",
                   "market_price_index": 1.25} for i in range(1, 4)]

    menu = get_menu_items()
    bom = get_recipe_ingredients()
    fp = build_financial_periods_data(3)
    low_reg = EntityRegistry(units=low_units, menu_items=menu, bom=bom, financial_periods=fp)
    high_reg = EntityRegistry(units=high_units, menu_items=menu, bom=bom, financial_periods=fp)

    ctx = build_context(1, datetime(2025, 9, 19, 12, 0), 1.0)
    low_totals, high_totals = [], []
    for _ in range(20):
        for r in generate_orders_for_tick(ctx, low_reg, tick_seconds=3600):
            if r["event_type"] == "guest_order" and r["order_status"] == "fulfilled":
                low_totals.append(r["total_amount"])
        for r in generate_orders_for_tick(ctx, high_reg, tick_seconds=3600):
            if r["event_type"] == "guest_order" and r["order_status"] == "fulfilled":
                high_totals.append(r["total_amount"])

    if low_totals and high_totals:
        low_aov = sum(low_totals) / len(low_totals)
        high_aov = sum(high_totals) / len(high_totals)
        assert high_aov > low_aov, \
            f"Expected high-price-index AOV ({high_aov:.2f}) > low ({low_aov:.2f})"
