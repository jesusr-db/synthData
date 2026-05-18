# tests/test_guest_loyalty_workforce.py
from datetime import datetime
from src.generator.causal_context import build_context
from src.generator.entity_registry import EntityRegistry
from src.generator.reference.us_locations import generate_units
from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
from src.generator.reference.seeder import build_financial_periods_data
from src.generator.domains.guest import generate_new_guest_profiles
from src.generator.domains.loyalty import generate_loyalty_events
from src.generator.domains.workforce import generate_shift_events

def _reg():
    return EntityRegistry(
        units=generate_units(3),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(3),
    )

def _ctx(hour=19):
    return build_context(1, datetime(2025, 9, 19, hour, 0), 1.0)

def test_new_guest_profiles_have_required_fields():
    rows = generate_new_guest_profiles(unit_id=1, date_str="2025-09-19", growth_rate=0.008, base_pool=500)
    for r in rows:
        assert "guest_profile_id" in r
        assert "digital_account_id" in r

def test_loyalty_events_reference_valid_orders():
    from src.generator.domains.orders import generate_orders_for_tick
    ctx = _ctx()
    reg = _reg()
    order_rows = generate_orders_for_tick(ctx, reg, tick_seconds=60)
    loyalty_rows = generate_loyalty_events(ctx, reg, order_rows)
    order_ids = {r["guest_order_id"] for r in order_rows if r["event_type"] == "guest_order"}
    for lr in loyalty_rows:
        if lr.get("guest_order_id"):
            assert lr["guest_order_id"] in order_ids

def test_shift_events_have_required_fields():
    rows = generate_shift_events(unit_id=1, date_str="2025-09-19", projected_orders=80)
    shift_rows = [r for r in rows if r["event_type"] == "shift"]
    assert len(shift_rows) > 0
    for s in shift_rows:
        assert "employee_id" in s
        assert "shift_start" in s
        assert "shift_end" in s

def test_high_volume_means_more_staff():
    low = generate_shift_events(unit_id=1, date_str="2025-09-22", projected_orders=20)
    high = generate_shift_events(unit_id=1, date_str="2025-09-22", projected_orders=200)
    low_staff = len([r for r in low if r["event_type"] == "shift"])
    high_staff = len([r for r in high if r["event_type"] == "shift"])
    assert high_staff >= low_staff

def test_new_guest_registrations_have_varied_status():
    all_rows = []
    for _ in range(200):
        rows = generate_new_guest_profiles(unit_id=1, date_str="2025-09-19",
                                           growth_rate=0.008, base_pool=500)
        all_rows.extend(rows)
    statuses = {r["account_status"] for r in all_rows}
    assert "active" in statuses
    if len(all_rows) > 30:
        assert "inactive" in statuses, "Expected some inactive registrations across 200 runs"

def test_generate_guest_churn_emits_inactive_profiles():
    from src.generator.domains.guest import generate_guest_churn
    reg = _reg()
    rows = generate_guest_churn(unit_id=1, registry=reg, date_str="2025-09-19",
                                 churn_rate=0.05,
                                 tick_ts=datetime(2025, 9, 19, 10, 0))
    assert len(rows) > 0
    for r in rows:
        assert r["event_type"] == "guest_profile"
        assert r["account_status"] == "inactive"

def test_reward_redemption_has_matching_redeem_transaction():
    from src.generator.domains.orders import generate_orders_for_tick
    ctx = build_context(1, datetime(2025, 9, 19, 19, 0), 2.0)
    reg = _reg()
    order_rows = generate_orders_for_tick(ctx, reg, tick_seconds=3600)
    loyalty_rows = generate_loyalty_events(ctx, reg, order_rows)

    redemption_order_ids = {r["guest_order_id"] for r in loyalty_rows if r["event_type"] == "reward_redemption"}
    redeem_txn_order_ids = {r["guest_order_id"] for r in loyalty_rows if r.get("transaction_type") == "redeem"}
    assert redemption_order_ids == redeem_txn_order_ids, \
        "Every reward_redemption must have a matching redeem loyalty_transaction"

def test_redeem_transaction_has_negative_points_delta():
    from src.generator.domains.orders import generate_orders_for_tick
    ctx = build_context(1, datetime(2025, 9, 19, 19, 0), 2.0)
    reg = _reg()
    order_rows = generate_orders_for_tick(ctx, reg, tick_seconds=3600)
    loyalty_rows = generate_loyalty_events(ctx, reg, order_rows)
    for r in loyalty_rows:
        if r.get("transaction_type") == "redeem":
            assert r["points_delta"] < 0, f"Expected negative points_delta, got {r['points_delta']}"
