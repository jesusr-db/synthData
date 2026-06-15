from datetime import datetime
from src.features.customer_features import compute_customer_features

# menu: id -> (category, subcategory, name)
MENU = {
    1: ("pizza", "pepperoni", "Large Pepperoni"),
    30: ("wings", "boneless", "8pc Boneless"),
    53: ("drinks", "soda", "20oz Coke"),
}

ORDERS = [
    # guest_profile_id, guest_order_id, total_amount, placed_at
    {"profile_id": 10, "guest_order_id": 100, "total_amount": 25.0, "placed_at": datetime(2026, 6, 1, 19, 0)},
    {"profile_id": 10, "guest_order_id": 101, "total_amount": 15.0, "placed_at": datetime(2026, 6, 8, 12, 0)},
]
ITEMS = [
    # guest_order_id, menu_item_id, quantity, line_net_amount
    {"guest_order_id": 100, "menu_item_id": 1, "quantity": 1, "line_net_amount": 18.0},
    {"guest_order_id": 100, "menu_item_id": 53, "quantity": 1, "line_net_amount": 2.0},
    {"guest_order_id": 101, "menu_item_id": 30, "quantity": 1, "line_net_amount": 9.0},
]
TIERS = {10: "gold"}  # member_id/profile_id -> tier (latest)
AS_OF = datetime(2026, 6, 14)


def test_computes_one_record_per_customer():
    recs = compute_customer_features(ORDERS, ITEMS, TIERS, MENU, as_of=AS_OF)
    assert len(recs) == 1
    assert recs[0]["guest_profile_id"] == 10


def test_rfm_fields():
    rec = compute_customer_features(ORDERS, ITEMS, TIERS, MENU, as_of=AS_OF)[0]
    assert rec["total_orders"] == 2
    assert rec["recency_days"] == 6  # last order 2026-06-08 -> 2026-06-14
    assert abs(rec["monetary_total"] - 40.0) < 1e-6
    assert abs(rec["aov"] - 20.0) < 1e-6


def test_tier_and_category_affinity():
    rec = compute_customer_features(ORDERS, ITEMS, TIERS, MENU, as_of=AS_OF)[0]
    assert rec["tier"] == "gold"
    # spend: pizza 18, drinks 2, wings 9 -> total 29
    assert abs(rec["affinity_pizza"] - (18.0 / 29.0)) < 1e-6
    assert abs(rec["affinity_wings"] - (9.0 / 29.0)) < 1e-6
    assert abs(rec["affinity_drinks"] - (2.0 / 29.0)) < 1e-6
    assert rec["affinity_desserts"] == 0.0


def test_unknown_tier_defaults_to_none_string():
    rec = compute_customer_features(ORDERS, ITEMS, {}, MENU, as_of=AS_OF)[0]
    assert rec["tier"] == "none"


def test_skips_orders_with_null_profile():
    orders = ORDERS + [{"profile_id": None, "guest_order_id": 200, "total_amount": 9.0,
                        "placed_at": datetime(2026, 6, 10)}]
    recs = compute_customer_features(orders, ITEMS, TIERS, MENU, as_of=AS_OF)
    assert {r["guest_profile_id"] for r in recs} == {10}
