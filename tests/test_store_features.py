from src.features.store_features import compute_store_features

MENU = {
    1: ("pizza", "pepperoni", "Large Pepperoni"),
    30: ("wings", "boneless", "8pc Boneless"),
    53: ("drinks", "soda", "20oz Coke"),
}
# units: unit_id -> attributes
UNITS = {
    42: {"metro_area": "New York-Newark", "region_id": 3, "franchisee_id": 7},
}
ORDERS = [
    {"guest_order_id": 100, "unit_id": 42, "total_amount": 25.0},
    {"guest_order_id": 101, "unit_id": 42, "total_amount": 15.0},
]
ITEMS = [
    {"guest_order_id": 100, "unit_id": 42, "menu_item_id": 1, "quantity": 2, "line_net_amount": 36.0},
    {"guest_order_id": 100, "unit_id": 42, "menu_item_id": 53, "quantity": 1, "line_net_amount": 2.0},
    {"guest_order_id": 101, "unit_id": 42, "menu_item_id": 1, "quantity": 1, "line_net_amount": 18.0},
]


def test_one_record_per_store_with_unit_attrs():
    recs = compute_store_features(ORDERS, ITEMS, UNITS, MENU)
    assert len(recs) == 1
    r = recs[0]
    assert r["unit_id"] == 42
    assert r["metro_area"] == "New York-Newark"
    assert r["region_id"] == 3
    assert r["franchisee_id"] == 7


def test_store_aov_and_popularity():
    r = compute_store_features(ORDERS, ITEMS, UNITS, MENU)[0]
    assert abs(r["store_aov"] - 20.0) < 1e-6  # (25+15)/2
    # item 1 ordered qty 3, item 53 qty 1 -> popularity normalized by max qty
    assert r["popularity"][1] == 1.0
    assert abs(r["popularity"][53] - (1 / 3)) < 1e-6


def test_top_item_per_category():
    r = compute_store_features(ORDERS, ITEMS, UNITS, MENU)[0]
    assert r["top_item_per_category"]["pizza"] == 1
    assert r["top_item_per_category"]["drinks"] == 53


def test_unknown_unit_attrs_default():
    recs = compute_store_features(ORDERS, ITEMS, {}, MENU)
    r = recs[0]
    assert r["metro_area"] == "unknown"
    assert r["region_id"] == -1
    assert r["franchisee_id"] == -1
