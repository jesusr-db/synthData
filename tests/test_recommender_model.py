import json
import pandas as pd
from src.ml.recommender_model import RecommenderModel

MENU = {
    1: ["pizza", "pepperoni", "Large Pepperoni"],
    53: ["drinks", "soda", "20oz Coke"],
    54: ["drinks", "soda", "20oz Diet Coke"],
    70: ["desserts", "cake", "Lava Cake"],
}
AFFINITY = {
    "complements": {
        "pizza": {"drinks": 0.9, "desserts": 0.5, "sides": 0.7, "wings": 0.4, "salads": 0.2},
        "drinks": {"pizza": 0.3, "desserts": 0.4, "sides": 0.3, "wings": 0.2, "salads": 0.1},
        "desserts": {"drinks": 0.6, "pizza": 0.2, "sides": 0.1, "wings": 0.1, "salads": 0.1},
        "wings": {"drinks": 0.8, "sides": 0.8, "pizza": 0.4, "desserts": 0.3, "salads": 0.2},
        "sides": {"drinks": 0.7, "pizza": 0.4, "desserts": 0.3, "wings": 0.3, "salads": 0.1},
        "salads": {"drinks": 0.7, "pizza": 0.3, "sides": 0.2, "desserts": 0.2, "wings": 0.1},
    },
    "empty_cart_priority": ["pizza", "wings", "salads", "sides", "drinks", "desserts"],
    "suppress_duplicate_subcategories": ["soda", "water"],
}


def _model():
    m = RecommenderModel()
    m._load(menu=MENU, affinity=AFFINITY, estimator=None)  # test hook bypassing artifact load
    return m


def test_predict_returns_one_result_per_row():
    m = _model()
    df = pd.DataFrame([
        {"profile_id": 10, "member_id": 10, "store_id": 42, "cart_product_ids": [1],
         "viewed_product_id": None, "num_recommendations": 3,
         "tier": "gold", "affinity_pizza": 0.5, "affinity_drinks": 0.1, "affinity_wings": 0.1,
         "affinity_sides": 0.1, "affinity_salads": 0.0, "affinity_desserts": 0.2,
         "aov": 22.0, "store_aov": 20.0},
    ])
    out = m.predict(None, df)
    assert len(out) == 1
    rec0 = out[0]
    assert rec0["personalized"] is True
    assert len(rec0["recommendations"]) <= 3
    ids = [r["menu_item_id"] for r in rec0["recommendations"]]
    assert 1 not in ids
    assert all(isinstance(i, int) for i in ids)  # ints out


def test_cold_start_when_customer_features_missing():
    m = _model()
    df = pd.DataFrame([
        {"profile_id": -1, "member_id": None, "store_id": 42, "cart_product_ids": [],
         "viewed_product_id": None, "num_recommendations": 2,
         "tier": None, "aov": None, "store_aov": 20.0},
    ])
    out = m.predict(None, df)
    assert out[0]["personalized"] is False
    assert len(out[0]["recommendations"]) == 2


def test_accepts_json_string_cart_and_viewed_item():
    m = _model()
    df = pd.DataFrame([
        {"profile_id": 10, "member_id": 10, "store_id": 42,
         "cart_product_ids": json.dumps([1, 53]), "viewed_product_id": 2,
         "num_recommendations": 5, "tier": "gold", "affinity_pizza": 0.5, "affinity_drinks": 0.1,
         "affinity_wings": 0.0, "affinity_sides": 0.0, "affinity_salads": 0.0, "affinity_desserts": 0.0,
         "aov": 22.0, "store_aov": 20.0},
    ])
    out = m.predict(None, df)
    ids = [r["menu_item_id"] for r in out[0]["recommendations"]]
    assert 54 not in ids  # second soda suppressed (soda already in cart)
    assert 1 not in ids and 53 not in ids  # cart items excluded
    assert 2 not in ids  # viewed item excluded


def test_nan_request_fields_do_not_crash():
    """Model Serving sends absent/null fields as float NaN — pyfunc must not crash."""
    m = _model()
    nan = float("nan")
    df = pd.DataFrame([{
        "profile_id": nan,
        "store_id": 42,
        "cart_product_ids": nan,
        "viewed_product_id": nan,
        "num_recommendations": nan,
        "tier": nan,
        "store_aov": nan,
    }])
    out = m.predict(None, df)
    assert len(out) == 1
    row = out[0]
    assert row["personalized"] is False
    assert len(row["recommendations"]) > 0
    for rec in row["recommendations"]:
        assert 0.0 <= rec["score"] <= 1.0


def test_nan_customer_features_are_cold_start():
    """All-NaN customer columns must yield cold-start (personalized=False)."""
    m = _model()
    nan = float("nan")
    df = pd.DataFrame([{
        "profile_id": 99,
        "store_id": 42,
        "cart_product_ids": [],
        "viewed_product_id": None,
        "num_recommendations": 3,
        "tier": nan,
        "aov": nan,
        "affinity_pizza": nan,
        "affinity_wings": nan,
        "affinity_sides": nan,
        "affinity_salads": nan,
        "affinity_drinks": nan,
        "affinity_desserts": nan,
        "store_aov": 20.0,
    }])
    out = m.predict(None, df)
    assert out[0]["personalized"] is False


def test_mixed_personalized_and_coldstart_rows():
    """Row0 has real features (personalized); row1 has NaN features (cold-start). No leakage."""
    m = _model()
    nan = float("nan")
    df = pd.DataFrame([
        # row0 — real customer features → personalized
        {
            "profile_id": 10, "store_id": 42,
            "cart_product_ids": [1],
            "viewed_product_id": None,
            "num_recommendations": 3,
            "tier": "gold",
            "aov": 22.0,
            "affinity_pizza": 0.8,
            "affinity_wings": 0.1,
            "affinity_sides": 0.1,
            "affinity_salads": 0.0,
            "affinity_drinks": 0.0,
            "affinity_desserts": 0.0,
            "store_aov": 20.0,
        },
        # row1 — NaN customer features → cold-start
        {
            "profile_id": nan, "store_id": 42,
            "cart_product_ids": nan,
            "viewed_product_id": nan,
            "num_recommendations": nan,
            "tier": nan,
            "aov": nan,
            "affinity_pizza": nan,
            "affinity_wings": nan,
            "affinity_sides": nan,
            "affinity_salads": nan,
            "affinity_drinks": nan,
            "affinity_desserts": nan,
            "store_aov": nan,
        },
    ])
    out = m.predict(None, df)
    assert len(out) == 2
    assert out[0]["personalized"] is True
    assert out[1]["personalized"] is False
