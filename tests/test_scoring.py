from src.features.affinity import load_affinity
from src.ml.scoring import rank_recommendations

CFG = load_affinity()
# menu: id -> (category, subcategory, name)
MENU = {
    1: ("pizza", "pepperoni", "Large Pepperoni"),
    2: ("pizza", "cheese", "Large Cheese"),
    30: ("wings", "boneless", "8pc Boneless"),
    53: ("drinks", "soda", "20oz Coke"),
    54: ("drinks", "soda", "20oz Diet Coke"),
    55: ("drinks", "water", "20oz Water"),
    70: ("desserts", "cake", "Lava Cake"),
}
# customer features (resolved); None => cold start
CUST = {"tier": "gold", "aov": 22.0,
        "affinity_pizza": 0.5, "affinity_wings": 0.2, "affinity_drinks": 0.1,
        "affinity_sides": 0.1, "affinity_salads": 0.0, "affinity_desserts": 0.1}
STORE = {"popularity": {1: 1.0, 53: 0.8, 70: 0.3}, "store_aov": 20.0}


def _ids(recs):
    return [r["menu_item_id"] for r in recs]


def test_excludes_items_already_in_cart():
    recs = rank_recommendations(cart=[1], cust=CUST, store=STORE, menu=MENU, cfg=CFG, max_results=10)
    assert 1 not in _ids(recs)


def test_pizza_cart_recommends_a_drink_near_top():
    recs = rank_recommendations(cart=[1], cust=CUST, store=STORE, menu=MENU, cfg=CFG, max_results=3)
    top_cats = [MENU[r["menu_item_id"]][0] for r in recs]
    assert "drinks" in top_cats


def test_suppresses_second_soda_when_soda_in_cart():
    recs = rank_recommendations(cart=[1, 53], cust=CUST, store=STORE, menu=MENU, cfg=CFG, max_results=10)
    # 53 in cart; 54 is another soda -> suppressed; 55 (water) allowed
    ids = _ids(recs)
    assert 54 not in ids
    assert 53 not in ids


def test_respects_max_results():
    recs = rank_recommendations(cart=[1], cust=CUST, store=STORE, menu=MENU, cfg=CFG, max_results=2)
    assert len(recs) == 2


def test_results_sorted_by_score_desc():
    recs = rank_recommendations(cart=[1], cust=CUST, store=STORE, menu=MENU, cfg=CFG, max_results=5)
    scores = [r["score"] for r in recs]
    assert scores == sorted(scores, reverse=True)


def test_cold_start_uses_store_popularity_without_customer():
    recs = rank_recommendations(cart=[], cust=None, store=STORE, menu=MENU, cfg=CFG, max_results=3)
    assert len(recs) > 0
    # empty cart + cold start -> popular items surface; item 1 is most popular
    assert 1 in _ids(recs)


def test_every_rec_has_contract_fields():
    recs = rank_recommendations(cart=[1], cust=CUST, store=STORE, menu=MENU, cfg=CFG, max_results=3)
    for r in recs:
        assert set(r.keys()) >= {"menu_item_id", "item_name", "category", "subcategory", "score", "reason"}
        assert isinstance(r["score"], float)
