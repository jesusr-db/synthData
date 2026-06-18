from src.agent.tools import ToolBox, TOOL_SPECS

MENU = {
    1: {"item_name": "Large Pepperoni", "category": "pizza", "subcategory": "classic"},
    14: {"item_name": "Garlic Knots", "category": "sides", "subcategory": "bread"},
    53: {"item_name": "20oz Coca-Cola", "category": "drinks", "subcategory": "soda"},
}
PRICES = {1: 14.99, 14: 5.49, 53: 2.49}


def _box():
    return ToolBox(
        menu=MENU, price_lookup=PRICES,
        recommend_fn=lambda profile_id, store_id, cart_product_ids, num_recommendations: (
            [{"menu_item_id": 53, "score": 0.9, "item_name": "20oz Coca-Cola"}]),
        customer_fn=lambda profile_id: (None if profile_id == "guest"
                                        else {"tier": "gold", "aov": 30.0}),
        history_fn=lambda profile_id, limit: [{"guest_order_id": 7, "items": [1, 14]}],
        occasion_fn=lambda store_id, date: [{"name": "Super Bowl", "date": "2026-02-08"}],
    )


def test_specs_expose_six_tools_including_propose_order():
    names = {t["function"]["name"] for t in TOOL_SPECS}
    assert names == {"search_menu", "get_recommendations", "get_customer_context",
                     "get_order_history", "get_occasion_context", "propose_order"}


def test_search_menu_filters_by_category():
    out = _box().dispatch("search_menu", {"category": "pizza"})
    assert [r["menu_item_id"] for r in out["results"]] == [1]
    assert out["results"][0]["price"] == 14.99


def test_get_recommendations_passes_through_injected_fn():
    out = _box().dispatch("get_recommendations",
                          {"profile_id": 1234, "store_id": 42, "cart_product_ids": [1]})
    assert out["recommendations"][0]["menu_item_id"] == 53


def test_get_customer_context_guest_returns_personalized_false():
    out = _box().dispatch("get_customer_context", {"profile_id": "guest"})
    assert out["personalized"] is False


def test_build_proposal_prices_items_and_marks_indicative():
    prop = _box().build_proposal({
        "items": [{"menu_item_id": 1, "quantity": 2}, {"menu_item_id": 14, "quantity": 1}],
        "order_type": "delivery",
    })
    assert prop["tool"] == "propose_order"
    assert prop["order_type"] == "delivery"
    assert prop["items"][0]["item_name"] == "Large Pepperoni"   # enriched from menu
    assert prop["subtotal"] == 35.47
    assert prop["total"] == 38.66
    assert "indicative" in prop["pricing_note"].lower()
