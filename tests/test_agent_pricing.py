from src.agent.pricing import price_items


def test_price_items_computes_subtotal_tax_total():
    items = [
        {"menu_item_id": 1, "quantity": 2, "item_name": "Large Pepperoni"},
        {"menu_item_id": 14, "quantity": 1, "item_name": "Garlic Knots"},
    ]
    price_lookup = {1: 14.99, 14: 5.49}
    out = price_items(items, price_lookup, tax_rate=0.09)
    assert out["currency"] == "USD"
    assert out["subtotal"] == 35.47          # 2*14.99 + 5.49
    assert out["tax_estimate"] == 3.19        # round(35.47 * 0.09, 2)
    assert out["total"] == 38.66
    assert out["items"][0]["unit_price"] == 14.99
    assert out["items"][1]["unit_price"] == 5.49


def test_price_items_unknown_id_prices_zero():
    out = price_items([{"menu_item_id": 999, "quantity": 1, "item_name": "Mystery"}], {})
    assert out["items"][0]["unit_price"] == 0.0
    assert out["subtotal"] == 0.0
    assert out["total"] == 0.0
