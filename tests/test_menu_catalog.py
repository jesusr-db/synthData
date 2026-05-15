# tests/test_menu_catalog.py
from src.generator.reference.menu_catalog import (
    get_menu_items, get_recipe_ingredients, get_item_price
)

def test_menu_items_count():
    items = get_menu_items()
    assert len(items) >= 60

def test_menu_item_fields():
    items = get_menu_items()
    required = {"menu_item_id", "item_name", "category", "subcategory",
                "base_price", "cost", "is_3pd_available", "is_olo_available",
                "daypart", "item_status"}
    for item in items[:5]:
        assert required.issubset(item.keys())

def test_recipe_ingredients_reference_valid_items():
    items = {i["menu_item_id"] for i in get_menu_items()}
    ingredients = get_recipe_ingredients()
    for ing in ingredients:
        assert ing["menu_item_id"] in items

def test_item_price_returns_float():
    price = get_item_price(menu_item_id=1)
    assert isinstance(price, float)
    assert price > 0
