# Domino's-style menu catalog: ~80 items across pizzas, wings, sides, drinks, desserts
from typing import Optional
import random

_MENU_ITEMS = [
    # Pizzas — hand-tossed, thin-crust, pan variants
    (1,  "Large Hand-Tossed Pepperoni",      "pizza", "pepperoni",   15.99, 4.20, "all_day"),
    (2,  "Large Hand-Tossed Cheese",         "pizza", "cheese",      13.99, 3.50, "all_day"),
    (3,  "Large Thin-Crust Veggie",          "pizza", "specialty",   16.99, 4.80, "all_day"),
    (4,  "Large Pan MeatZZa",                "pizza", "specialty",   17.99, 5.10, "all_day"),
    (5,  "Medium Hand-Tossed Pepperoni",     "pizza", "pepperoni",   12.99, 3.30, "all_day"),
    (6,  "Medium Cheese",                    "pizza", "cheese",      10.99, 2.90, "all_day"),
    (7,  "Small Personal Pepperoni",         "pizza", "pepperoni",    7.99, 2.10, "all_day"),
    (8,  "Large Extravaganzza",              "pizza", "specialty",   18.99, 5.50, "all_day"),
    (9,  "Large Pacific Veggie",             "pizza", "specialty",   17.49, 4.90, "all_day"),
    (10, "Large BBQ Chicken",                "pizza", "specialty",   16.99, 4.60, "all_day"),
    (11, "Large Spinach & Feta",             "pizza", "specialty",   16.49, 4.40, "all_day"),
    (12, "Large Ultimate Pepperoni",         "pizza", "pepperoni",   16.99, 4.80, "all_day"),
    (13, "Medium Thin-Crust Cheese",         "pizza", "cheese",       9.99, 2.70, "all_day"),
    (14, "Medium BBQ Chicken",               "pizza", "specialty",   14.99, 4.00, "all_day"),
    (15, "Large Philly Cheese Steak",        "pizza", "specialty",   17.49, 4.70, "all_day"),
    (16, "Large Buffalo Chicken",            "pizza", "specialty",   16.99, 4.50, "all_day"),
    (17, "Medium Extravaganzza",             "pizza", "specialty",   15.99, 4.50, "all_day"),
    (18, "Small Personal Cheese",            "pizza", "cheese",       6.99, 1.80, "all_day"),
    (19, "Large Honolulu Hawaiian",          "pizza", "specialty",   17.49, 4.80, "all_day"),
    # Wings
    (20, "8pc Traditional Wings Buffalo",    "wings", "traditional",  8.99, 2.20, "all_day"),
    (21, "8pc Traditional Wings BBQ",        "wings", "traditional",  8.99, 2.20, "all_day"),
    (22, "8pc Boneless Wings Mango Habanero","wings", "boneless",     8.99, 2.10, "all_day"),
    (23, "16pc Traditional Wings",           "wings", "traditional", 15.99, 4.00, "all_day"),
    (24, "16pc Boneless Wings",              "wings", "boneless",    15.99, 3.80, "all_day"),
    (25, "32pc Party Wings",                 "wings", "party",       29.99, 7.50, "all_day"),
    (26, "8pc Boneless Wings BBQ",           "wings", "boneless",     8.99, 2.10, "all_day"),
    (27, "8pc Boneless Wings Buffalo",       "wings", "boneless",     8.99, 2.10, "all_day"),
    (28, "8pc Traditional Wings Garlic Parm","wings", "traditional",  8.99, 2.20, "all_day"),
    # Sides
    (30, "Bread Twists Garlic",              "sides", "bread",        5.99, 1.20, "all_day"),
    (31, "Bread Twists Cheesy",              "sides", "bread",        6.49, 1.40, "all_day"),
    (32, "Stuffed Cheesy Bread",             "sides", "bread",        6.99, 1.60, "all_day"),
    (33, "Parmesan Bread Bites",             "sides", "bread",        4.99, 0.90, "all_day"),
    (34, "Pasta Primavera",                  "sides", "pasta",        7.99, 2.10, "lunch"),
    (35, "Chicken Alfredo",                  "sides", "pasta",        8.49, 2.40, "lunch"),
    (36, "Italian Sausage Marinara",         "sides", "pasta",        8.49, 2.30, "lunch"),
    (37, "Pepperoni Bread Twists",           "sides", "bread",        6.49, 1.40, "all_day"),
    (38, "Mac & Cheese Bites",               "sides", "snack",        5.99, 1.30, "all_day"),
    (39, "Chicken Habanero Bread Twists",    "sides", "bread",        6.99, 1.60, "all_day"),
    # Salads
    (40, "Garden Salad",                     "salads", "salad",       6.99, 1.80, "lunch"),
    (41, "Caesar Salad",                     "salads", "salad",       7.49, 2.00, "lunch"),
    (42, "Grilled Chicken Caesar Salad",     "salads", "salad",       9.49, 2.80, "lunch"),
    (43, "Southwest Salad",                  "salads", "salad",       8.99, 2.50, "lunch"),
    # Dips & Sauces
    (45, "Blue Cheese Dipping Cup",          "sides", "dip",          0.99, 0.15, "all_day"),
    (46, "Ranch Dipping Cup",                "sides", "dip",          0.99, 0.15, "all_day"),
    (47, "Marinara Sauce Cup",               "sides", "dip",          0.75, 0.10, "all_day"),
    (48, "Garlic Dipping Sauce Cup",         "sides", "dip",          0.75, 0.10, "all_day"),
    (49, "Sweet Mango Habanero Cup",         "sides", "dip",          0.99, 0.15, "all_day"),
    # Drinks
    (50, "2-Liter Coca-Cola",                "drinks", "soda",        3.29, 0.60, "all_day"),
    (51, "2-Liter Diet Coke",                "drinks", "soda",        3.29, 0.60, "all_day"),
    (52, "2-Liter Sprite",                   "drinks", "soda",        3.29, 0.60, "all_day"),
    (53, "20oz Coca-Cola",                   "drinks", "soda",        2.29, 0.40, "all_day"),
    (54, "20oz Diet Coke",                   "drinks", "soda",        2.29, 0.40, "all_day"),
    (55, "20oz Water",                       "drinks", "water",       1.99, 0.10, "all_day"),
    (56, "2-Liter Root Beer",                "drinks", "soda",        3.29, 0.60, "all_day"),
    (57, "20oz Sprite",                      "drinks", "soda",        2.29, 0.40, "all_day"),
    (58, "20oz Orange Fanta",                "drinks", "soda",        2.29, 0.40, "all_day"),
    # Desserts
    (60, "Lava Cake (2pc)",                  "desserts", "cake",      5.99, 1.10, "all_day"),
    (61, "Marble Cookie Brownie",            "desserts", "brownie",   5.99, 1.00, "all_day"),
    (62, "Cinnamon Bread Twists",            "desserts", "bread",     5.99, 1.20, "all_day"),
    (63, "Chocolate Lava Crunch (4pc)",      "desserts", "cake",      7.99, 1.60, "all_day"),
    (64, "Oreo Dessert Pizza",               "desserts", "special",   7.49, 1.50, "all_day"),
    (65, "Oven-Baked Brownie",               "desserts", "brownie",   6.49, 1.20, "all_day"),
    # LTO items (limited-time)
    (70, "Loaded Tots",                      "sides", "lto",          5.49, 1.30, "all_day"),
    (71, "New Yorker Pizza Large",           "pizza", "lto",         18.99, 5.20, "all_day"),
    (72, "Pepperoni Stuffed Cheesy Bread",   "sides", "lto",          7.49, 1.80, "all_day"),
    (73, "Spicy Sausage Pizza Large",        "pizza", "lto",         17.99, 4.90, "all_day"),
    (74, "Crispy Chicken Jalapeño Melt",     "sides", "lto",          8.49, 2.10, "all_day"),
    (75, "Handmade Pan Garlic Pizza Medium", "pizza", "lto",         13.99, 3.60, "all_day"),
]

def get_menu_items() -> list[dict]:
    return [
        {
            "menu_item_id": r[0],
            "item_name": r[1],
            "category": r[2],
            "subcategory": r[3],
            "base_price": r[4],
            "cost": r[5],
            "daypart": r[6],
            "item_status": "lto" if r[3] == "lto" else "active",
            "is_3pd_available": True,
            "is_olo_available": True,
            "is_delivery_available": True,
            "is_carryout_available": True,
        }
        for r in _MENU_ITEMS
    ]

# Simple BOM: each pizza uses ~0.5lb dough, ~0.2lb sauce, ~0.3lb cheese
_INGREDIENT_TEMPLATES = {
    "pizza":    [("dough_lb", 0.5), ("sauce_oz", 3.0), ("cheese_lb", 0.3)],
    "wings":    [("wings_raw_lb", 0.6), ("sauce_oz", 2.0)],
    "sides":    [("misc_ingredient_lb", 0.2)],
    "drinks":   [("syrup_oz", 0.5)],
    "desserts": [("dessert_mix_oz", 4.0)],
    "salads":   [("fresh_veg_lb", 0.3)],
}

def get_recipe_ingredients() -> list[dict]:
    rows = []
    ing_id = 1
    for item in get_menu_items():
        category = item["category"]
        template = _INGREDIENT_TEMPLATES.get(category, [("misc_ingredient_lb", 0.1)])
        for stock_sku, qty in template:
            rows.append({
                "recipe_ingredient_id": ing_id,
                "menu_item_id": item["menu_item_id"],
                "stock_sku": stock_sku,
                "quantity": qty,
                "unit_of_measure": stock_sku.split("_")[-1],
                "cost_per_unit": round(item["cost"] / len(template), 4),
            })
            ing_id += 1
    return rows

def get_item_price(menu_item_id: int, channel: str = "carryout") -> float:
    for r in _MENU_ITEMS:
        if r[0] == menu_item_id:
            surcharge = 0.75 if channel == "3pd_delivery" else 0.0
            return round(r[4] + surcharge, 2)
    raise ValueError(f"menu_item_id {menu_item_id} not found")

def get_items_for_daypart(hour: int) -> list[dict]:
    daypart = "lunch" if 10 <= hour <= 14 else "all_day"
    return [i for i in get_menu_items()
            if i["daypart"] == "all_day" or i["daypart"] == daypart]

def get_wing_item_ids() -> list[int]:
    return [i["menu_item_id"] for i in get_menu_items() if i["category"] == "wings"]
