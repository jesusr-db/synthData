from src.features.affinity import (
    load_affinity,
    complement_score,
    cart_categories,
    is_suppressed_subcategory,
)


def test_load_affinity_has_all_categories():
    cfg = load_affinity()
    cats = {"pizza", "wings", "sides", "salads", "drinks", "desserts"}
    assert set(cfg["complements"].keys()) == cats


def test_complement_score_pizza_pulls_drinks_strongly():
    cfg = load_affinity()
    # basket of pizza -> drinks should outscore pizza -> salads
    drink = complement_score(["pizza"], "drinks", cfg)
    salad = complement_score(["pizza"], "salads", cfg)
    assert drink > salad
    assert 0.0 <= drink <= 1.0


def test_complement_score_aggregates_over_basket_categories():
    cfg = load_affinity()
    # pizza+wings should pull sides strictly harder than pizza alone (max wins)
    only_pizza = complement_score(["pizza"], "sides", cfg)
    pizza_wings = complement_score(["pizza", "wings"], "sides", cfg)
    assert pizza_wings > only_pizza


def test_complement_score_max_not_diluted_by_weak_item():
    cfg = load_affinity()
    # adding a weakly-complementary item must not lower the score
    # with max: complement_score(["pizza","salads"], "sides") >= complement_score(["pizza"], "sides")
    only_pizza = complement_score(["pizza"], "sides", cfg)
    pizza_salads = complement_score(["pizza", "salads"], "sides", cfg)
    assert pizza_salads >= only_pizza


def test_complement_score_empty_cart_is_zero():
    cfg = load_affinity()
    assert complement_score([], "drinks", cfg) == 0.0


def test_cart_categories_maps_item_ids_to_categories():
    # menu lookup: id -> (category, subcategory, name)
    menu = {1: ("pizza", "pepperoni", "Large Pepperoni"),
            53: ("drinks", "soda", "20oz Coca-Cola")}
    assert cart_categories([1, 53], menu) == {"pizza", "drinks"}


def test_is_suppressed_subcategory_true_for_soda_when_soda_in_cart():
    cfg = load_affinity()
    menu = {53: ("drinks", "soda", "20oz Coke")}
    assert is_suppressed_subcategory("soda", [53], menu, cfg) is True


def test_is_suppressed_subcategory_false_for_dessert():
    cfg = load_affinity()
    menu = {53: ("drinks", "soda", "20oz Coke")}
    assert is_suppressed_subcategory("cake", [53], menu, cfg) is False


def test_is_suppressed_false_when_subcat_in_list_but_not_in_cart():
    cfg = load_affinity()
    # soda IS in the suppress list, but the cart contains a dessert — not soda
    menu = {70: ("desserts", "cake", "Lava Cake")}
    assert is_suppressed_subcategory("soda", [70], menu, cfg) is False


def test_load_affinity_has_priority_and_suppress_keys():
    cfg = load_affinity()
    cats = {"pizza", "wings", "sides", "salads", "drinks", "desserts"}
    assert set(cfg["empty_cart_priority"]) == cats
    assert "soda" in cfg["suppress_duplicate_subcategories"]
