# tests/test_entity_registry.py
from src.generator.entity_registry import EntityRegistry
from src.generator.reference.us_locations import generate_units
from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
from src.generator.reference.seeder import build_financial_periods_data

def _make_registry():
    units = generate_units(10)
    menu = get_menu_items()
    bom = get_recipe_ingredients()
    periods = build_financial_periods_data(backfill_months=3)
    return EntityRegistry(units=units, menu_items=menu, bom=bom,
                          financial_periods=periods, num_guests_per_unit=50)

def test_random_unit_id_in_range():
    reg = _make_registry()
    uid = reg.random_unit_id()
    assert 1 <= uid <= 10

def test_random_menu_item_returns_valid():
    reg = _make_registry()
    item = reg.random_menu_item(hour=19)
    assert item["menu_item_id"] > 0

def test_random_guest_profile_sometimes_none():
    reg = _make_registry()
    results = [reg.random_guest_profile_id(unit_id=1) for _ in range(100)]
    none_count = sum(1 for r in results if r is None)
    # ~60% should be None (unregistered guests)
    assert 40 < none_count < 80

def test_financial_period_for_date():
    from datetime import date
    reg = _make_registry()
    pid = reg.financial_period_for_date(date.today())
    assert pid is not None

def test_bom_for_item():
    reg = _make_registry()
    bom = reg.bom_for_item(menu_item_id=1)
    assert len(bom) > 0
