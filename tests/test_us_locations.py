# tests/test_us_locations.py
from src.generator.reference.us_locations import generate_units

def test_generates_correct_count():
    units = generate_units(10)
    assert len(units) == 10

def test_unit_has_required_fields():
    units = generate_units(5)
    required = {"unit_id", "unit_name", "city", "state", "lat", "lon",
                "metro_area", "district_id", "region_id", "franchisee_id",
                "format", "unit_volume_bias", "is_franchise"}
    for u in units:
        assert required.issubset(u.keys())

def test_units_span_multiple_metros():
    units = generate_units(50)
    metros = {u["metro_area"] for u in units}
    assert len(metros) >= 5

def test_franchise_ratio():
    units = generate_units(100)
    franchise_count = sum(1 for u in units if u["is_franchise"])
    assert 70 <= franchise_count <= 90  # ~80% franchised
