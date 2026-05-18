# tests/test_seeder.py
from src.generator.reference.seeder import (
    build_units_df_data, build_franchisees_data, build_financial_periods_data,
    build_suppliers_data
)

def test_build_item_price_data_returns_valid_multipliers():
    from src.generator.reference.seeder import build_item_price_data
    periods = build_financial_periods_data(6)
    rows = build_item_price_data(periods)
    assert len(rows) > 0
    for row in rows:
        assert "menu_item_id" in row
        assert "financial_period_id" in row
        assert 0.7 <= row["price_multiplier"] <= 1.4, \
            f"price_multiplier {row['price_multiplier']} out of bounds"

def test_build_units_returns_correct_count():
    rows = build_units_df_data(num_units=10)
    assert len(rows) == 10

def test_financial_periods_covers_backfill(base_params):
    periods = build_financial_periods_data(backfill_months=12)
    assert len(periods) >= 12

def test_franchisees_data():
    rows = build_franchisees_data(num_units=100)
    assert len(rows) > 0
    assert all("franchisee_id" in r for r in rows)

def test_suppliers_data():
    rows = build_suppliers_data()
    assert len(rows) >= 5
    assert all("supplier_id" in r for r in rows)
