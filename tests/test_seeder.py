# tests/test_seeder.py
from src.generator.reference.seeder import (
    build_units_df_data, build_franchisees_data, build_financial_periods_data,
    build_suppliers_data
)

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
