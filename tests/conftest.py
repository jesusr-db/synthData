import pytest

@pytest.fixture
def base_params():
    return {
        "catalog_name": "qsr_synth_test",
        "num_units": 5,
        "backfill_months": 1,
        "live_tick_seconds": 60,
        "base_orders_per_unit_per_hour": 18,
    }
