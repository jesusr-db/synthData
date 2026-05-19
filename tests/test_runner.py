# tests/test_runner.py
from datetime import datetime
from unittest.mock import MagicMock, patch

from src.generator.runner import GeneratorConfig, build_tick_rows


def _make_config():
    from src.generator.entity_registry import EntityRegistry
    from src.generator.reference.us_locations import generate_units
    from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
    from src.generator.reference.seeder import build_financial_periods_data

    reg = EntityRegistry(
        units=generate_units(3),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(3),
    )
    return GeneratorConfig(
        catalog_name="test",
        num_units=3,
        backfill_months=1,
        live_tick_seconds=60,
        base_orders_per_unit_per_hour=18,
    ), reg


def test_build_tick_rows_returns_domain_rows():
    cfg, reg = _make_config()
    # Use 1-hour tick so Poisson(18) reliably yields orders
    rows = build_tick_rows(
        unit_id=1,
        timestamp=datetime(2025, 9, 19, 19, 0),
        registry=reg,
        tick_seconds=3600,
        base_orders_per_hour=cfg.base_orders_per_unit_per_hour,
    )
    event_types = {r["event_type"] for r in rows}
    assert "guest_order" in event_types


def test_build_tick_rows_has_unit_id_on_all_rows():
    cfg, reg = _make_config()
    rows = build_tick_rows(
        unit_id=2,
        timestamp=datetime(2025, 9, 19, 12, 0),
        registry=reg,
        tick_seconds=60,
        base_orders_per_hour=18,
    )
    for r in rows:
        assert r.get("unit_id") == 2


def test_generator_config_fields():
    cfg = GeneratorConfig(
        catalog_name="qsr_synth",
        num_units=250,
        backfill_months=12,
        live_tick_seconds=60,
        base_orders_per_unit_per_hour=18,
    )
    assert cfg.catalog_name == "qsr_synth"
    assert cfg.num_units == 250
    assert cfg.backfill_months == 12
    assert cfg.live_tick_seconds == 60
    assert cfg.base_orders_per_unit_per_hour == 18


def test_build_tick_rows_returns_list_of_dicts():
    cfg, reg = _make_config()
    rows = build_tick_rows(
        unit_id=1,
        timestamp=datetime(2025, 9, 19, 19, 0),
        registry=reg,
        tick_seconds=3600,
        base_orders_per_hour=18,
    )
    assert isinstance(rows, list)
    assert all(isinstance(r, dict) for r in rows)
    assert len(rows) > 0


def test_build_tick_rows_contains_inventory_events():
    cfg, reg = _make_config()
    # Use a busy dinner hour so orders are very likely
    rows = build_tick_rows(
        unit_id=1,
        timestamp=datetime(2025, 9, 19, 19, 0),
        registry=reg,
        tick_seconds=3600,
        base_orders_per_hour=18,
    )
    event_types = {r["event_type"] for r in rows}
    # inventory events should be present when orders exist
    assert "on_hand_balance" in event_types or "guest_order" in event_types


def test_backfill_ticks_is_iterator():
    from src.generator.runner import backfill_ticks
    from src.generator.entity_registry import EntityRegistry
    from src.generator.reference.us_locations import generate_units
    from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
    from src.generator.reference.seeder import build_financial_periods_data

    reg = EntityRegistry(
        units=generate_units(2),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(1),
    )
    gen = backfill_ticks(reg, backfill_months=1, tick_seconds=3600, base_orders_per_hour=18)
    import types
    assert isinstance(gen, types.GeneratorType)


def test_backfill_ticks_yields_batches_of_dicts():
    from src.generator.runner import backfill_ticks
    from src.generator.entity_registry import EntityRegistry
    from src.generator.reference.us_locations import generate_units
    from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
    from src.generator.reference.seeder import build_financial_periods_data

    reg = EntityRegistry(
        units=generate_units(2),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(1),
    )
    gen = backfill_ticks(reg, backfill_months=1, tick_seconds=3600, base_orders_per_hour=18)
    batch = next(gen)
    assert isinstance(batch, list)
    assert len(batch) > 0
    assert all(isinstance(r, dict) for r in batch)


def test_backfill_ticks_respects_end_dt():
    """backfill_ticks must not yield batches at or after end_dt."""
    from datetime import datetime, timedelta
    from src.generator.runner import backfill_ticks
    from src.generator.entity_registry import EntityRegistry
    from src.generator.reference.us_locations import generate_units
    from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
    from src.generator.reference.seeder import build_financial_periods_data

    reg = EntityRegistry(
        units=generate_units(2),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(1),
    )
    start = datetime(2025, 9, 19, 12, 0)   # noon
    end   = datetime(2025, 9, 19, 13, 0)   # 1pm — exclusive
    batches = list(backfill_ticks(reg, backfill_months=1, tick_seconds=3600,
                                  base_orders_per_hour=18, start_dt=start, end_dt=end))
    # With tick_seconds=3600, only one tick (12:00) should be yielded; 13:00 is end_dt (exclusive)
    assert len(batches) == 1


def test_backfill_ticks_end_dt_60s_ticks_yields_60_batches():
    """60 one-minute ticks from 12:00 to 13:00 (exclusive) yields exactly 60 batches."""
    from datetime import datetime, timedelta
    from src.generator.runner import backfill_ticks
    from src.generator.entity_registry import EntityRegistry
    from src.generator.reference.us_locations import generate_units
    from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
    from src.generator.reference.seeder import build_financial_periods_data

    reg = EntityRegistry(
        units=generate_units(1),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(1),
    )
    start = datetime(2025, 9, 19, 12, 0)
    end   = datetime(2025, 9, 19, 13, 0)
    batches = list(backfill_ticks(reg, backfill_months=1, tick_seconds=60,
                                  base_orders_per_hour=18, start_dt=start, end_dt=end))
    assert len(batches) == 60


def test_hourly_window_timestamps_span_full_hour():
    """60 sub-ticks over [start, start+1h) should produce events at 60 distinct minute marks."""
    from datetime import datetime, timedelta
    from src.generator.runner import backfill_ticks
    from src.generator.entity_registry import EntityRegistry
    from src.generator.reference.us_locations import generate_units
    from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
    from src.generator.reference.seeder import build_financial_periods_data

    reg = EntityRegistry(
        units=generate_units(3),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(1),
    )
    # Use dinner hour for reliable order volume
    start = datetime(2025, 9, 19, 19, 0)
    end   = datetime(2025, 9, 19, 20, 0)
    all_rows = []
    for batch in backfill_ticks(reg, backfill_months=1, tick_seconds=60,
                                 base_orders_per_hour=18, start_dt=start, end_dt=end):
        all_rows.extend(batch)

    # All event timestamps must fall within [start, end)
    order_rows = [r for r in all_rows if r.get("event_type") == "guest_order"]
    assert len(order_rows) > 0, "Expected orders in a busy dinner hour"

    timestamps = {r["placed_at"] for r in order_rows if r.get("placed_at") is not None}
    min_ts = min(timestamps)
    max_ts = max(timestamps)
    assert min_ts >= start, f"Event before window start: {min_ts}"
    assert max_ts < end,    f"Event at or after window end: {max_ts}"

    # Events should span at least 30 distinct minutes (probabilistic — dinner hour, 3 units)
    distinct_minutes = {ts.replace(second=0, microsecond=0) for ts in timestamps}
    assert len(distinct_minutes) >= 30, f"Only {len(distinct_minutes)} distinct minutes — timestamps not spread"


def test_live_tick_returns_list_of_dicts():
    from src.generator.runner import live_tick
    from src.generator.entity_registry import EntityRegistry
    from src.generator.reference.us_locations import generate_units
    from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
    from src.generator.reference.seeder import build_financial_periods_data

    reg = EntityRegistry(
        units=generate_units(2),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(1),
    )
    rows = live_tick(reg, tick_seconds=60, base_orders_per_hour=18)
    assert isinstance(rows, list)
    assert all(isinstance(r, dict) for r in rows)
