# tests/test_demand_model.py
from datetime import datetime
from src.generator.demand_model import orders_for_tick, channel_for_order
from src.generator.causal_context import build_context

def test_orders_for_tick_returns_nonnegative():
    ctx = build_context(1, datetime(2025, 6, 13, 19, 0), 1.0)
    n = orders_for_tick(ctx, tick_seconds=60)
    assert n >= 0

def test_dinner_friday_more_than_monday_morning():
    # Use a 1-hour tick so effective_order_volume differences are clearly visible.
    # Friday 19:00 yields ~51 orders/hr; Monday 08:00 yields ~2.7 orders/hr.
    ctx_fri = build_context(1, datetime(2025, 6, 13, 19, 0), 1.0)
    ctx_mon = build_context(1, datetime(2025, 6, 9, 8, 0), 1.0)
    assert orders_for_tick(ctx_fri, 3600) > orders_for_tick(ctx_mon, 3600)

def test_channel_for_order_valid():
    ctx = build_context(1, datetime(2025, 6, 10, 12, 0), 1.0)
    ch = channel_for_order(ctx)
    assert ch in ("3pd_delivery", "own_delivery", "carryout", "catering")

def test_late_night_skews_delivery():
    results = [channel_for_order(build_context(1, datetime(2025, 6, 10, 23, 0), 1.0))
               for _ in range(200)]
    delivery_rate = sum(1 for r in results if "delivery" in r) / 200
    assert delivery_rate > 0.50
