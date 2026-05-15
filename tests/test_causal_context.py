# tests/test_causal_context.py
from datetime import datetime
import pytest
from src.generator.causal_context import CausalContext, build_context

def test_build_context_sets_hour_and_dow():
    ts = datetime(2025, 1, 6, 19, 30)  # Monday 7:30pm
    ctx = build_context(unit_id=1, timestamp=ts, unit_volume_bias=1.0)
    assert ctx.hour_of_day == 19
    assert ctx.day_of_week == 0  # Monday

def test_build_context_detects_super_bowl():
    ts = datetime(2025, 2, 9, 18, 0)  # Super Bowl Sunday 2025
    ctx = build_context(unit_id=1, timestamp=ts, unit_volume_bias=1.0)
    assert ctx.is_holiday is True
    assert ctx.holiday_name == "super_bowl"

def test_phase2_fields_are_none():
    ts = datetime(2025, 6, 1, 12, 0)
    ctx = build_context(unit_id=1, timestamp=ts, unit_volume_bias=1.0)
    assert ctx.weather_condition is None
    assert ctx.local_event_type is None

def test_effective_order_volume_is_positive():
    ts = datetime(2025, 11, 7, 19, 0)  # Friday dinner
    ctx = build_context(unit_id=1, timestamp=ts, unit_volume_bias=1.0)
    assert ctx.effective_order_volume > 0

def test_unit_volume_bias_scales_volume():
    ts = datetime(2025, 6, 10, 19, 0)
    ctx_low = build_context(unit_id=1, timestamp=ts, unit_volume_bias=0.8)
    ctx_high = build_context(unit_id=2, timestamp=ts, unit_volume_bias=1.2)
    assert ctx_high.effective_order_volume > ctx_low.effective_order_volume
