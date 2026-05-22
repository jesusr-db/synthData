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


def test_weather_event_data_applies_demand_multiplier():
    from src.generator.causal_context import build_context
    ts = datetime(2025, 6, 1, 12, 0)
    ctx_plain = build_context(unit_id=1, timestamp=ts, unit_volume_bias=1.0)
    ctx_rain = build_context(
        unit_id=1, timestamp=ts, unit_volume_bias=1.0,
        weather_event_data={"demand_multiplier": 0.85, "channel_shift_delivery": 0.0},
    )
    assert ctx_rain.effective_order_volume < ctx_plain.effective_order_volume
    assert abs(ctx_rain.effective_order_volume - ctx_plain.effective_order_volume * 0.85) < 0.01


def test_weather_event_data_populates_stub_fields():
    from src.generator.causal_context import build_context
    ts = datetime(2025, 9, 13, 19, 0)
    ctx = build_context(
        unit_id=1, timestamp=ts, unit_volume_bias=1.0,
        weather_event_data={
            "demand_multiplier": 1.0,
            "channel_shift_delivery": 0.0,
            "weather_condition": "rain",
            "precipitation_inches": 0.45,
            "high_temp_f": 65.0,
            "event_category": "major_sports",
            "est_attendance": 80000,
            "event_demand_multiplier": 1.6,
        },
    )
    assert ctx.weather_condition == "rain"
    assert ctx.precipitation_inches == 0.45
    assert ctx.temperature_f == 65.0
    assert ctx.local_event_type == "major_sports"
    assert ctx.local_event_attendance == 80000


def test_weather_event_data_shifts_delivery_channel():
    from src.generator.causal_context import build_context
    ts = datetime(2025, 6, 1, 12, 0)
    ctx_rain = build_context(
        unit_id=1, timestamp=ts, unit_volume_bias=1.0,
        weather_event_data={"demand_multiplier": 1.0, "channel_shift_delivery": 0.15},
    )
    ctx_plain = build_context(unit_id=1, timestamp=ts, unit_volume_bias=1.0)
    assert ctx_rain.channel_mix["3pd_delivery"] > ctx_plain.channel_mix["3pd_delivery"]
    assert ctx_rain.channel_mix["carryout"] < ctx_plain.channel_mix["carryout"]


def test_none_weather_event_data_leaves_fields_none():
    from src.generator.causal_context import build_context
    ts = datetime(2025, 6, 1, 12, 0)
    ctx = build_context(unit_id=1, timestamp=ts, unit_volume_bias=1.0, weather_event_data=None)
    assert ctx.weather_condition is None
    assert ctx.local_event_type is None
