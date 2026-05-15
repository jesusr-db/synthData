from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

# (month, day) → (name, multiplier)
_FIXED_EVENTS: dict[tuple[int, int], tuple[str, float]] = {
    (10, 31): ("halloween", 1.8),
    (12, 24): ("christmas_eve", 1.1),
    (12, 25): ("christmas_day", 0.7),
    (12, 31): ("new_years_eve", 2.3),
}

# Super Bowl: second Sunday of February — approximated as Feb 9 for 2025 seed
# At runtime we compute it properly.
def _super_bowl_date(year: int) -> date:
    """Second Sunday of February."""
    d = date(year, 2, 1)
    days_until_sunday = (6 - d.weekday()) % 7
    first_sunday = d.replace(day=1 + days_until_sunday)
    return first_sunday.replace(day=first_sunday.day + 7)

def _is_nfl_sunday(d: date) -> bool:
    """NFL regular season: first Sunday of September through mid-January."""
    if d.weekday() != 6:
        return False
    return (d.month == 9 and d.day >= 7) or d.month in (10, 11, 12) or (d.month == 1 and d.day <= 15)

def _classify_event(ts: datetime) -> tuple[bool, Optional[str], float]:
    """Returns (is_holiday, holiday_name, multiplier)."""
    d = ts.date()
    if d == _super_bowl_date(d.year):
        return True, "super_bowl", 3.2
    if (d.month, d.day) in _FIXED_EVENTS:
        name, mult = _FIXED_EVENTS[(d.month, d.day)]
        return True, name, mult
    # Black Friday: day after Thanksgiving (4th Thursday of November)
    if d.month == 11 and d.weekday() == 4:
        thanksgiving = _nth_weekday(d.year, 11, 3, 4)  # 4th Thursday
        if d == thanksgiving.replace(day=thanksgiving.day + 1):
            return True, "black_friday", 1.6
    if _is_nfl_sunday(d):
        return True, "nfl_sunday", 2.0
    if d.month == 6 and d.weekday() == 4:  # summer friday
        return False, "summer_friday", 1.4
    if d.month == 1:
        return False, None, 0.85
    return False, None, 1.0

def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    d = date(year, month, 1)
    offset = (weekday - d.weekday()) % 7
    return d.replace(day=1 + offset + (n - 1) * 7)

HOURLY_MULTIPLIERS = {
    0: 0.05, 1: 0.12, 2: 0.10, 3: 0.03, 4: 0.02, 5: 0.02,
    6: 0.08, 7: 0.12, 8: 0.15, 9: 0.18, 10: 0.22,
    11: 0.55, 12: 0.80, 13: 0.70, 14: 0.42, 15: 0.30,
    16: 0.35, 17: 0.65, 18: 0.90, 19: 1.00, 20: 0.95,
    21: 0.75, 22: 0.60, 23: 0.45,
}

DOW_MULTIPLIERS = {0: 1.0, 1: 1.1, 2: 1.2, 3: 1.25, 4: 1.45, 5: 1.6, 6: 1.35}

BASE_CHANNEL_MIX = {
    "3pd_delivery": 0.40,
    "own_delivery": 0.16,
    "carryout": 0.40,
    "catering": 0.04,
}

BASE_TENDER_MIX = {
    "credit_card": 0.55,
    "digital_wallet": 0.22,
    "loyalty_redemption": 0.12,
    "cash": 0.11,
}

@dataclass
class CausalContext:
    unit_id: int
    timestamp: datetime
    hour_of_day: int
    day_of_week: int
    is_holiday: bool
    holiday_name: Optional[str]
    unit_volume_bias: float
    effective_order_volume: float
    channel_mix: dict
    tender_mix: dict
    sos_breach_probability: float
    cancellation_rate: float
    waste_probability: float
    # Phase 2 stubs — None until daily_refresh_job populates ref tables
    weather_condition: Optional[str] = None
    precipitation_inches: Optional[float] = None
    temperature_f: Optional[float] = None
    local_event_type: Optional[str] = None
    local_event_attendance: Optional[int] = None

def build_context(unit_id: int, timestamp: datetime, unit_volume_bias: float,
                  base_orders_per_hour: int = 18) -> CausalContext:
    is_holiday, holiday_name, event_mult = _classify_event(timestamp)
    hourly = HOURLY_MULTIPLIERS[timestamp.hour]
    dow = DOW_MULTIPLIERS[timestamp.weekday()]
    effective_volume = base_orders_per_hour * hourly * dow * event_mult * unit_volume_bias

    # Late-night delivery shift (10pm–1am): +15pp to delivery from carryout
    mix = dict(BASE_CHANNEL_MIX)
    if timestamp.hour >= 22 or timestamp.hour <= 1:
        shift = 0.15
        mix["3pd_delivery"] = min(1.0, mix["3pd_delivery"] + shift)
        mix["carryout"] = max(0.0, mix["carryout"] - shift)

    # SOS breach spikes with high event multiplier
    sos_base = 0.08
    sos = sos_base + max(0, (event_mult - 1.5) * 0.05)

    return CausalContext(
        unit_id=unit_id,
        timestamp=timestamp,
        hour_of_day=timestamp.hour,
        day_of_week=timestamp.weekday(),
        is_holiday=is_holiday,
        holiday_name=holiday_name,
        unit_volume_bias=unit_volume_bias,
        effective_order_volume=effective_volume,
        channel_mix=mix,
        tender_mix=dict(BASE_TENDER_MIX),
        sos_breach_probability=sos,
        cancellation_rate=0.025,
        waste_probability=0.03,
    )
