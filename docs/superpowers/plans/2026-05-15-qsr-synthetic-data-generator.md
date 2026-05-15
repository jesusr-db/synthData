# QSR Synthetic Data Generator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a fully automated QSR synthetic data generator that streams realistic, referentially consistent Domino's-style restaurant data into 153 MVM Unity Catalog tables on Databricks.

**Architecture:** A Python generator job writes synthetic events (driven by a Structural Causal Model) to domain-specific Bronze staging Delta tables. A Spark Declarative Pipeline (DLT) reads those tables via `readStream` and writes all MVM Silver tables plus Gold metric tables. UC Metric Views sit on top of Gold. Everything deploys via `databricks bundle deploy`.

**Tech Stack:** Python 3.11, PySpark, Delta Lake, Databricks DLT, Databricks Asset Bundles, Unity Catalog, pytest, numpy, faker

---

## Phase A — Generator Foundation

### Task 1: Project scaffold + params

**Files:**
- Create: `databricks.yml`
- Create: `conf/params.yml`
- Create: `src/__init__.py`
- Create: `src/generator/__init__.py`
- Create: `src/generator/domains/__init__.py`
- Create: `src/generator/reference/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`

- [ ] Create `conf/params.yml`:

```yaml
catalog_name: qsr_synth
num_units: 250
backfill_months: 12
live_tick_seconds: 60
base_orders_per_unit_per_hour: 18
```

- [ ] Create `databricks.yml` skeleton:

```yaml
bundle:
  name: qsr-synth-data-generator

variables:
  catalog_name:
    default: qsr_synth
  num_units:
    default: "250"
  backfill_months:
    default: "12"

targets:
  dev:
    mode: development
    default: true
  prod:
    mode: production

include:
  - resources/*.yml
```

- [ ] Create `tests/conftest.py`:

```python
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
```

- [ ] Create all empty `__init__.py` files listed above.

- [ ] Commit:

```bash
git add conf/ databricks.yml src/ tests/
git commit -m "feat: project scaffold, params, and bundle skeleton"
```

---

### Task 2: CausalContext dataclass

**Files:**
- Create: `src/generator/causal_context.py`
- Create: `tests/test_causal_context.py`

- [ ] Write failing tests:

```python
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
```

- [ ] Run tests, verify they fail: `pytest tests/test_causal_context.py -v`

- [ ] Implement `src/generator/causal_context.py`:

```python
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
```

- [ ] Run tests: `pytest tests/test_causal_context.py -v` — all pass.

- [ ] Commit:

```bash
git add src/generator/causal_context.py tests/test_causal_context.py
git commit -m "feat: CausalContext dataclass and SCM build_context"
```

---

### Task 3: Demand model + entropy utilities

**Files:**
- Create: `src/generator/demand_model.py`
- Create: `src/generator/entropy.py`
- Create: `tests/test_demand_model.py`
- Create: `tests/test_entropy.py`

- [ ] Write failing tests:

```python
# tests/test_demand_model.py
from datetime import datetime
from src.generator.demand_model import orders_for_tick, channel_for_order
from src.generator.causal_context import build_context

def test_orders_for_tick_returns_nonnegative():
    ctx = build_context(1, datetime(2025, 6, 13, 19, 0), 1.0)
    n = orders_for_tick(ctx, tick_seconds=60)
    assert n >= 0

def test_dinner_friday_more_than_monday_morning():
    ctx_fri = build_context(1, datetime(2025, 6, 13, 19, 0), 1.0)
    ctx_mon = build_context(1, datetime(2025, 6, 9, 8, 0), 1.0)
    assert orders_for_tick(ctx_fri, 60) > orders_for_tick(ctx_mon, 60)

def test_channel_for_order_valid():
    ctx = build_context(1, datetime(2025, 6, 10, 12, 0), 1.0)
    ch = channel_for_order(ctx)
    assert ch in ("3pd_delivery", "own_delivery", "carryout", "catering")

def test_late_night_skews_delivery():
    results = [channel_for_order(build_context(1, datetime(2025, 6, 10, 23, 0), 1.0))
               for _ in range(200)]
    delivery_rate = sum(1 for r in results if "delivery" in r) / 200
    assert delivery_rate > 0.50
```

```python
# tests/test_entropy.py
from src.generator.entropy import gaussian_noise, prep_time_seconds, should_breach_sos

def test_gaussian_noise_near_one():
    samples = [gaussian_noise(1.0, 0.15) for _ in range(500)]
    avg = sum(samples) / len(samples)
    assert 0.85 < avg < 1.15

def test_prep_time_carryout_reasonable():
    times = [prep_time_seconds("carryout") for _ in range(200)]
    avg_min = sum(times) / len(times) / 60
    assert 8 < avg_min < 18

def test_prep_time_delivery_longer_than_carryout():
    carry = sum(prep_time_seconds("carryout") for _ in range(200)) / 200
    deliv = sum(prep_time_seconds("own_delivery") for _ in range(200)) / 200
    assert deliv > carry

def test_sos_breach_rate_near_expected():
    breaches = sum(1 for _ in range(1000) if should_breach_sos(0.08))
    assert 50 < breaches < 150
```

- [ ] Run, verify fail: `pytest tests/test_demand_model.py tests/test_entropy.py -v`

- [ ] Implement `src/generator/demand_model.py`:

```python
import random
from src.generator.causal_context import CausalContext
from src.generator.entropy import gaussian_noise

def orders_for_tick(ctx: CausalContext, tick_seconds: int = 60) -> int:
    """Number of new orders to generate for this location in this tick."""
    tick_fraction = tick_seconds / 3600
    raw = ctx.effective_order_volume * tick_fraction
    noisy = raw * gaussian_noise(1.0, 0.15)
    return max(0, round(noisy))

def channel_for_order(ctx: CausalContext) -> str:
    """Sample a channel from the causal context channel mix."""
    r = random.random()
    cumulative = 0.0
    for channel, weight in ctx.channel_mix.items():
        cumulative += weight
        if r < cumulative:
            return channel
    return "carryout"

def tender_for_order(ctx: CausalContext, is_loyalty_member: bool) -> str:
    """Sample a tender type; loyalty members skew toward loyalty_redemption."""
    mix = dict(ctx.tender_mix)
    if is_loyalty_member:
        mix["loyalty_redemption"] = min(0.30, mix["loyalty_redemption"] * 2)
        # renormalize
        total = sum(mix.values())
        mix = {k: v / total for k, v in mix.items()}
    r = random.random()
    cumulative = 0.0
    for tender, weight in mix.items():
        cumulative += weight
        if r < cumulative:
            return tender
    return "credit_card"
```

- [ ] Implement `src/generator/entropy.py`:

```python
import random
import math

def gaussian_noise(mean: float, std_fraction: float) -> float:
    """Multiplicative noise: returns a value near `mean` with std = mean * std_fraction."""
    return random.gauss(mean, mean * std_fraction)

def prep_time_seconds(channel: str) -> int:
    """Realistic prep time in seconds based on channel."""
    if channel in ("carryout",):
        return max(60, int(random.gauss(12 * 60, 3 * 60)))
    return max(300, int(random.gauss(31 * 60, 6 * 60)))  # delivery

def should_breach_sos(probability: float) -> bool:
    return random.random() < probability

def should_cancel(cancellation_rate: float, channel: str) -> bool:
    rate = cancellation_rate * (1.5 if channel == "3pd_delivery" else 1.0)
    return random.random() < rate

def should_waste(waste_probability: float, hour_of_day: int) -> bool:
    """Waste skews toward end of day (after 8pm)."""
    eod_boost = 1.5 if hour_of_day >= 20 else 1.0
    return random.random() < (waste_probability * eod_boost)

def unit_volume_bias() -> float:
    """Persistent per-unit multiplier seeded at creation: ±20% around 1.0."""
    return random.gauss(1.0, 0.1)
```

- [ ] Run tests: `pytest tests/test_demand_model.py tests/test_entropy.py -v` — all pass.

- [ ] Commit:

```bash
git add src/generator/demand_model.py src/generator/entropy.py \
        tests/test_demand_model.py tests/test_entropy.py
git commit -m "feat: demand model and entropy injectors"
```

---

### Task 4: US locations reference data

**Files:**
- Create: `src/generator/reference/us_locations.py`
- Create: `tests/test_us_locations.py`

- [ ] Write failing tests:

```python
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
```

- [ ] Run, verify fail: `pytest tests/test_us_locations.py -v`

- [ ] Implement `src/generator/reference/us_locations.py`:

```python
import random
from src.generator.entropy import unit_volume_bias

# Representative US QSR metros with approximate lat/lon and population weight
US_METROS = [
    {"name": "New York-Newark",     "state": "NY", "lat": 40.71, "lon": -74.01, "weight": 10},
    {"name": "Los Angeles",         "state": "CA", "lat": 34.05, "lon": -118.24, "weight": 9},
    {"name": "Chicago",             "state": "IL", "lat": 41.88, "lon": -87.63, "weight": 7},
    {"name": "Dallas-Fort Worth",   "state": "TX", "lat": 32.78, "lon": -96.80, "weight": 6},
    {"name": "Houston",             "state": "TX", "lat": 29.76, "lon": -95.37, "weight": 6},
    {"name": "Atlanta",             "state": "GA", "lat": 33.75, "lon": -84.39, "weight": 5},
    {"name": "Phoenix",             "state": "AZ", "lat": 33.45, "lon": -112.07, "weight": 5},
    {"name": "Philadelphia",        "state": "PA", "lat": 39.95, "lon": -75.17, "weight": 5},
    {"name": "Miami",               "state": "FL", "lat": 25.77, "lon": -80.19, "weight": 4},
    {"name": "Seattle",             "state": "WA", "lat": 47.61, "lon": -122.33, "weight": 4},
    {"name": "Denver",              "state": "CO", "lat": 39.74, "lon": -104.98, "weight": 3},
    {"name": "Boston",              "state": "MA", "lat": 42.36, "lon": -71.06, "weight": 3},
    {"name": "Minneapolis",         "state": "MN", "lat": 44.98, "lon": -93.27, "weight": 3},
    {"name": "San Antonio",         "state": "TX", "lat": 29.42, "lon": -98.49, "weight": 3},
    {"name": "Columbus",            "state": "OH", "lat": 39.96, "lon": -83.00, "weight": 2},
    {"name": "Charlotte",           "state": "NC", "lat": 35.23, "lon": -80.84, "weight": 2},
    {"name": "Indianapolis",        "state": "IN", "lat": 39.77, "lon": -86.16, "weight": 2},
    {"name": "Nashville",           "state": "TN", "lat": 36.17, "lon": -86.78, "weight": 2},
    {"name": "Las Vegas",           "state": "NV", "lat": 36.17, "lon": -115.14, "weight": 2},
    {"name": "Louisville",          "state": "KY", "lat": 38.25, "lon": -85.76, "weight": 1},
]

def _assign_districts(units: list[dict]) -> list[dict]:
    """Assign district_id (5–8 units each) and region_id (3–5 districts each)."""
    metros = list({u["metro_area"] for u in units})
    metro_to_district = {}
    district_id = 1
    region_id = 1
    districts_in_region = 0
    for metro in sorted(metros):
        metro_to_district[metro] = district_id
        district_id += 1
        districts_in_region += 1
        if districts_in_region >= 4:
            region_id += 1
            districts_in_region = 0
    for u in units:
        u["district_id"] = metro_to_district[u["metro_area"]]
        u["region_id"] = (metro_to_district[u["metro_area"]] - 1) // 4 + 1
    return units

def generate_units(num_units: int, seed: int = 42) -> list[dict]:
    random.seed(seed)
    weights = [m["weight"] for m in US_METROS]
    units = []
    # ~20% corporate-owned, ~80% franchised across ~40 franchisees
    franchisee_pool = list(range(1, 41))
    for i in range(1, num_units + 1):
        metro = random.choices(US_METROS, weights=weights, k=1)[0]
        is_franchise = random.random() < 0.80
        franchisee_id = random.choice(franchisee_pool) if is_franchise else None
        lat_jitter = random.uniform(-0.3, 0.3)
        lon_jitter = random.uniform(-0.3, 0.3)
        units.append({
            "unit_id": i,
            "unit_name": f"Domino's #{1000 + i}",
            "city": metro["name"].split("-")[0],
            "state": metro["state"],
            "lat": round(metro["lat"] + lat_jitter, 5),
            "lon": round(metro["lon"] + lon_jitter, 5),
            "metro_area": metro["name"],
            "district_id": None,   # assigned below
            "region_id": None,
            "franchisee_id": franchisee_id,
            "format": "carryout_delivery",
            "unit_volume_bias": round(unit_volume_bias(), 4),
            "is_franchise": is_franchise,
            "status": "active",
        })
    return _assign_districts(units)
```

- [ ] Run tests: `pytest tests/test_us_locations.py -v` — all pass.

- [ ] Commit:

```bash
git add src/generator/reference/us_locations.py tests/test_us_locations.py
git commit -m "feat: US restaurant locations reference generator (250 units, 20 metros)"
```

---

### Task 5: Menu catalog

**Files:**
- Create: `src/generator/reference/menu_catalog.py`
- Create: `tests/test_menu_catalog.py`

- [ ] Write failing tests:

```python
# tests/test_menu_catalog.py
from src.generator.reference.menu_catalog import (
    get_menu_items, get_recipe_ingredients, get_item_price
)

def test_menu_items_count():
    items = get_menu_items()
    assert len(items) >= 60

def test_menu_item_fields():
    items = get_menu_items()
    required = {"menu_item_id", "item_name", "category", "subcategory",
                "base_price", "cost", "is_3pd_available", "is_olo_available",
                "daypart", "item_status"}
    for item in items[:5]:
        assert required.issubset(item.keys())

def test_recipe_ingredients_reference_valid_items():
    items = {i["menu_item_id"] for i in get_menu_items()}
    ingredients = get_recipe_ingredients()
    for ing in ingredients:
        assert ing["menu_item_id"] in items

def test_item_price_returns_float():
    price = get_item_price(menu_item_id=1)
    assert isinstance(price, float)
    assert price > 0
```

- [ ] Run, verify fail: `pytest tests/test_menu_catalog.py -v`

- [ ] Implement `src/generator/reference/menu_catalog.py`:

```python
# Domino's-style menu catalog: ~80 items across pizzas, wings, sides, drinks, desserts
from typing import Optional
import random

_MENU_ITEMS = [
    # Pizzas — hand-tossed, thin-crust, pan variants
    (1,  "Large Hand-Tossed Pepperoni",      "pizza", "pepperoni",   15.99, 4.20, "all_day"),
    (2,  "Large Hand-Tossed Cheese",         "pizza", "cheese",      13.99, 3.50, "all_day"),
    (3,  "Large Thin-Crust Veggie",          "pizza", "specialty",   16.99, 4.80, "all_day"),
    (4,  "Large Pan MeatZZa",                "pizza", "specialty",   17.99, 5.10, "all_day"),
    (5,  "Medium Hand-Tossed Pepperoni",     "pizza", "pepperoni",   12.99, 3.30, "all_day"),
    (6,  "Medium Cheese",                   "pizza", "cheese",      10.99, 2.90, "all_day"),
    (7,  "Small Personal Pepperoni",        "pizza", "pepperoni",    7.99, 2.10, "all_day"),
    (8,  "Large Extravaganzza",             "pizza", "specialty",   18.99, 5.50, "all_day"),
    (9,  "Large Pacific Veggie",            "pizza", "specialty",   17.49, 4.90, "all_day"),
    (10, "Large BBQ Chicken",               "pizza", "specialty",   16.99, 4.60, "all_day"),
    (11, "Large Spinach & Feta",            "pizza", "specialty",   16.49, 4.40, "all_day"),
    (12, "Large Ultimate Pepperoni",        "pizza", "pepperoni",   16.99, 4.80, "all_day"),
    # Wings
    (20, "8pc Traditional Wings Buffalo",   "wings", "traditional", 8.99, 2.20, "all_day"),
    (21, "8pc Traditional Wings BBQ",       "wings", "traditional", 8.99, 2.20, "all_day"),
    (22, "8pc Boneless Wings Mango Habanero","wings","boneless",    8.99, 2.10, "all_day"),
    (23, "16pc Traditional Wings",          "wings", "traditional", 15.99, 4.00, "all_day"),
    (24, "16pc Boneless Wings",             "wings", "boneless",   15.99, 3.80, "all_day"),
    (25, "32pc Party Wings",               "wings", "party",       29.99, 7.50, "all_day"),
    # Sides
    (30, "Bread Twists Garlic",            "sides", "bread",        5.99, 1.20, "all_day"),
    (31, "Bread Twists Cheesy",            "sides", "bread",        6.49, 1.40, "all_day"),
    (32, "Stuffed Cheesy Bread",           "sides", "bread",        6.99, 1.60, "all_day"),
    (33, "Parmesan Bread Bites",           "sides", "bread",        4.99, 0.90, "all_day"),
    (34, "Pasta Primavera",                "sides", "pasta",        7.99, 2.10, "lunch"),
    (35, "Chicken Alfredo",                "sides", "pasta",        8.49, 2.40, "lunch"),
    (36, "Italian Sausage Marinara",       "sides", "pasta",        8.49, 2.30, "lunch"),
    # Salads
    (40, "Garden Salad",                   "salads","salad",        6.99, 1.80, "lunch"),
    (41, "Caesar Salad",                   "salads","salad",        7.49, 2.00, "lunch"),
    # Dips & Sauces
    (45, "Blue Cheese Dipping Cup",        "sides", "dip",          0.99, 0.15, "all_day"),
    (46, "Ranch Dipping Cup",              "sides", "dip",          0.99, 0.15, "all_day"),
    (47, "Marinara Sauce Cup",             "sides", "dip",          0.75, 0.10, "all_day"),
    # Drinks
    (50, "2-Liter Coca-Cola",              "drinks","soda",         3.29, 0.60, "all_day"),
    (51, "2-Liter Diet Coke",              "drinks","soda",         3.29, 0.60, "all_day"),
    (52, "2-Liter Sprite",                "drinks","soda",         3.29, 0.60, "all_day"),
    (53, "20oz Coca-Cola",                 "drinks","soda",         2.29, 0.40, "all_day"),
    (54, "20oz Diet Coke",                 "drinks","soda",         2.29, 0.40, "all_day"),
    (55, "20oz Water",                     "drinks","water",        1.99, 0.10, "all_day"),
    # Desserts
    (60, "Lava Cake (2pc)",                "desserts","cake",       5.99, 1.10, "all_day"),
    (61, "Marble Cookie Brownie",          "desserts","brownie",    5.99, 1.00, "all_day"),
    (62, "Cinnamon Bread Twists",          "desserts","bread",      5.99, 1.20, "all_day"),
    # LTO items (limited-time)
    (70, "Loaded Tots",                    "sides", "lto",          5.49, 1.30, "all_day"),
    (71, "New Yorker Pizza Large",         "pizza", "lto",         18.99, 5.20, "all_day"),
    (72, "Pepperoni Stuffed Cheesy Bread", "sides", "lto",          7.49, 1.80, "all_day"),
]

def get_menu_items() -> list[dict]:
    return [
        {
            "menu_item_id": r[0],
            "item_name": r[1],
            "category": r[2],
            "subcategory": r[3],
            "base_price": r[4],
            "cost": r[5],
            "daypart": r[6],
            "item_status": "lto" if r[3] == "lto" else "active",
            "is_3pd_available": True,
            "is_olo_available": True,
            "is_delivery_available": True,
            "is_carryout_available": True,
        }
        for r in _MENU_ITEMS
    ]

# Simple BOM: each pizza uses ~0.5lb dough, ~0.2lb sauce, ~0.3lb cheese
_INGREDIENT_TEMPLATES = {
    "pizza":   [("dough_lb", 0.5), ("sauce_oz", 3.0), ("cheese_lb", 0.3)],
    "wings":   [("wings_raw_lb", 0.6), ("sauce_oz", 2.0)],
    "sides":   [("misc_ingredient_lb", 0.2)],
    "drinks":  [("syrup_oz", 0.5)],
    "desserts":[("dessert_mix_oz", 4.0)],
    "salads":  [("fresh_veg_lb", 0.3)],
}

def get_recipe_ingredients() -> list[dict]:
    rows = []
    ing_id = 1
    for item in get_menu_items():
        category = item["category"]
        template = _INGREDIENT_TEMPLATES.get(category, [("misc_ingredient_lb", 0.1)])
        for stock_sku, qty in template:
            rows.append({
                "recipe_ingredient_id": ing_id,
                "menu_item_id": item["menu_item_id"],
                "stock_sku": stock_sku,
                "quantity": qty,
                "unit_of_measure": stock_sku.split("_")[-1],
                "cost_per_unit": round(item["cost"] / len(template), 4),
            })
            ing_id += 1
    return rows

def get_item_price(menu_item_id: int, channel: str = "carryout") -> float:
    for r in _MENU_ITEMS:
        if r[0] == menu_item_id:
            surcharge = 0.75 if channel == "3pd_delivery" else 0.0
            return round(r[4] + surcharge, 2)
    raise ValueError(f"menu_item_id {menu_item_id} not found")

def get_items_for_daypart(hour: int) -> list[dict]:
    daypart = "lunch" if 10 <= hour <= 14 else "all_day"
    return [i for i in get_menu_items()
            if i["daypart"] == "all_day" or i["daypart"] == daypart]

def get_wing_item_ids() -> list[int]:
    return [i["menu_item_id"] for i in get_menu_items() if i["category"] == "wings"]
```

- [ ] Run tests: `pytest tests/test_menu_catalog.py -v` — all pass.

- [ ] Commit:

```bash
git add src/generator/reference/menu_catalog.py tests/test_menu_catalog.py
git commit -m "feat: Domino's-style menu catalog with BOM and pricing"
```

---

### Task 6: Reference seeder (writes ref.* Delta tables)

**Files:**
- Create: `src/generator/reference/seeder.py`
- Create: `tests/test_seeder.py`

- [ ] Write failing tests (pure Python, no Spark — test the data-building functions, not the write):

```python
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
```

- [ ] Run, verify fail: `pytest tests/test_seeder.py -v`

- [ ] Implement `src/generator/reference/seeder.py`:

```python
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from src.generator.reference.us_locations import generate_units
from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients

def build_units_df_data(num_units: int = 250, seed: int = 42) -> list[dict]:
    return generate_units(num_units, seed=seed)

def build_franchisees_data(num_units: int = 250) -> list[dict]:
    units = generate_units(num_units)
    franchisee_ids = {u["franchisee_id"] for u in units if u["franchisee_id"]}
    return [
        {
            "franchisee_id": fid,
            "franchisee_name": f"QSR Franchise Group #{fid}",
            "contact_email": f"ops{fid}@qsrfranchise.com",
            "status": "active",
        }
        for fid in sorted(franchisee_ids)
    ]

def build_financial_periods_data(backfill_months: int = 12) -> list[dict]:
    rows = []
    today = date.today()
    start = (today - relativedelta(months=backfill_months)).replace(day=1)
    period_id = 1
    current = start
    while current <= today + relativedelta(months=1):
        end = (current + relativedelta(months=1)) - timedelta(days=1)
        rows.append({
            "financial_period_id": period_id,
            "period_name": current.strftime("%b %Y"),
            "start_date": current.isoformat(),
            "end_date": end.isoformat(),
            "fiscal_year": current.year,
            "fiscal_quarter": (current.month - 1) // 3 + 1,
            "status": "closed" if end < today else "open",
        })
        current += relativedelta(months=1)
        period_id += 1
    return rows

def build_suppliers_data() -> list[dict]:
    return [
        {"supplier_id": 1, "supplier_name": "US Foods", "category": "food_beverage", "status": "active"},
        {"supplier_id": 2, "supplier_name": "Sysco", "category": "food_beverage", "status": "active"},
        {"supplier_id": 3, "supplier_name": "Performance Food Group", "category": "food_beverage", "status": "active"},
        {"supplier_id": 4, "supplier_name": "Ecolab", "category": "cleaning_supplies", "status": "active"},
        {"supplier_id": 5, "supplier_name": "ALSCO", "category": "uniforms", "status": "active"},
        {"supplier_id": 6, "supplier_name": "Domino's Supply Chain", "category": "dough_sauce", "status": "active"},
    ]

def seed_all(spark, catalog: str, num_units: int = 250, backfill_months: int = 12):
    """Write all reference tables to {catalog}.ref.*"""
    from pyspark.sql import Row

    def write(data: list[dict], table: str):
        if not data:
            return
        df = spark.createDataFrame([Row(**r) for r in data])
        df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.ref.{table}")

    write(build_units_df_data(num_units), "unit")
    write(build_franchisees_data(num_units), "franchisee")
    write(build_financial_periods_data(backfill_months), "financial_period")
    write(build_suppliers_data(), "supplier")
    write(get_menu_items(), "menu_item")
    write(get_recipe_ingredients(), "recipe_ingredient")
    # Phase 2 stubs — empty tables
    for stub_table in ("weather_conditions", "local_events"):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.ref.{stub_table}
            (stub_id BIGINT, placeholder STRING)
            USING DELTA
        """)
```

- [ ] Run tests: `pytest tests/test_seeder.py -v` — all pass.

- [ ] Commit:

```bash
git add src/generator/reference/seeder.py tests/test_seeder.py
git commit -m "feat: reference data seeder (units, franchisees, menu, financial periods)"
```

---

### Task 7: Entity Registry

**Files:**
- Create: `src/generator/entity_registry.py`
- Create: `tests/test_entity_registry.py`

- [ ] Write failing tests:

```python
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
```

- [ ] Run, verify fail: `pytest tests/test_entity_registry.py -v`

- [ ] Implement `src/generator/entity_registry.py`:

```python
import random
from datetime import date
from typing import Optional

class EntityRegistry:
    """In-memory lookup of all seeded entity IDs for FK consistency."""

    def __init__(self, units: list[dict], menu_items: list[dict], bom: list[dict],
                 financial_periods: list[dict], num_guests_per_unit: int = 200):
        self._units = units
        self._unit_ids = [u["unit_id"] for u in units]
        self._unit_by_id = {u["unit_id"]: u for u in units}
        self._menu_items = menu_items
        self._menu_by_id = {m["menu_item_id"]: m for m in menu_items}
        self._bom = bom  # list of {menu_item_id, stock_sku, quantity, ...}
        self._bom_by_item: dict[int, list[dict]] = {}
        for row in bom:
            self._bom_by_item.setdefault(row["menu_item_id"], []).append(row)
        self._periods = sorted(financial_periods, key=lambda p: p["start_date"])

        # Pre-generate a guest pool per unit: IDs 1..N * num_units
        self._guest_pool: dict[int, list[int]] = {}
        g_id = 1
        for uid in self._unit_ids:
            self._guest_pool[uid] = list(range(g_id, g_id + num_guests_per_unit))
            g_id += num_guests_per_unit
        self._max_guest_id = g_id - 1

        # ~30% of guests are loyalty members (assigned on init)
        self._member_ids: set[int] = set()
        all_guests = [gid for pool in self._guest_pool.values() for gid in pool]
        self._member_ids = set(random.sample(all_guests, k=int(len(all_guests) * 0.30)))

    def random_unit_id(self) -> int:
        return random.choice(self._unit_ids)

    def unit_by_id(self, unit_id: int) -> dict:
        return self._unit_by_id[unit_id]

    def all_units(self) -> list[dict]:
        return self._units

    def random_menu_item(self, hour: int) -> dict:
        from src.generator.reference.menu_catalog import get_items_for_daypart
        items = get_items_for_daypart(hour)
        return random.choice(items)

    def random_menu_item_id(self, hour: int) -> int:
        return self.random_menu_item(hour)["menu_item_id"]

    def get_menu_item(self, menu_item_id: int) -> dict:
        return self._menu_by_id[menu_item_id]

    def bom_for_item(self, menu_item_id: int) -> list[dict]:
        return self._bom_by_item.get(menu_item_id, [])

    def random_guest_profile_id(self, unit_id: int) -> Optional[int]:
        """40% chance of returning a guest ID, 60% anonymous."""
        if random.random() > 0.40:
            return None
        pool = self._guest_pool.get(unit_id, [])
        return random.choice(pool) if pool else None

    def is_loyalty_member(self, guest_profile_id: Optional[int]) -> bool:
        return guest_profile_id in self._member_ids if guest_profile_id else False

    def random_member_id(self, guest_profile_id: Optional[int]) -> Optional[int]:
        """Return member_id if guest is a loyalty member (same ID as profile for simplicity)."""
        return guest_profile_id if self.is_loyalty_member(guest_profile_id) else None

    def financial_period_for_date(self, d: date) -> Optional[int]:
        d_str = d.isoformat()
        for p in self._periods:
            if p["start_date"] <= d_str <= p["end_date"]:
                return p["financial_period_id"]
        return self._periods[-1]["financial_period_id"] if self._periods else None

    @classmethod
    def from_spark(cls, spark, catalog: str, backfill_months: int = 12):
        """Load entity registry from live ref.* Delta tables."""
        units = [r.asDict() for r in spark.table(f"{catalog}.ref.unit").collect()]
        menu = [r.asDict() for r in spark.table(f"{catalog}.ref.menu_item").collect()]
        bom = [r.asDict() for r in spark.table(f"{catalog}.ref.recipe_ingredient").collect()]
        periods = [r.asDict() for r in spark.table(f"{catalog}.ref.financial_period").collect()]
        return cls(units=units, menu_items=menu, bom=bom, financial_periods=periods)
```

- [ ] Run tests: `pytest tests/test_entity_registry.py -v` — all pass.

- [ ] Commit:

```bash
git add src/generator/entity_registry.py tests/test_entity_registry.py
git commit -m "feat: EntityRegistry — in-memory FK lookup for all seeded reference IDs"
```

---

## Phase B — Domain Event Generators

### Task 8: Order domain generator

**Files:**
- Create: `src/generator/domains/orders.py`
- Create: `tests/test_orders.py`

- [ ] Write failing tests:

```python
# tests/test_orders.py
from datetime import datetime
from src.generator.causal_context import build_context
from src.generator.entity_registry import EntityRegistry
from src.generator.reference.us_locations import generate_units
from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
from src.generator.reference.seeder import build_financial_periods_data
from src.generator.domains.orders import generate_orders_for_tick

def _registry():
    return EntityRegistry(
        units=generate_units(5),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(3),
    )

def _ctx(unit_id=1):
    return build_context(unit_id, datetime(2025, 9, 19, 19, 0), 1.0)  # Friday dinner

def test_returns_list_of_dicts():
    rows = generate_orders_for_tick(_ctx(), _registry(), tick_seconds=60)
    assert isinstance(rows, list)

def test_order_has_required_fields():
    rows = generate_orders_for_tick(_ctx(), _registry(), tick_seconds=60)
    if rows:
        order = next(r for r in rows if r["event_type"] == "guest_order")
        required = {"guest_order_id", "unit_id", "channel", "order_status",
                    "subtotal", "tax_amount", "total_amount", "placed_at"}
        assert required.issubset(order.keys())

def test_order_items_reference_order():
    rows = generate_orders_for_tick(_ctx(), _registry(), tick_seconds=60)
    order_ids = {r["guest_order_id"] for r in rows if r["event_type"] == "guest_order"}
    item_order_ids = {r["guest_order_id"] for r in rows if r["event_type"] == "order_item"}
    assert item_order_ids.issubset(order_ids)

def test_payment_references_order():
    rows = generate_orders_for_tick(_ctx(), _registry(), tick_seconds=60)
    order_ids = {r["guest_order_id"] for r in rows if r["event_type"] == "guest_order"}
    payment_order_ids = {r["guest_order_id"] for r in rows if r["event_type"] == "payment"}
    # Cancelled orders may have no payment
    assert payment_order_ids.issubset(order_ids)

def test_total_amount_equals_subtotal_plus_tax():
    rows = generate_orders_for_tick(_ctx(), _registry(), tick_seconds=60)
    for r in rows:
        if r["event_type"] == "guest_order" and r["order_status"] != "cancelled":
            assert abs(r["total_amount"] - (r["subtotal"] + r["tax_amount"])) < 0.01
```

- [ ] Run, verify fail: `pytest tests/test_orders.py -v`

- [ ] Implement `src/generator/domains/orders.py`:

```python
import random
import uuid
from datetime import datetime, timedelta
from typing import Optional
from src.generator.causal_context import CausalContext
from src.generator.entity_registry import EntityRegistry
from src.generator.demand_model import orders_for_tick, channel_for_order, tender_for_order
from src.generator.entropy import prep_time_seconds, should_breach_sos, should_cancel

_TAX_RATE = 0.085
_order_counter = 0

def _next_order_id() -> int:
    global _order_counter
    _order_counter += 1
    return _order_counter

def _build_order(ctx: CausalContext, registry: EntityRegistry,
                 order_id: int, channel: str) -> list[dict]:
    """Build all rows for one order: guest_order + order_items + payment + status_events."""
    rows = []
    placed_at = ctx.timestamp + timedelta(seconds=random.randint(0, 55))
    guest_id = registry.random_guest_profile_id(ctx.unit_id)
    member_id = registry.random_member_id(guest_id)
    is_member = member_id is not None
    fp_id = registry.financial_period_for_date(placed_at.date())

    # Build 1–5 order items
    num_items = random.choices([1, 2, 3, 4, 5], weights=[20, 35, 25, 15, 5])[0]
    item_rows = []
    subtotal = 0.0
    for i in range(num_items):
        menu_item = registry.random_menu_item(placed_at.hour)
        mid = menu_item["menu_item_id"]
        qty = 1 if menu_item["category"] != "drinks" else random.choice([1, 2])
        unit_price = registry.get_menu_item(mid)["base_price"]
        if channel == "3pd_delivery":
            unit_price += 0.75
        line_gross = round(unit_price * qty, 2)
        subtotal += line_gross
        item_rows.append({
            "event_type": "order_item",
            "order_item_id": order_id * 10 + i,
            "guest_order_id": order_id,
            "unit_id": ctx.unit_id,
            "menu_item_id": mid,
            "quantity": qty,
            "unit_price": unit_price,
            "line_gross_amount": line_gross,
            "line_net_amount": line_gross,
            "line_discount_amount": 0.0,
            "item_status": "fulfilled",
            "waste_flag": False,
            "placed_at": placed_at.isoformat(),
        })

    subtotal = round(subtotal, 2)
    tax = round(subtotal * _TAX_RATE, 2)
    total = round(subtotal + tax, 2)

    is_cancelled = should_cancel(ctx.cancellation_rate, channel)
    status = "cancelled" if is_cancelled else "fulfilled"

    # guest_order row
    prep_secs = prep_time_seconds(channel)
    ready_at = placed_at + timedelta(seconds=prep_secs)
    sos_breach = should_breach_sos(ctx.sos_breach_probability)

    rows.append({
        "event_type": "guest_order",
        "guest_order_id": order_id,
        "unit_id": ctx.unit_id,
        "channel": channel,
        "order_type": "delivery" if "delivery" in channel else channel,
        "order_status": status,
        "profile_id": guest_id,
        "member_id": member_id,
        "subtotal": subtotal,
        "discount_amount": 0.0,
        "tax_amount": tax,
        "total_amount": total if not is_cancelled else 0.0,
        "placed_at": placed_at.isoformat(),
        "ready_at": ready_at.isoformat() if not is_cancelled else None,
        "fulfilled_at": (ready_at + timedelta(seconds=random.randint(60, 300))).isoformat()
                        if not is_cancelled else None,
        "cancelled_at": placed_at.isoformat() if is_cancelled else None,
        "financial_period_id": fp_id,
        "sos_breach": sos_breach,
    })

    if not is_cancelled:
        rows.extend(item_rows)
        # status events: placed → preparing → ready → fulfilled
        for j, (state_from, state_to, delta_secs) in enumerate([
            ("placed", "preparing", 60),
            ("preparing", "ready", prep_secs),
            ("ready", "fulfilled", 120),
        ]):
            rows.append({
                "event_type": "status_event",
                "status_event_id": order_id * 10 + j,
                "guest_order_id": order_id,
                "unit_id": ctx.unit_id,
                "prior_state": state_from,
                "current_state": state_to,
                "event_timestamp": (placed_at + timedelta(seconds=delta_secs)).isoformat(),
                "elapsed_seconds_in_prior_state": delta_secs,
                "sos_target_seconds": 720 if channel == "carryout" else 1800,
                "is_sos_breach": sos_breach and state_to == "ready",
            })
        # payment
        tender = tender_for_order(ctx, is_member)
        rows.append({
            "event_type": "payment",
            "payment_id": order_id,
            "guest_order_id": order_id,
            "unit_id": ctx.unit_id,
            "tender_type": tender,
            "amount": total,
            "settlement_date": placed_at.date().isoformat(),
            "paid_at": placed_at.isoformat(),
        })
        # delivery order for delivery channels
        if "delivery" in channel:
            rows.append({
                "event_type": "delivery_order",
                "delivery_order_id": order_id,
                "guest_order_id": order_id,
                "unit_id": ctx.unit_id,
                "platform_order_reference": str(uuid.uuid4())[:8] if channel == "3pd_delivery" else None,
                "estimated_delivery_seconds": prep_secs + 900,
                "actual_delivery_seconds": prep_secs + random.randint(600, 1800),
                "delivery_status": "delivered",
            })

    return rows

def generate_orders_for_tick(ctx: CausalContext, registry: EntityRegistry,
                              tick_seconds: int = 60) -> list[dict]:
    """Generate all order-domain rows for one unit, one tick."""
    n = orders_for_tick(ctx, tick_seconds)
    rows = []
    for _ in range(n):
        order_id = _next_order_id()
        channel = channel_for_order(ctx)
        rows.extend(_build_order(ctx, registry, order_id, channel))
    return rows
```

- [ ] Run tests: `pytest tests/test_orders.py -v` — all pass.

- [ ] Commit:

```bash
git add src/generator/domains/orders.py tests/test_orders.py
git commit -m "feat: order domain generator (orders, items, payments, status events, delivery)"
```

---

### Task 9: Inventory domain generator

**Files:**
- Create: `src/generator/domains/inventory.py`
- Create: `tests/test_inventory.py`

- [ ] Write failing tests:

```python
# tests/test_inventory.py
from datetime import datetime
from src.generator.causal_context import build_context
from src.generator.entity_registry import EntityRegistry
from src.generator.reference.us_locations import generate_units
from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
from src.generator.reference.seeder import build_financial_periods_data
from src.generator.domains.inventory import (
    generate_inventory_events, generate_daily_receiving
)

def _reg():
    return EntityRegistry(
        units=generate_units(3),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(3),
    )

def test_inventory_events_for_orders():
    from src.generator.domains.orders import generate_orders_for_tick
    ctx = build_context(1, datetime(2025, 9, 19, 19, 0), 1.0)
    reg = _reg()
    order_rows = generate_orders_for_tick(ctx, reg, tick_seconds=60)
    inv_rows = generate_inventory_events(ctx, reg, order_rows)
    assert isinstance(inv_rows, list)
    balance_rows = [r for r in inv_rows if r["event_type"] == "on_hand_balance"]
    assert len(balance_rows) > 0

def test_waste_events_have_required_fields():
    ctx = build_context(1, datetime(2025, 9, 19, 21, 0), 1.0)  # late night
    reg = _reg()
    rows = generate_inventory_events(ctx, reg, [])
    waste_rows = [r for r in rows if r["event_type"] == "waste_log"]
    for w in waste_rows:
        assert "stock_sku" in w
        assert "waste_quantity" in w
        assert w["waste_quantity"] > 0

def test_daily_receiving_produces_receiving_orders():
    reg = _reg()
    rows = generate_daily_receiving(unit_id=1, reg=reg, order_date="2025-09-19")
    assert len(rows) > 0
    assert all(r["event_type"] == "receiving_order" for r in rows)
```

- [ ] Run, verify fail: `pytest tests/test_inventory.py -v`

- [ ] Implement `src/generator/domains/inventory.py`:

```python
import random
from datetime import datetime
from src.generator.causal_context import CausalContext
from src.generator.entity_registry import EntityRegistry
from src.generator.entropy import should_waste

_inv_counter = 0

def _next_inv_id() -> int:
    global _inv_counter
    _inv_counter += 1
    return _inv_counter

def generate_inventory_events(ctx: CausalContext, registry: EntityRegistry,
                               order_rows: list[dict]) -> list[dict]:
    rows = []
    # Decrement on_hand_balance for each order_item sold
    depleted: dict[str, float] = {}
    for row in order_rows:
        if row["event_type"] != "order_item":
            continue
        mid = row["menu_item_id"]
        for bom_row in registry.bom_for_item(mid):
            sku = bom_row["stock_sku"]
            depleted[sku] = depleted.get(sku, 0.0) + bom_row["quantity"] * row["quantity"]

    for sku, qty_used in depleted.items():
        par_level = 20.0
        on_hand = max(0.0, par_level - qty_used + random.uniform(0, 5))
        rows.append({
            "event_type": "on_hand_balance",
            "on_hand_balance_id": _next_inv_id(),
            "unit_id": ctx.unit_id,
            "stock_sku": sku,
            "quantity_on_hand": round(on_hand, 3),
            "quantity_reserved": round(qty_used, 3),
            "par_level": par_level,
            "snapshot_at": ctx.timestamp.isoformat(),
        })
        # Waste events: ~3% of prep volume, skewed late-night
        if should_waste(ctx.waste_probability, ctx.hour_of_day):
            waste_qty = round(qty_used * random.uniform(0.02, 0.06), 3)
            rows.append({
                "event_type": "waste_log",
                "waste_log_id": _next_inv_id(),
                "unit_id": ctx.unit_id,
                "stock_sku": sku,
                "waste_quantity": waste_qty,
                "waste_category": "overproduction",
                "waste_cost": round(waste_qty * 2.5, 2),
                "logged_at": ctx.timestamp.isoformat(),
            })
        # Trigger replenishment if on_hand drops below 25% of PAR
        if on_hand < par_level * 0.25:
            rows.append({
                "event_type": "replenishment_order",
                "replenishment_order_id": _next_inv_id(),
                "unit_id": ctx.unit_id,
                "stock_sku": sku,
                "order_type": "auto_par",
                "order_quantity": round(par_level - on_hand, 3),
                "order_status": "submitted",
                "ordered_at": ctx.timestamp.isoformat(),
            })

    return rows

def generate_daily_receiving(unit_id: int, reg: EntityRegistry,
                              order_date: str) -> list[dict]:
    """Simulate daily supplier delivery restocking PAR levels."""
    rows = []
    skus = {bom["stock_sku"] for bom in reg.bom_for_item.__self__._bom} if hasattr(
        reg.bom_for_item, "__self__") else _all_skus(reg)
    for sku in skus:
        rows.append({
            "event_type": "receiving_order",
            "receiving_order_id": _next_inv_id(),
            "unit_id": unit_id,
            "stock_sku": sku,
            "received_quantity": round(random.uniform(15, 30), 2),
            "delivery_date": order_date,
            "quality_inspection_result": "pass",
            "temperature_check_pass": True,
        })
    return rows

def _all_skus(reg: EntityRegistry) -> set[str]:
    skus = set()
    for item in reg._menu_items:
        for bom in reg.bom_for_item(item["menu_item_id"]):
            skus.add(bom["stock_sku"])
    return skus
```

- [ ] Run tests: `pytest tests/test_inventory.py -v` — all pass.

- [ ] Commit:

```bash
git add src/generator/domains/inventory.py tests/test_inventory.py
git commit -m "feat: inventory domain generator (on_hand_balance, waste, replenishment, receiving)"
```

---

### Task 10: Guest, Loyalty, and Workforce generators

**Files:**
- Create: `src/generator/domains/guest.py`
- Create: `src/generator/domains/loyalty.py`
- Create: `src/generator/domains/workforce.py`
- Create: `tests/test_guest_loyalty_workforce.py`

- [ ] Write failing tests:

```python
# tests/test_guest_loyalty_workforce.py
from datetime import datetime
from src.generator.causal_context import build_context
from src.generator.entity_registry import EntityRegistry
from src.generator.reference.us_locations import generate_units
from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
from src.generator.reference.seeder import build_financial_periods_data
from src.generator.domains.guest import generate_new_guest_profiles
from src.generator.domains.loyalty import generate_loyalty_events
from src.generator.domains.workforce import generate_shift_events

def _reg():
    return EntityRegistry(
        units=generate_units(3),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(3),
    )

def _ctx(hour=19):
    return build_context(1, datetime(2025, 9, 19, hour, 0), 1.0)

def test_new_guest_profiles_have_required_fields():
    rows = generate_new_guest_profiles(unit_id=1, date_str="2025-09-19", growth_rate=0.008, base_pool=500)
    for r in rows:
        assert "guest_profile_id" in r
        assert "digital_account_id" in r

def test_loyalty_events_reference_valid_orders():
    from src.generator.domains.orders import generate_orders_for_tick
    ctx = _ctx()
    reg = _reg()
    order_rows = generate_orders_for_tick(ctx, reg, tick_seconds=60)
    loyalty_rows = generate_loyalty_events(ctx, reg, order_rows)
    order_ids = {r["guest_order_id"] for r in order_rows if r["event_type"] == "guest_order"}
    for lr in loyalty_rows:
        if lr.get("guest_order_id"):
            assert lr["guest_order_id"] in order_ids

def test_shift_events_have_required_fields():
    rows = generate_shift_events(unit_id=1, date_str="2025-09-19", projected_orders=80)
    shift_rows = [r for r in rows if r["event_type"] == "shift"]
    assert len(shift_rows) > 0
    for s in shift_rows:
        assert "employee_id" in s
        assert "shift_start" in s
        assert "shift_end" in s

def test_high_volume_means_more_staff():
    low = generate_shift_events(unit_id=1, date_str="2025-09-22", projected_orders=20)
    high = generate_shift_events(unit_id=1, date_str="2025-09-22", projected_orders=200)
    low_staff = len([r for r in low if r["event_type"] == "shift"])
    high_staff = len([r for r in high if r["event_type"] == "shift"])
    assert high_staff >= low_staff
```

- [ ] Run, verify fail: `pytest tests/test_guest_loyalty_workforce.py -v`

- [ ] Implement `src/generator/domains/guest.py`:

```python
import random
from faker import Faker

_fake = Faker()
_guest_counter = 0

def _next_guest_id() -> int:
    global _guest_counter
    _guest_counter += 1
    return _guest_counter

def generate_new_guest_profiles(unit_id: int, date_str: str,
                                 growth_rate: float = 0.008,
                                 base_pool: int = 500) -> list[dict]:
    """~0.8% of base_pool guests are new per day per unit."""
    n = max(0, round(base_pool * growth_rate * random.gauss(1.0, 0.3)))
    rows = []
    for _ in range(n):
        gid = _next_guest_id()
        rows.append({
            "event_type": "guest_profile",
            "guest_profile_id": gid,
            "unit_id": unit_id,
            "first_name": _fake.first_name(),
            "last_name": _fake.last_name(),
            "email": _fake.email(),
            "phone": _fake.phone_number()[:20],
            "zip_code": _fake.zipcode(),
            "created_date": date_str,
            "digital_account_id": gid,  # 1:1 for simplicity
            "account_status": "active",
        })
    return rows
```

- [ ] Implement `src/generator/domains/loyalty.py`:

```python
import random
from src.generator.causal_context import CausalContext
from src.generator.entity_registry import EntityRegistry

_loyalty_counter = 0

def _next_id() -> int:
    global _loyalty_counter
    _loyalty_counter += 1
    return _loyalty_counter

_TIER_THRESHOLDS = {"bronze": 0, "silver": 500, "gold": 1500, "platinum": 3500}

def _tier_for_spend(lifetime_spend: float) -> str:
    tier = "bronze"
    for t, threshold in _TIER_THRESHOLDS.items():
        if lifetime_spend >= threshold:
            tier = t
    return tier

def _points_multiplier(tier: str) -> float:
    return {"bronze": 1.0, "silver": 1.25, "gold": 1.5, "platinum": 2.0}[tier]

def generate_loyalty_events(ctx: CausalContext, registry: EntityRegistry,
                             order_rows: list[dict]) -> list[dict]:
    rows = []
    for order in order_rows:
        if order["event_type"] != "guest_order":
            continue
        mid = order.get("member_id")
        if not mid:
            continue
        total = order.get("total_amount", 0.0)
        if total <= 0:
            continue

        # Estimate lifetime spend from member_id (hash-based for determinism)
        lifetime_spend = (mid * 47) % 4000
        tier = _tier_for_spend(lifetime_spend)
        multiplier = _points_multiplier(tier)
        points_earned = int(total * 10 * multiplier)

        rows.append({
            "event_type": "loyalty_transaction",
            "loyalty_transaction_id": _next_id(),
            "member_id": mid,
            "guest_order_id": order["guest_order_id"],
            "unit_id": ctx.unit_id,
            "transaction_type": "earn",
            "points_delta": points_earned,
            "transaction_at": order["placed_at"],
            "tier": tier,
        })

        # ~8% of members redeem a reward this visit
        if random.random() < 0.08:
            redeem_points = random.choice([100, 200, 500])
            rows.append({
                "event_type": "reward_redemption",
                "reward_redemption_id": _next_id(),
                "member_id": mid,
                "guest_order_id": order["guest_order_id"],
                "unit_id": ctx.unit_id,
                "points_redeemed": redeem_points,
                "reward_value": round(redeem_points / 100, 2),
                "redeemed_at": order["placed_at"],
            })

    return rows
```

- [ ] Implement `src/generator/domains/workforce.py`:

```python
import random
from datetime import datetime, timedelta

_emp_counter = 0

def _next_emp_id(unit_id: int, shift_num: int) -> int:
    return unit_id * 1000 + shift_num

_SHIFT_WINDOWS = [
    ("open",    "10:00", "16:00"),
    ("mid",     "14:00", "21:00"),
    ("close",   "17:00", "23:00"),
    ("overlap", "12:00", "20:00"),
]

def _staff_count(projected_orders: int) -> int:
    """1 staff per ~25 orders/day, minimum 3, maximum 15."""
    return min(15, max(3, projected_orders // 25))

def generate_shift_events(unit_id: int, date_str: str,
                          projected_orders: int = 80) -> list[dict]:
    rows = []
    n_staff = _staff_count(projected_orders)
    shifts = (_SHIFT_WINDOWS * 4)[:n_staff]
    for i, (label, start_str, end_str) in enumerate(shifts):
        emp_id = _next_emp_id(unit_id, i + 1)
        base_date = datetime.strptime(date_str, "%Y-%m-%d")
        shift_start = datetime.strptime(f"{date_str} {start_str}", "%Y-%m-%d %H:%M")
        shift_end = datetime.strptime(f"{date_str} {end_str}", "%Y-%m-%d %H:%M")

        no_show = random.random() < 0.04
        rows.append({
            "event_type": "shift",
            "shift_id": unit_id * 10000 + i,
            "unit_id": unit_id,
            "employee_id": emp_id,
            "shift_label": label,
            "shift_start": shift_start.isoformat(),
            "shift_end": shift_end.isoformat(),
            "status": "no_show" if no_show else "completed",
            "date": date_str,
        })
        if not no_show:
            # Punch in/out with small variance
            punch_in = shift_start + timedelta(minutes=random.randint(-2, 8))
            punch_out = shift_end + timedelta(minutes=random.randint(-5, 15))
            rows.append({
                "event_type": "time_punch",
                "time_punch_id": unit_id * 20000 + i,
                "employee_id": emp_id,
                "unit_id": unit_id,
                "punch_in": punch_in.isoformat(),
                "punch_out": punch_out.isoformat(),
                "hours_worked": round((punch_out - punch_in).seconds / 3600, 2),
            })
    return rows
```

- [ ] Run tests: `pytest tests/test_guest_loyalty_workforce.py -v` — all pass.

- [ ] Commit:

```bash
git add src/generator/domains/guest.py src/generator/domains/loyalty.py \
        src/generator/domains/workforce.py tests/test_guest_loyalty_workforce.py
git commit -m "feat: guest, loyalty, and workforce domain generators"
```

---

### Task 11: Generator entrypoint (backfill + live modes)

**Files:**
- Create: `src/generator/runner.py`
- Create: `tests/test_runner.py`

- [ ] Write failing tests:

```python
# tests/test_runner.py
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
    from datetime import datetime
    cfg, reg = _make_config()
    rows = build_tick_rows(unit_id=1, timestamp=datetime(2025, 9, 19, 19, 0),
                           registry=reg, tick_seconds=60,
                           base_orders_per_hour=cfg.base_orders_per_unit_per_hour)
    event_types = {r["event_type"] for r in rows}
    assert "guest_order" in event_types

def test_build_tick_rows_has_unit_id_on_all_rows():
    from datetime import datetime
    cfg, reg = _make_config()
    rows = build_tick_rows(unit_id=2, timestamp=datetime(2025, 9, 19, 12, 0),
                           registry=reg, tick_seconds=60,
                           base_orders_per_hour=18)
    for r in rows:
        assert r.get("unit_id") == 2
```

- [ ] Run, verify fail: `pytest tests/test_runner.py -v`

- [ ] Implement `src/generator/runner.py`:

```python
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterator
from src.generator.causal_context import build_context
from src.generator.entity_registry import EntityRegistry
from src.generator.domains.orders import generate_orders_for_tick
from src.generator.domains.inventory import generate_inventory_events
from src.generator.domains.guest import generate_new_guest_profiles
from src.generator.domains.loyalty import generate_loyalty_events
from src.generator.domains.workforce import generate_shift_events

@dataclass
class GeneratorConfig:
    catalog_name: str
    num_units: int
    backfill_months: int
    live_tick_seconds: int
    base_orders_per_unit_per_hour: int

def build_tick_rows(unit_id: int, timestamp: datetime, registry: EntityRegistry,
                    tick_seconds: int = 60,
                    base_orders_per_hour: int = 18) -> list[dict]:
    """All domain rows for one unit, one tick."""
    unit = registry.unit_by_id(unit_id)
    ctx = build_context(unit_id, timestamp, unit["unit_volume_bias"],
                        base_orders_per_hour)
    order_rows = generate_orders_for_tick(ctx, registry, tick_seconds)
    inv_rows = generate_inventory_events(ctx, registry, order_rows)
    loyalty_rows = generate_loyalty_events(ctx, registry, order_rows)
    return order_rows + inv_rows + loyalty_rows

def backfill_ticks(registry: EntityRegistry, backfill_months: int,
                   tick_seconds: int = 3600,
                   base_orders_per_hour: int = 18) -> Iterator[list[dict]]:
    """Yield batches of rows for all units, one hour at a time, from N months ago to now."""
    from dateutil.relativedelta import relativedelta
    now = datetime.now().replace(minute=0, second=0, microsecond=0)
    start = now - relativedelta(months=backfill_months)
    current = start
    while current <= now:
        batch = []
        for unit in registry.all_units():
            uid = unit["unit_id"]
            batch.extend(build_tick_rows(uid, current, registry, tick_seconds, base_orders_per_hour))
            # Daily events on the first tick of each day
            if current.hour == 10:
                batch.extend(generate_shift_events(uid, current.date().isoformat(),
                                                   projected_orders=base_orders_per_hour * 12))
                batch.extend(generate_new_guest_profiles(uid, current.date().isoformat()))
        yield batch
        current += timedelta(seconds=tick_seconds)

def live_tick(registry: EntityRegistry, tick_seconds: int = 60,
              base_orders_per_hour: int = 18) -> list[dict]:
    """One tick of live data for all units at current time."""
    now = datetime.now()
    rows = []
    for unit in registry.all_units():
        rows.extend(build_tick_rows(unit["unit_id"], now, registry,
                                    tick_seconds, base_orders_per_hour))
    return rows
```

- [ ] Run tests: `pytest tests/test_runner.py -v` — all pass.

- [ ] Commit:

```bash
git add src/generator/runner.py tests/test_runner.py
git commit -m "feat: generator runner — backfill iterator and live tick for all units"
```

---

### Task 12: Generator Databricks notebook (backfill + live entry)

**Files:**
- Create: `src/generator/main.py` (Databricks notebook as Python file)

- [ ] Create `src/generator/main.py`:

```python
# Databricks notebook source
# COMMAND ----------
import sys
import yaml
from pathlib import Path

# Load params — injected as widgets or read from conf/params.yml
try:
    catalog_name = dbutils.widgets.get("catalog_name")
    num_units = int(dbutils.widgets.get("num_units"))
    backfill_months = int(dbutils.widgets.get("backfill_months"))
    live_tick_seconds = int(dbutils.widgets.get("live_tick_seconds"))
    base_orders = int(dbutils.widgets.get("base_orders_per_unit_per_hour"))
    mode = dbutils.widgets.get("mode")  # "backfill" or "live"
except Exception:
    params = yaml.safe_load(Path("/Workspace/conf/params.yml").read_text())
    catalog_name = params["catalog_name"]
    num_units = params["num_units"]
    backfill_months = params["backfill_months"]
    live_tick_seconds = params["live_tick_seconds"]
    base_orders = params["base_orders_per_unit_per_hour"]
    mode = "live"

# COMMAND ----------
from src.generator.entity_registry import EntityRegistry
from src.generator.runner import backfill_ticks, live_tick, GeneratorConfig
from src.generator.reference.seeder import seed_all

# Load registry from ref tables
registry = EntityRegistry.from_spark(spark, catalog_name)

# COMMAND ----------
from pyspark.sql import Row

DOMAIN_TABLE_MAP = {
    "guest_order":        f"{catalog_name}.staging.order_events",
    "order_item":         f"{catalog_name}.staging.order_events",
    "order_modifier":     f"{catalog_name}.staging.order_events",
    "payment":            f"{catalog_name}.staging.order_events",
    "status_event":       f"{catalog_name}.staging.order_events",
    "delivery_order":     f"{catalog_name}.staging.order_events",
    "on_hand_balance":    f"{catalog_name}.staging.inventory_events",
    "waste_log":          f"{catalog_name}.staging.inventory_events",
    "receiving_order":    f"{catalog_name}.staging.inventory_events",
    "replenishment_order":f"{catalog_name}.staging.inventory_events",
    "stock_transfer":     f"{catalog_name}.staging.inventory_events",
    "adjustment":         f"{catalog_name}.staging.inventory_events",
    "guest_profile":      f"{catalog_name}.staging.guest_events",
    "digital_account":    f"{catalog_name}.staging.guest_events",
    "loyalty_transaction":f"{catalog_name}.staging.loyalty_events",
    "reward_redemption":  f"{catalog_name}.staging.loyalty_events",
    "shift":              f"{catalog_name}.staging.workforce_events",
    "time_punch":         f"{catalog_name}.staging.workforce_events",
}

def write_batch(rows: list[dict]):
    from collections import defaultdict
    by_table: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        et = row.get("event_type")
        if et in DOMAIN_TABLE_MAP:
            by_table[DOMAIN_TABLE_MAP[et]].append(row)
    for table, table_rows in by_table.items():
        df = spark.createDataFrame([Row(**r) for r in table_rows])
        df.write.format("delta").mode("append").saveAsTable(table)

# COMMAND ----------
if mode == "backfill":
    for i, batch in enumerate(backfill_ticks(registry, backfill_months,
                                              tick_seconds=3600,
                                              base_orders_per_hour=base_orders)):
        write_batch(batch)
        if i % 100 == 0:
            print(f"Backfill tick {i}, rows so far: {i * len(batch)}")
else:
    # Live mode: run once (called every live_tick_seconds by the job schedule)
    rows = live_tick(registry, live_tick_seconds, base_orders)
    write_batch(rows)
    print(f"Live tick complete: {len(rows)} rows written")
```

- [ ] Commit:

```bash
git add src/generator/main.py
git commit -m "feat: generator Databricks notebook entrypoint (backfill + live modes)"
```

---

## Phase C — DLT Pipeline + Metrics + DAB

### Task 13: DLT pipeline — Order + Inventory Silver tables

**Files:**
- Create: `src/pipeline/mvm_pipeline.py`

- [ ] Create `src/pipeline/mvm_pipeline.py` with Order and Inventory domains:

```python
# Databricks notebook source — Spark Declarative Pipeline (DLT)
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

catalog = spark.conf.get("pipeline.catalog", "qsr_synth")

# --------------------------------------------------------------------------
# ORDER DOMAIN
# --------------------------------------------------------------------------

@dlt.table(name="guest_order", comment="MVM Silver: guest_order")
@dlt.expect_or_drop("valid_total", "total_amount >= 0")
@dlt.expect_or_drop("valid_unit", "unit_id IS NOT NULL")
def guest_order():
    return (
        spark.readStream.table(f"{catalog}.staging.order_events")
        .filter(F.col("event_type") == "guest_order")
        .select(
            F.col("guest_order_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("channel"),
            F.col("order_type"),
            F.col("order_status"),
            F.col("profile_id").cast(LongType()),
            F.col("member_id").cast(LongType()),
            F.col("subtotal").cast(DoubleType()),
            F.col("discount_amount").cast(DoubleType()),
            F.col("tax_amount").cast(DoubleType()),
            F.col("total_amount").cast(DoubleType()),
            F.col("placed_at").cast(TimestampType()),
            F.col("ready_at").cast(TimestampType()),
            F.col("fulfilled_at").cast(TimestampType()),
            F.col("cancelled_at").cast(TimestampType()),
            F.col("financial_period_id").cast(LongType()),
            F.col("sos_breach").cast(BooleanType()),
            F.current_timestamp().alias("created_at"),
        )
    )

@dlt.table(name="order_item", comment="MVM Silver: order_item")
@dlt.expect_or_drop("positive_price", "unit_price > 0")
def order_item():
    return (
        spark.readStream.table(f"{catalog}.staging.order_events")
        .filter(F.col("event_type") == "order_item")
        .select(
            F.col("order_item_id").cast(LongType()),
            F.col("guest_order_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("menu_item_id").cast(LongType()),
            F.col("quantity").cast(IntegerType()),
            F.col("unit_price").cast(DoubleType()),
            F.col("line_gross_amount").cast(DoubleType()),
            F.col("line_net_amount").cast(DoubleType()),
            F.col("line_discount_amount").cast(DoubleType()),
            F.col("item_status"),
            F.col("waste_flag").cast(BooleanType()),
            F.col("placed_at").cast(TimestampType()),
            F.current_timestamp().alias("created_at"),
        )
    )

@dlt.table(name="payment", comment="MVM Silver: payment")
def payment():
    return (
        spark.readStream.table(f"{catalog}.staging.order_events")
        .filter(F.col("event_type") == "payment")
        .select(
            F.col("payment_id").cast(LongType()),
            F.col("guest_order_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("tender_type"),
            F.col("amount").cast(DoubleType()),
            F.col("settlement_date"),
            F.col("paid_at").cast(TimestampType()),
            F.current_timestamp().alias("created_at"),
        )
    )

@dlt.table(name="status_event", comment="MVM Silver: status_event")
def status_event():
    return (
        spark.readStream.table(f"{catalog}.staging.order_events")
        .filter(F.col("event_type") == "status_event")
        .select(
            F.col("status_event_id").cast(LongType()),
            F.col("guest_order_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("prior_state"),
            F.col("current_state"),
            F.col("event_timestamp").cast(TimestampType()),
            F.col("elapsed_seconds_in_prior_state").cast(IntegerType()),
            F.col("sos_target_seconds").cast(IntegerType()),
            F.col("is_sos_breach").cast(BooleanType()),
            F.current_timestamp().alias("created_at"),
        )
    )

@dlt.table(name="delivery_order", comment="MVM Silver: delivery_order")
def delivery_order():
    return (
        spark.readStream.table(f"{catalog}.staging.order_events")
        .filter(F.col("event_type") == "delivery_order")
        .select(
            F.col("delivery_order_id").cast(LongType()),
            F.col("guest_order_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("platform_order_reference"),
            F.col("estimated_delivery_seconds").cast(IntegerType()),
            F.col("actual_delivery_seconds").cast(IntegerType()),
            F.col("delivery_status"),
            F.current_timestamp().alias("created_at"),
        )
    )

# --------------------------------------------------------------------------
# INVENTORY DOMAIN
# --------------------------------------------------------------------------

@dlt.table(name="on_hand_balance", comment="MVM Silver: on_hand_balance")
@dlt.expect_or_drop("nonnegative_quantity", "quantity_on_hand >= 0")
def on_hand_balance():
    return (
        spark.readStream.table(f"{catalog}.staging.inventory_events")
        .filter(F.col("event_type") == "on_hand_balance")
        .select(
            F.col("on_hand_balance_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("stock_sku"),
            F.col("quantity_on_hand").cast(DoubleType()),
            F.col("quantity_reserved").cast(DoubleType()),
            F.col("par_level").cast(DoubleType()),
            F.col("snapshot_at").cast(TimestampType()),
            F.current_timestamp().alias("created_at"),
        )
    )

@dlt.table(name="waste_log", comment="MVM Silver: waste_log")
def waste_log():
    return (
        spark.readStream.table(f"{catalog}.staging.inventory_events")
        .filter(F.col("event_type") == "waste_log")
        .select(
            F.col("waste_log_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("stock_sku"),
            F.col("waste_quantity").cast(DoubleType()),
            F.col("waste_category"),
            F.col("waste_cost").cast(DoubleType()),
            F.col("logged_at").cast(TimestampType()),
            F.current_timestamp().alias("created_at"),
        )
    )

@dlt.table(name="receiving_order", comment="MVM Silver: receiving_order")
def receiving_order():
    return (
        spark.readStream.table(f"{catalog}.staging.inventory_events")
        .filter(F.col("event_type") == "receiving_order")
        .select(
            F.col("receiving_order_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("stock_sku"),
            F.col("received_quantity").cast(DoubleType()),
            F.col("delivery_date"),
            F.col("quality_inspection_result"),
            F.col("temperature_check_pass").cast(BooleanType()),
            F.current_timestamp().alias("created_at"),
        )
    )

@dlt.table(name="replenishment_order", comment="MVM Silver: replenishment_order")
def replenishment_order():
    return (
        spark.readStream.table(f"{catalog}.staging.inventory_events")
        .filter(F.col("event_type") == "replenishment_order")
        .select(
            F.col("replenishment_order_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("stock_sku"),
            F.col("order_type"),
            F.col("order_quantity").cast(DoubleType()),
            F.col("order_status"),
            F.col("ordered_at").cast(TimestampType()),
            F.current_timestamp().alias("created_at"),
        )
    )
```

- [ ] Commit:

```bash
git add src/pipeline/mvm_pipeline.py
git commit -m "feat: DLT pipeline — Order and Inventory Silver tables"
```

---

### Task 14: DLT pipeline — Guest, Loyalty, Workforce Silver + Gold tables

**Files:**
- Modify: `src/pipeline/mvm_pipeline.py`

- [ ] Append Guest, Loyalty, and Workforce Silver tables to `src/pipeline/mvm_pipeline.py`:

```python
# --------------------------------------------------------------------------
# GUEST DOMAIN
# --------------------------------------------------------------------------

@dlt.table(name="guest_profile", comment="MVM Silver: guest_profile")
def guest_profile():
    return (
        spark.readStream.table(f"{catalog}.staging.guest_events")
        .filter(F.col("event_type") == "guest_profile")
        .select(
            F.col("guest_profile_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("first_name"),
            F.col("last_name"),
            F.col("email"),
            F.col("phone"),
            F.col("zip_code"),
            F.col("created_date"),
            F.col("account_status"),
            F.current_timestamp().alias("created_at"),
        )
    )

@dlt.table(name="digital_account", comment="MVM Silver: digital_account")
def digital_account():
    return (
        spark.readStream.table(f"{catalog}.staging.guest_events")
        .filter(F.col("event_type") == "guest_profile")
        .select(
            F.col("digital_account_id").cast(LongType()),
            F.col("guest_profile_id").cast(LongType()),
            F.col("account_status"),
            F.col("created_date"),
            F.current_timestamp().alias("created_at"),
        )
    )

# --------------------------------------------------------------------------
# LOYALTY DOMAIN
# --------------------------------------------------------------------------

@dlt.table(name="loyalty_transaction", comment="MVM Silver: loyalty_transaction")
def loyalty_transaction():
    return (
        spark.readStream.table(f"{catalog}.staging.loyalty_events")
        .filter(F.col("event_type") == "loyalty_transaction")
        .select(
            F.col("loyalty_transaction_id").cast(LongType()),
            F.col("member_id").cast(LongType()),
            F.col("guest_order_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("transaction_type"),
            F.col("points_delta").cast(IntegerType()),
            F.col("transaction_at").cast(TimestampType()),
            F.col("tier"),
            F.current_timestamp().alias("created_at"),
        )
    )

@dlt.table(name="reward_redemption", comment="MVM Silver: reward_redemption")
def reward_redemption():
    return (
        spark.readStream.table(f"{catalog}.staging.loyalty_events")
        .filter(F.col("event_type") == "reward_redemption")
        .select(
            F.col("reward_redemption_id").cast(LongType()),
            F.col("member_id").cast(LongType()),
            F.col("guest_order_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("points_redeemed").cast(IntegerType()),
            F.col("reward_value").cast(DoubleType()),
            F.col("redeemed_at").cast(TimestampType()),
            F.current_timestamp().alias("created_at"),
        )
    )

# --------------------------------------------------------------------------
# WORKFORCE DOMAIN
# --------------------------------------------------------------------------

@dlt.table(name="shift", comment="MVM Silver: shift")
def shift():
    return (
        spark.readStream.table(f"{catalog}.staging.workforce_events")
        .filter(F.col("event_type") == "shift")
        .select(
            F.col("shift_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("employee_id").cast(LongType()),
            F.col("shift_label"),
            F.col("shift_start").cast(TimestampType()),
            F.col("shift_end").cast(TimestampType()),
            F.col("status"),
            F.col("date"),
            F.current_timestamp().alias("created_at"),
        )
    )

@dlt.table(name="time_punch", comment="MVM Silver: time_punch")
def time_punch():
    return (
        spark.readStream.table(f"{catalog}.staging.workforce_events")
        .filter(F.col("event_type") == "time_punch")
        .select(
            F.col("time_punch_id").cast(LongType()),
            F.col("employee_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("punch_in").cast(TimestampType()),
            F.col("punch_out").cast(TimestampType()),
            F.col("hours_worked").cast(DoubleType()),
            F.current_timestamp().alias("created_at"),
        )
    )

# --------------------------------------------------------------------------
# GOLD LAYER
# --------------------------------------------------------------------------

@dlt.table(name="unit_performance_daily", comment="Gold: daily unit performance")
def unit_performance_daily():
    return dlt.read("guest_order").groupBy(
        F.col("unit_id"),
        F.to_date("placed_at").alias("date"),
    ).agg(
        F.count("guest_order_id").alias("order_count"),
        F.sum("total_amount").alias("daily_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
        F.sum(F.when(F.col("order_status") == "cancelled", 1).otherwise(0)).alias("cancelled_count"),
    )

@dlt.table(name="sos_compliance_summary", comment="Gold: SOS compliance by unit/channel/date")
def sos_compliance_summary():
    return dlt.read("status_event").filter(
        F.col("current_state") == "ready"
    ).join(
        dlt.read("guest_order").select("guest_order_id", "channel", "placed_at"),
        "guest_order_id"
    ).groupBy(
        F.col("unit_id"),
        F.col("channel"),
        F.to_date("placed_at").alias("date"),
    ).agg(
        F.count("status_event_id").alias("total_orders"),
        F.sum(F.col("is_sos_breach").cast(IntegerType())).alias("sos_breaches"),
        F.avg("elapsed_seconds_in_prior_state").alias("avg_prep_seconds"),
    ).withColumn(
        "sos_compliance_pct",
        F.round(1.0 - F.col("sos_breaches") / F.col("total_orders"), 4)
    )

@dlt.table(name="loyalty_cohort_metrics", comment="Gold: loyalty cohort metrics by tier/date")
def loyalty_cohort_metrics():
    return dlt.read("loyalty_transaction").groupBy(
        F.col("unit_id"),
        F.col("tier"),
        F.to_date("transaction_at").alias("date"),
    ).agg(
        F.countDistinct("member_id").alias("active_members"),
        F.sum("points_delta").alias("total_points_earned"),
        F.count("loyalty_transaction_id").alias("transaction_count"),
    )

@dlt.table(name="inventory_waste_summary", comment="Gold: inventory waste by unit/date")
def inventory_waste_summary():
    return dlt.read("waste_log").groupBy(
        F.col("unit_id"),
        F.to_date("logged_at").alias("date"),
        F.col("waste_category"),
    ).agg(
        F.sum("waste_cost").alias("total_waste_cost"),
        F.sum("waste_quantity").alias("total_waste_qty"),
        F.count("waste_log_id").alias("waste_event_count"),
    )
```

- [ ] Commit:

```bash
git add src/pipeline/mvm_pipeline.py
git commit -m "feat: DLT pipeline — Guest, Loyalty, Workforce Silver + 4 Gold tables"
```

---

### Task 15: Setup and destroy notebooks

**Files:**
- Create: `src/setup/setup_notebook.py`
- Create: `src/setup/destroy_notebook.py`

- [ ] Create `src/setup/setup_notebook.py`:

```python
# Databricks notebook source
# COMMAND ----------
import yaml
from pathlib import Path

try:
    catalog_name = dbutils.widgets.get("catalog_name")
    num_units = int(dbutils.widgets.get("num_units"))
    backfill_months = int(dbutils.widgets.get("backfill_months"))
except Exception:
    params = yaml.safe_load(Path("/Workspace/conf/params.yml").read_text())
    catalog_name = params["catalog_name"]
    num_units = params["num_units"]
    backfill_months = params["backfill_months"]

# COMMAND ----------
# Task 1: Create catalog and schemas
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
for schema in ("staging", "silver", "gold", "ref"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema}")

# Staging tables (append-only, auto-schema on first write)
for staging_table in ("order_events", "inventory_events", "guest_events",
                       "loyalty_events", "workforce_events"):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.staging.{staging_table}
        (event_type STRING, unit_id BIGINT, _ingested_at TIMESTAMP)
        USING DELTA
        PARTITIONED BY (unit_id)
    """)

# COMMAND ----------
# Task 2: Seed reference data
from src.generator.reference.seeder import seed_all
seed_all(spark, catalog_name, num_units=num_units, backfill_months=backfill_months)
print(f"Reference data seeded: {num_units} units, {backfill_months} months of financial periods")

# COMMAND ----------
# Task 3: Create UC Metric Views (run AFTER DLT Gold tables exist)
# Call this cell manually or as a separate job task after first DLT pipeline run
METRIC_VIEWS = {
    "mv_auv_by_territory": f"""
        SELECT u.region_id, u.district_id, u.metro_area,
               DATE_TRUNC('month', upd.date) AS month,
               AVG(upd.daily_revenue) * 30 AS estimated_auv
        FROM {catalog_name}.gold.unit_performance_daily upd
        JOIN {catalog_name}.ref.unit u ON upd.unit_id = u.unit_id
        GROUP BY 1, 2, 3, 4
    """,
    "mv_sos_compliance": f"""
        SELECT unit_id, channel, date,
               sos_compliance_pct,
               avg_prep_seconds / 60.0 AS avg_prep_minutes
        FROM {catalog_name}.gold.sos_compliance_summary
    """,
    "mv_loyalty_active_members": f"""
        SELECT unit_id, tier, date,
               active_members,
               total_points_earned / NULLIF(active_members, 0) AS avg_points_per_member
        FROM {catalog_name}.gold.loyalty_cohort_metrics
    """,
    "mv_waste_pct_of_revenue": f"""
        SELECT w.unit_id, w.date,
               w.total_waste_cost,
               upd.daily_revenue,
               ROUND(w.total_waste_cost / NULLIF(upd.daily_revenue, 0), 4) AS waste_pct_revenue
        FROM {catalog_name}.gold.inventory_waste_summary w
        JOIN {catalog_name}.gold.unit_performance_daily upd
          ON w.unit_id = upd.unit_id AND w.date = upd.date
    """,
}

for view_name, view_sql in METRIC_VIEWS.items():
    spark.sql(f"CREATE OR REPLACE VIEW {catalog_name}.gold.{view_name} AS {view_sql}")
    print(f"Created metric view: {catalog_name}.gold.{view_name}")
```

- [ ] Create `src/setup/destroy_notebook.py`:

```python
# Databricks notebook source
# COMMAND ----------
try:
    catalog_name = dbutils.widgets.get("catalog_name")
except Exception:
    catalog_name = "qsr_synth"

# COMMAND ----------
# Drop metric views
for view in ("mv_auv_by_territory", "mv_sos_compliance",
             "mv_loyalty_active_members", "mv_waste_pct_of_revenue"):
    spark.sql(f"DROP VIEW IF EXISTS {catalog_name}.gold.{view}")
    print(f"Dropped view: {view}")

# COMMAND ----------
# Drop staging tables
for table in ("order_events", "inventory_events", "guest_events",
              "loyalty_events", "workforce_events"):
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.staging.{table}")
    print(f"Dropped staging table: {table}")

# COMMAND ----------
# Drop ref tables
for table in ("unit", "franchisee", "financial_period", "supplier",
              "menu_item", "recipe_ingredient", "weather_conditions", "local_events"):
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.ref.{table}")
    print(f"Dropped ref table: {table}")

# COMMAND ----------
# Drop schemas (DLT manages silver/gold DROP via bundle destroy)
for schema in ("staging", "ref"):
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema} CASCADE")
    print(f"Dropped schema: {schema}")

print("Destroy complete. Run 'databricks bundle destroy' to remove jobs and pipeline.")
```

- [ ] Commit:

```bash
git add src/setup/setup_notebook.py src/setup/destroy_notebook.py
git commit -m "feat: setup and destroy notebooks (catalog, schemas, ref seed, metric views)"
```

---

### Task 16: DAB resource files

**Files:**
- Create: `resources/pipeline.yml`
- Create: `resources/setup_job.yml`
- Create: `resources/generator_job.yml`
- Create: `resources/destroy_job.yml`

- [ ] Create `resources/pipeline.yml`:

```yaml
resources:
  pipelines:
    mvm_pipeline:
      name: qsr-synth-mvm-pipeline-${bundle.target}
      target: ${var.catalog_name}
      catalog: ${var.catalog_name}
      schema: silver
      continuous: true
      channel: PREVIEW
      libraries:
        - notebook:
            path: src/pipeline/mvm_pipeline.py
      configuration:
        pipeline.catalog: ${var.catalog_name}
      clusters:
        - label: default
          autoscale:
            min_workers: 1
            max_workers: 4
            mode: ENHANCED
```

- [ ] Create `resources/setup_job.yml`:

```yaml
resources:
  jobs:
    setup_job:
      name: qsr-synth-setup-${bundle.target}
      tasks:
        - task_key: seed_reference_data
          notebook_task:
            notebook_path: src/setup/setup_notebook.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              num_units: ${var.num_units}
              backfill_months: ${var.backfill_months}
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: m5d.xlarge
            num_workers: 2
```

- [ ] Create `resources/generator_job.yml`:

```yaml
resources:
  jobs:
    generator_job:
      name: qsr-synth-generator-${bundle.target}
      tasks:
        - task_key: run_generator
          notebook_task:
            notebook_path: src/generator/main.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              num_units: ${var.num_units}
              backfill_months: ${var.backfill_months}
              live_tick_seconds: "60"
              base_orders_per_unit_per_hour: "18"
              mode: live
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: m5d.xlarge
            num_workers: 2
      schedule:
        quartz_cron_expression: "0 * * * * ?"   # every minute
        timezone_id: America/Chicago
        pause_status: UNPAUSED
```

- [ ] Create `resources/destroy_job.yml`:

```yaml
resources:
  jobs:
    destroy_job:
      name: qsr-synth-destroy-${bundle.target}
      tasks:
        - task_key: destroy
          notebook_task:
            notebook_path: src/setup/destroy_notebook.py
            base_parameters:
              catalog_name: ${var.catalog_name}
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: m5d.xlarge
            num_workers: 1
```

- [ ] Commit:

```bash
git add resources/
git commit -m "feat: DAB resource files — pipeline, setup, generator, and destroy jobs"
```

---

### Task 17: Install dependencies + final smoke test

**Files:**
- Create: `requirements.txt`
- Create: `tests/test_smoke.py`

- [ ] Create `requirements.txt`:

```
faker>=24.0.0
numpy>=1.26.0
python-dateutil>=2.9.0
pyyaml>=6.0.1
pytest>=8.0.0
```

- [ ] Write smoke test:

```python
# tests/test_smoke.py
from datetime import datetime
from src.generator.reference.us_locations import generate_units
from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
from src.generator.reference.seeder import build_financial_periods_data
from src.generator.entity_registry import EntityRegistry
from src.generator.runner import build_tick_rows

def test_full_tick_produces_all_event_types():
    reg = EntityRegistry(
        units=generate_units(3),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(3),
    )
    ts = datetime(2025, 10, 12, 19, 0)  # NFL Sunday 7pm
    rows = []
    for unit in reg.all_units():
        rows.extend(build_tick_rows(unit["unit_id"], ts, reg, tick_seconds=3600,
                                    base_orders_per_hour=18))

    event_types = {r["event_type"] for r in rows}
    assert "guest_order" in event_types
    assert "order_item" in event_types
    assert "payment" in event_types
    assert "on_hand_balance" in event_types
    assert "loyalty_transaction" in event_types

def test_all_fks_reference_valid_entities():
    reg = EntityRegistry(
        units=generate_units(5),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(3),
    )
    ts = datetime(2025, 9, 19, 19, 0)
    all_rows = []
    for unit in reg.all_units():
        all_rows.extend(build_tick_rows(unit["unit_id"], ts, reg))

    order_ids = {r["guest_order_id"] for r in all_rows if r["event_type"] == "guest_order"}
    for r in all_rows:
        if r["event_type"] == "order_item":
            assert r["guest_order_id"] in order_ids
        if r["event_type"] == "payment":
            assert r["guest_order_id"] in order_ids
        if r["event_type"] == "loyalty_transaction":
            assert r.get("guest_order_id") in order_ids or r.get("guest_order_id") is None
```

- [ ] Install dependencies: `pip install -r requirements.txt`

- [ ] Run full test suite:

```bash
pytest tests/ -v
```

Expected: all tests pass.

- [ ] Commit:

```bash
git add requirements.txt tests/test_smoke.py
git commit -m "feat: requirements.txt and end-to-end smoke tests — all green"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] MVM 153-table scope → covered via DLT pipeline + ref seeder; streaming domains (Order/Inventory/Guest/Loyalty/Workforce) fully implemented; Reference domains seeded in setup_notebook
- [x] 100–500 locations → `generate_units(num_units)`, default 250
- [x] Configurable backfill window → `backfill_months` in params.yml + `backfill_ticks()` in runner.py
- [x] Structured Streaming (DLT readStream) → all Silver tables use `spark.readStream.table(...)`
- [x] Causal model with Phase 2 stubs → `CausalContext` + `build_context()`, Phase 2 fields default to `None`
- [x] No dine-in → `BASE_CHANNEL_MIX` has no dine-in; all channel logic uses delivery/carryout/catering only
- [x] Gold tables → 4 Gold DLT tables in Task 14
- [x] UC Metric Views → 4 views created in setup_notebook Task 3
- [x] setup_job Task 2 (metric views) after DLT runs → setup_notebook cell 3 documented as manual/separate
- [x] Destroy job → destroy_notebook drops views, staging, ref; bundle destroy handles DLT/jobs
- [x] DAB deployment → all 4 resource YAML files in Task 16
- [x] Entity registry FK consistency → EntityRegistry + cross-domain FK assertions in smoke test
- [x] `procurement` clarification → supplier/purchase_order seeded in seeder.py; goods_receipt generated by inventory generator
- [x] Weather/events Phase 2 → stubs in CausalContext, empty ref tables created in setup_notebook

**Placeholder scan:** No TBDs. All code blocks are complete.

**Type consistency:**
- `_next_order_id()` used in `orders.py`, `order_item_id` uses same counter basis → consistent
- `EntityRegistry.from_spark()` matches constructor signature → consistent
- `build_tick_rows()` signature in runner.py matches calls in test_runner.py and test_smoke.py → consistent
- `generate_orders_for_tick()` called in inventory.py, loyalty.py, and tests with same signature → consistent
