# Weather & Local Events (Phase 3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Populate `ref.weather_conditions` and `ref.local_events` with real automated data from Open-Meteo (forecasts), NOAA NWS (alerts), Nager.Date (holidays), Ticketmaster, and SeatGeek (events), and wire the data into `CausalContext` so the demand model produces realistic weather- and event-driven order volume.

**Architecture:** A new daily DAB job (`refresh_weather_events_job`) fetches weather and events at metro granularity (20 metros × 14-day forward window) and MERGEs results into two ref tables. `CausalContext.build_context()` accepts an optional `(metro_area, date) → dict` lookup loaded once per generator run from those tables; when absent it falls back silently to multiplier=1.0. A new metric view `demand_risk_forecast` surfaces (unit, date, risk_level, drivers) for the next 14 days for Genie queries.

**Tech Stack:** Python `requests` (already in project), `PyYAML`, Open-Meteo API (no key), NOAA NWS alerts API (no key), Nager.Date API (no key), Ticketmaster Discovery API (apiKey — optional), SeatGeek API (apiKey — optional), Databricks Delta MERGE, DAB YAML.

---

## File Map

**New files:**
- `src/refresh/__init__.py` — empty package marker
- `src/refresh/openmeteo_client.py` — Open-Meteo forecast fetcher; injectable `_fetch` for tests
- `src/refresh/noaa_client.py` — NOAA NWS active alerts fetcher; injectable `_fetch` for tests
- `src/refresh/nager_client.py` — Nager.Date public holidays fetcher; injectable `_fetch` for tests
- `src/refresh/events_client.py` — Ticketmaster + SeatGeek events fetcher (optional, key-gated)
- `src/refresh/multiplier_engine.py` — maps weather conditions + alert levels + event categories to demand multipliers; reads `conf/weather_event_multipliers.yml`
- `src/refresh/refresh_notebook.py` — Databricks notebook: orchestrates all fetchers, writes via MERGE to ref tables
- `conf/weather_event_multipliers.yml` — auditable multiplier config with cited comments
- `resources/refresh_weather_events.yml` — DAB job definition, daily cron
- `tests/fixtures/openmeteo_forecast.json` — recorded Open-Meteo response for hermetic tests
- `tests/fixtures/noaa_alerts_ny.json` — recorded NOAA alerts response
- `tests/fixtures/nager_holidays.json` — recorded Nager.Date response
- `tests/fixtures/ticketmaster_events.json` — recorded Ticketmaster response
- `tests/test_refresh.py` — hermetic tests for all clients + multiplier engine

**Modified files:**
- `src/generator/reference/seeder.py` — replace stub schemas with real `weather_conditions` + `local_events` schemas (tables stay empty; refresh job populates them)
- `src/generator/causal_context.py` — `build_context()` gains optional `weather_event_data: dict | None` param; applies multipliers and populates stub fields
- `src/generator/runner.py` — `build_tick_rows()` and `backfill_ticks()` gain optional `weather_event_lookup: dict | None` param; look up `(metro_area, date)` and pass to `build_context()`
- `src/generator/main.py` — load lookup once at start of backfill/live run from ref tables; pass to `backfill_ticks()`
- `src/setup/create_metric_views.py` — add `demand_risk_forecast` view
- `resources/setup_job.yml` — add `initial_weather_refresh` task (depends on `setup`, blocks `backfill`)
- `databricks.yml` — add `weather_refresh_cron`, `ticketmaster_secret_scope`, `seatgeek_secret_scope` variables

---

## Task 1: Real Schemas for ref.weather_conditions and ref.local_events

**Files:**
- Modify: `src/generator/reference/seeder.py:89-95`
- Test: `tests/test_seeder.py`

- [ ] **Step 1: Write a failing test that verifies the new schema columns exist**

```python
# In tests/test_seeder.py — add at the end of the file:
def test_seed_all_weather_conditions_schema():
    """seed_all must create weather_conditions with the real schema, not the stub."""
    import types, textwrap

    calls = []

    class FakeSpark:
        def createDataFrame(self, data):
            return self
        def write(self):
            return self
        def sql(self, query):
            calls.append(query)
            return self
        # chained write methods
        format = lambda self, *a, **kw: self
        mode = lambda self, *a, **kw: self
        option = lambda self, *a, **kw: self
        saveAsTable = lambda self, *a, **kw: self

    fake = FakeSpark()
    fake.write = fake  # write is accessed as attribute, not method

    # We can't call seed_all without PySpark Row, so just verify the SQL strings directly.
    # Import the module and check the schema strings are not stubs.
    from src.generator.reference import seeder
    import inspect
    src = inspect.getsource(seeder.seed_all)
    assert "stub_id" not in src, "stub_id still present — replace stub schema with real schema"
    assert "metro_area" in src, "weather_conditions must have metro_area column"
    assert "forecast_date" in src, "weather_conditions must have forecast_date column"
    assert "demand_multiplier" in src, "weather_conditions must have demand_multiplier column"
    assert "event_id" in src, "local_events must have event_id column"
    assert "event_category" in src, "local_events must have event_category column"
```

- [ ] **Step 2: Run the test to confirm it fails**

```bash
pytest tests/test_seeder.py::test_seed_all_weather_conditions_schema -v
```
Expected: FAIL — `AssertionError: stub_id still present`

- [ ] **Step 3: Replace the stub section in seeder.py**

In `src/generator/reference/seeder.py`, replace lines 89–95:

```python
    # Phase 2 stubs — empty tables
    for stub_table in ("weather_conditions", "local_events"):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{schema_prefix}ref.{stub_table}
            (stub_id BIGINT, placeholder STRING)
            USING DELTA
        """)
```

With:

```python
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema_prefix}ref.weather_conditions (
            metro_area             STRING,
            forecast_date          DATE,
            observation_type       STRING,
            high_temp_f            DOUBLE,
            low_temp_f             DOUBLE,
            precipitation_inches   DOUBLE,
            weather_condition      STRING,
            alert_level            STRING,
            demand_multiplier      DOUBLE,
            channel_shift_delivery DOUBLE,
            refreshed_at           TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema_prefix}ref.local_events (
            metro_area            STRING,
            event_date            DATE,
            event_id              STRING,
            event_name            STRING,
            event_category        STRING,
            venue                 STRING,
            est_attendance        INT,
            est_demand_multiplier DOUBLE,
            source                STRING,
            refreshed_at          TIMESTAMP
        ) USING DELTA
    """)
```

- [ ] **Step 4: Run the test to confirm it passes**

```bash
pytest tests/test_seeder.py::test_seed_all_weather_conditions_schema -v
```
Expected: PASS

- [ ] **Step 5: Run the full suite to check no regressions**

```bash
pytest tests/ -q
```
Expected: all 75 pass

- [ ] **Step 6: Commit**

```bash
git add src/generator/reference/seeder.py tests/test_seeder.py
git commit -m "feat(seeder): real schemas for ref.weather_conditions and ref.local_events"
```

---

## Task 2: Open-Meteo Weather Client

Open-Meteo returns daily weather for a lat/lon: WMO weather code, max/min temperature (°F), precipitation (inches). We request 30 past days + 14 forecast days in one call. WMO codes are mapped to a small condition vocabulary; extreme heat (>100°F) and extreme cold (<15°F) override the WMO-derived condition.

**Files:**
- Create: `src/refresh/__init__.py`
- Create: `src/refresh/openmeteo_client.py`
- Create: `tests/fixtures/openmeteo_forecast.json`
- Create: `tests/test_refresh.py`

- [ ] **Step 1: Create the package marker**

```bash
touch src/refresh/__init__.py
```

- [ ] **Step 2: Create the fixture file**

Create `tests/fixtures/openmeteo_forecast.json`:

```json
{
  "latitude": 40.71,
  "longitude": -74.01,
  "generationtime_ms": 1.23,
  "utc_offset_seconds": -18000,
  "timezone": "America/New_York",
  "daily_units": {
    "time": "iso8601",
    "weather_code": "wmo code",
    "temperature_2m_max": "°F",
    "temperature_2m_min": "°F",
    "precipitation_sum": "inch"
  },
  "daily": {
    "time": ["2026-05-21", "2026-05-22", "2026-05-23"],
    "weather_code": [3, 61, 95],
    "temperature_2m_max": [72.1, 65.4, 58.2],
    "temperature_2m_min": [55.3, 52.1, 48.6],
    "precipitation_sum": [0.0, 0.45, 1.2]
  }
}
```

- [ ] **Step 3: Write the failing tests**

Create `tests/test_refresh.py`:

```python
import json
from pathlib import Path
from unittest.mock import MagicMock

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Open-Meteo client
# ---------------------------------------------------------------------------

def _mock_get(fixture_file):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = json.loads((FIXTURES / fixture_file).read_text())
    resp.raise_for_status = lambda: None
    return lambda url, **kw: resp


def test_openmeteo_returns_rows_for_each_date():
    from src.refresh.openmeteo_client import fetch_metro_weather
    rows = fetch_metro_weather(
        "New York-Newark", 40.71, -74.01,
        _fetch=_mock_get("openmeteo_forecast.json"),
    )
    assert len(rows) == 3
    assert all(r["metro_area"] == "New York-Newark" for r in rows)
    assert all("forecast_date" in r for r in rows)
    assert all("weather_condition" in r for r in rows)


def test_openmeteo_wmo_mapping():
    from src.refresh.openmeteo_client import fetch_metro_weather
    rows = fetch_metro_weather(
        "New York-Newark", 40.71, -74.01,
        _fetch=_mock_get("openmeteo_forecast.json"),
    )
    # WMO 3 → clear, WMO 61 → rain, WMO 95 → storm
    conditions = [r["weather_condition"] for r in rows]
    assert conditions[0] == "clear"
    assert conditions[1] == "rain"
    assert conditions[2] == "storm"


def test_openmeteo_extreme_heat_override():
    from src.refresh.openmeteo_client import fetch_metro_weather
    data = {
        "daily": {
            "time": ["2026-07-04"],
            "weather_code": [0],
            "temperature_2m_max": [105.0],
            "temperature_2m_min": [88.0],
            "precipitation_sum": [0.0],
        }
    }
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = data
    resp.raise_for_status = lambda: None
    rows = fetch_metro_weather("Phoenix", 33.45, -112.07, _fetch=lambda url, **kw: resp)
    assert rows[0]["weather_condition"] == "extreme_heat"


def test_openmeteo_observation_type():
    from src.refresh.openmeteo_client import fetch_metro_weather
    rows = fetch_metro_weather(
        "New York-Newark", 40.71, -74.01,
        _fetch=_mock_get("openmeteo_forecast.json"),
    )
    # fixture has past/present/future dates — just verify field exists and valid values
    valid = {"historical", "forecast"}
    assert all(r["observation_type"] in valid for r in rows)
```

- [ ] **Step 4: Run the tests to confirm they fail**

```bash
pytest tests/test_refresh.py -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'src.refresh.openmeteo_client'`

- [ ] **Step 5: Implement openmeteo_client.py**

Create `src/refresh/openmeteo_client.py`:

```python
from datetime import datetime


def _wmo_to_condition(code: int) -> str:
    if code <= 3 or code in (45, 48):
        return "clear"
    if 51 <= code <= 57:
        return "rain"
    if 61 <= code <= 63:
        return "rain"
    if 64 <= code <= 67:
        return "heavy_rain"
    if 71 <= code <= 73:
        return "snow"
    if 74 <= code <= 77:
        return "heavy_snow"
    if 80 <= code <= 82:
        return "rain"
    if 83 <= code <= 84:
        return "heavy_rain"
    if 85 <= code <= 86:
        return "heavy_snow"
    if 95 <= code <= 99:
        return "storm"
    return "clear"


def fetch_metro_weather(
    metro_name: str,
    lat: float,
    lon: float,
    _fetch=None,
) -> list[dict]:
    """Fetch 30-day historical + 14-day forecast weather for a metro from Open-Meteo.

    Returns list of dicts matching ref.weather_conditions schema (alert_level=None;
    filled by NOAA pass). _fetch is injectable for hermetic tests.
    """
    import requests

    if _fetch is None:
        _fetch = requests.get

    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&daily=weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum"
        "&temperature_unit=fahrenheit&precipitation_unit=inch"
        "&forecast_days=14&past_days=30&timezone=auto"
    )
    resp = _fetch(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    today = datetime.now().date()
    rows = []
    daily = data["daily"]

    for i, date_str in enumerate(daily["time"]):
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
        wmo = daily["weather_code"][i]
        high_f = daily["temperature_2m_max"][i] or 0.0
        low_f = daily["temperature_2m_min"][i] or 0.0
        precip = daily["precipitation_sum"][i] or 0.0

        condition = _wmo_to_condition(wmo)
        if high_f > 100:
            condition = "extreme_heat"
        elif low_f < 15:
            condition = "extreme_cold"

        rows.append({
            "metro_area": metro_name,
            "forecast_date": date_str,
            "observation_type": "historical" if d < today else "forecast",
            "high_temp_f": high_f,
            "low_temp_f": low_f,
            "precipitation_inches": precip,
            "weather_condition": condition,
            "alert_level": None,
        })

    return rows
```

- [ ] **Step 6: Run the tests to confirm they pass**

```bash
pytest tests/test_refresh.py::test_openmeteo_returns_rows_for_each_date tests/test_refresh.py::test_openmeteo_wmo_mapping tests/test_refresh.py::test_openmeteo_extreme_heat_override tests/test_refresh.py::test_openmeteo_observation_type -v
```
Expected: 4 PASS

- [ ] **Step 7: Commit**

```bash
git add src/refresh/__init__.py src/refresh/openmeteo_client.py tests/fixtures/openmeteo_forecast.json tests/test_refresh.py
git commit -m "feat(refresh): Open-Meteo weather client with WMO-to-condition mapping"
```

---

## Task 3: NOAA NWS Alerts Client

NOAA NWS `alerts/active` returns GeoJSON features with an `event` string like `"Heat Advisory"`, `"Winter Storm Warning"`, `"Tornado Watch"`. We classify by substring: `"Warning"` → `warning`, `"Watch"` → `watch`, `"Advisory"` → `advisory`. The refresh notebook applies these to any weather row whose `forecast_date` falls within the alert's onset–expires window.

**Files:**
- Create: `src/refresh/noaa_client.py`
- Create: `tests/fixtures/noaa_alerts_ny.json`
- Modify: `tests/test_refresh.py`

- [ ] **Step 1: Create the fixture file**

Create `tests/fixtures/noaa_alerts_ny.json`:

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "event": "Heat Advisory",
        "severity": "Moderate",
        "urgency": "Expected",
        "onset": "2026-05-23T14:00:00-04:00",
        "expires": "2026-05-23T20:00:00-04:00",
        "description": "Heat index values up to 100 degrees."
      }
    },
    {
      "type": "Feature",
      "properties": {
        "event": "Rip Current Statement",
        "severity": "Unknown",
        "urgency": "Future",
        "onset": "2026-05-24T06:00:00-04:00",
        "expires": "2026-05-24T20:00:00-04:00",
        "description": "Dangerous rip currents."
      }
    }
  ]
}
```

- [ ] **Step 2: Add failing tests**

Add to `tests/test_refresh.py`:

```python
# ---------------------------------------------------------------------------
# NOAA alerts client
# ---------------------------------------------------------------------------

def test_noaa_returns_only_classifiable_alerts():
    from src.refresh.noaa_client import fetch_state_alerts
    rows = fetch_state_alerts("NY", _fetch=_mock_get("noaa_alerts_ny.json"))
    # "Heat Advisory" → advisory; "Rip Current Statement" → None (filtered out)
    assert len(rows) == 1
    assert rows[0]["alert_level"] == "advisory"
    assert rows[0]["event"] == "Heat Advisory"


def test_noaa_classifies_warning():
    from src.refresh.noaa_client import _classify_alert
    assert _classify_alert("Winter Storm Warning") == "warning"


def test_noaa_classifies_watch():
    from src.refresh.noaa_client import _classify_alert
    assert _classify_alert("Tornado Watch") == "watch"


def test_noaa_classifies_advisory():
    from src.refresh.noaa_client import _classify_alert
    assert _classify_alert("Heat Advisory") == "advisory"


def test_noaa_returns_empty_on_non_200():
    from src.refresh.noaa_client import fetch_state_alerts
    resp = MagicMock()
    resp.status_code = 503
    rows = fetch_state_alerts("TX", _fetch=lambda url, **kw: resp)
    assert rows == []
```

- [ ] **Step 3: Run to confirm failures**

```bash
pytest tests/test_refresh.py::test_noaa_returns_only_classifiable_alerts tests/test_refresh.py::test_noaa_classifies_warning -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'src.refresh.noaa_client'`

- [ ] **Step 4: Implement noaa_client.py**

Create `src/refresh/noaa_client.py`:

```python
def _classify_alert(event: str) -> str | None:
    """Returns 'warning', 'watch', or 'advisory', or None if not classifiable."""
    e = event.lower()
    if "warning" in e:
        return "warning"
    if "watch" in e:
        return "watch"
    if "advisory" in e:
        return "advisory"
    return None


def fetch_state_alerts(state: str, _fetch=None) -> list[dict]:
    """Fetch active NWS alerts for a US state.

    Returns list of {event, alert_level, onset, expires} for classifiable alerts only.
    Returns [] on any HTTP error (best-effort — stale data is fine).
    _fetch is injectable for hermetic tests.
    """
    import requests

    if _fetch is None:
        _fetch = requests.get

    url = f"https://api.weather.gov/alerts/active?area={state}&status=actual&message_type=alert"
    headers = {
        "User-Agent": "qsr-synth-data/1.0 (databricks-demo)",
        "Accept": "application/geo+json",
    }
    resp = _fetch(url, headers=headers, timeout=30)
    if resp.status_code != 200:
        return []

    alerts = []
    for feature in resp.json().get("features", []):
        props = feature.get("properties", {})
        event = props.get("event", "")
        alert_level = _classify_alert(event)
        if alert_level:
            alerts.append({
                "event": event,
                "alert_level": alert_level,
                "onset": props.get("onset", ""),
                "expires": props.get("expires", ""),
            })
    return alerts
```

- [ ] **Step 5: Run the tests to confirm they pass**

```bash
pytest tests/test_refresh.py -k "noaa" -v
```
Expected: 5 PASS

- [ ] **Step 6: Commit**

```bash
git add src/refresh/noaa_client.py tests/fixtures/noaa_alerts_ny.json tests/test_refresh.py
git commit -m "feat(refresh): NOAA NWS alerts client"
```

---

## Task 4: Nager.Date Holidays Client

Nager.Date returns US federal and state-level public holidays by year. `counties: null` means national; `counties: ["US-TX"]` means Texas-only. We convert each holiday into a `local_events` row with `event_category = "national_holiday"` or `"civic_holiday"` and a stable `event_id` derived from a hash of `(source, date, name)`.

**Files:**
- Create: `src/refresh/nager_client.py`
- Create: `tests/fixtures/nager_holidays.json`
- Modify: `tests/test_refresh.py`

- [ ] **Step 1: Create the fixture**

Create `tests/fixtures/nager_holidays.json`:

```json
[
  {
    "date": "2026-07-04",
    "localName": "Independence Day",
    "name": "Independence Day",
    "countryCode": "US",
    "fixed": true,
    "global": true,
    "counties": null,
    "types": ["Public"]
  },
  {
    "date": "2026-11-26",
    "localName": "Thanksgiving Day",
    "name": "Thanksgiving Day",
    "countryCode": "US",
    "fixed": false,
    "global": true,
    "counties": null,
    "types": ["Public"]
  },
  {
    "date": "2026-06-19",
    "localName": "Juneteenth National Independence Day",
    "name": "Juneteenth",
    "countryCode": "US",
    "fixed": true,
    "global": true,
    "counties": null,
    "types": ["Public"]
  }
]
```

- [ ] **Step 2: Add failing tests**

Add to `tests/test_refresh.py`:

```python
# ---------------------------------------------------------------------------
# Nager.Date holidays client
# ---------------------------------------------------------------------------

def test_nager_returns_holiday_rows():
    from src.refresh.nager_client import fetch_us_holidays
    rows = fetch_us_holidays(2026, "NY", _fetch=_mock_get("nager_holidays.json"))
    assert len(rows) == 3
    assert all(r["source"] == "nager" for r in rows)
    assert all(r["event_category"] == "national_holiday" for r in rows)


def test_nager_event_id_is_stable():
    from src.refresh.nager_client import fetch_us_holidays
    rows1 = fetch_us_holidays(2026, "NY", _fetch=_mock_get("nager_holidays.json"))
    rows2 = fetch_us_holidays(2026, "TX", _fetch=_mock_get("nager_holidays.json"))
    # Same holiday same date → same event_id regardless of state
    ids1 = {r["event_id"] for r in rows1}
    ids2 = {r["event_id"] for r in rows2}
    assert ids1 == ids2


def test_nager_returns_empty_on_non_200():
    from src.refresh.nager_client import fetch_us_holidays
    resp = MagicMock()
    resp.status_code = 404
    rows = fetch_us_holidays(2026, "NY", _fetch=lambda url, **kw: resp)
    assert rows == []
```

- [ ] **Step 3: Run to confirm failures**

```bash
pytest tests/test_refresh.py -k "nager" -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 4: Implement nager_client.py**

Create `src/refresh/nager_client.py`:

```python
import hashlib


def _make_event_id(source: str, date_str: str, name: str) -> str:
    raw = f"{source}:{date_str}:{name}".encode()
    return hashlib.sha256(raw).hexdigest()[:16]


def fetch_us_holidays(year: int, state: str, _fetch=None) -> list[dict]:
    """Fetch US public holidays for a year from Nager.Date.

    Filters to holidays applicable to `state` (national + state-specific).
    Returns list of dicts matching ref.local_events schema subset.
    _fetch is injectable for hermetic tests.
    """
    import requests

    if _fetch is None:
        _fetch = requests.get

    url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/US"
    resp = _fetch(url, timeout=30)
    if resp.status_code != 200:
        return []

    state_code = f"US-{state}"
    rows = []
    for h in resp.json():
        counties = h.get("counties")
        if counties is not None and state_code not in counties:
            continue
        name = h["localName"]
        date_str = h["date"]
        rows.append({
            "event_date": date_str,
            "event_id": _make_event_id("nager", date_str, name),
            "event_name": name,
            "event_category": "national_holiday",
            "venue": "",
            "est_attendance": 0,
            "source": "nager",
        })
    return rows
```

- [ ] **Step 5: Run the tests to confirm they pass**

```bash
pytest tests/test_refresh.py -k "nager" -v
```
Expected: 3 PASS

- [ ] **Step 6: Commit**

```bash
git add src/refresh/nager_client.py tests/fixtures/nager_holidays.json tests/test_refresh.py
git commit -m "feat(refresh): Nager.Date public holidays client"
```

---

## Task 5: Ticketmaster + SeatGeek Events Client (Optional)

Both APIs require a key stored as a Databricks secret. The client skips gracefully if no key is found. Ticketmaster uses `stateCode` + `classificationName`; SeatGeek uses `venue.state` + `type`. Results are merged and deduplicated by `event_id` (stable hash of source+metro+date+name).

**Files:**
- Create: `src/refresh/events_client.py`
- Create: `tests/fixtures/ticketmaster_events.json`
- Create: `tests/fixtures/seatgeek_events.json`
- Modify: `tests/test_refresh.py`

- [ ] **Step 1: Create fixtures**

Create `tests/fixtures/ticketmaster_events.json`:

```json
{
  "_embedded": {
    "events": [
      {
        "name": "New York Giants vs Dallas Cowboys",
        "dates": {"start": {"localDate": "2026-09-13"}},
        "classifications": [{"segment": {"name": "Sports"}}],
        "_embedded": {
          "venues": [{"name": "MetLife Stadium", "upcomingEvents": {"_total": 8}}]
        }
      },
      {
        "name": "Taylor Swift | The Eras Tour",
        "dates": {"start": {"localDate": "2026-08-01"}},
        "classifications": [{"segment": {"name": "Music"}}],
        "_embedded": {
          "venues": [{"name": "Madison Square Garden", "upcomingEvents": {"_total": 12}}]
        }
      }
    ]
  }
}
```

Create `tests/fixtures/seatgeek_events.json`:

```json
{
  "events": [
    {
      "title": "New York Knicks vs Boston Celtics",
      "datetime_local": "2026-10-15T19:30:00",
      "type": "nba",
      "venue": {"name": "Madison Square Garden", "state": "NY"},
      "stats": {"average_price": 180}
    }
  ]
}
```

- [ ] **Step 2: Add failing tests**

Add to `tests/test_refresh.py`:

```python
# ---------------------------------------------------------------------------
# Events client (Ticketmaster + SeatGeek)
# ---------------------------------------------------------------------------

def _mock_tm_get(fixture_file):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = json.loads((FIXTURES / fixture_file).read_text())
    return lambda url, params=None, **kw: resp


def test_ticketmaster_returns_events():
    from src.refresh.events_client import fetch_ticketmaster_events
    rows = fetch_ticketmaster_events(
        "New York-Newark", "NY",
        start_date="2026-08-01", end_date="2026-09-30",
        api_key="test_key",
        _fetch=_mock_tm_get("ticketmaster_events.json"),
    )
    assert len(rows) == 2
    assert rows[0]["event_category"] == "major_sports"
    assert rows[1]["event_category"] == "concert"
    assert all(r["source"] == "ticketmaster" for r in rows)
    assert all(r["metro_area"] == "New York-Newark" for r in rows)


def test_ticketmaster_event_id_is_stable():
    from src.refresh.events_client import fetch_ticketmaster_events
    rows1 = fetch_ticketmaster_events(
        "New York-Newark", "NY",
        start_date="2026-08-01", end_date="2026-09-30",
        api_key="test_key",
        _fetch=_mock_tm_get("ticketmaster_events.json"),
    )
    rows2 = fetch_ticketmaster_events(
        "New York-Newark", "NY",
        start_date="2026-08-01", end_date="2026-09-30",
        api_key="test_key",
        _fetch=_mock_tm_get("ticketmaster_events.json"),
    )
    assert rows1[0]["event_id"] == rows2[0]["event_id"]


def test_seatgeek_returns_events():
    from src.refresh.events_client import fetch_seatgeek_events
    rows = fetch_seatgeek_events(
        "New York-Newark", "NY",
        start_date="2026-10-01", end_date="2026-10-31",
        api_key="test_key",
        _fetch=_mock_tm_get("seatgeek_events.json"),
    )
    assert len(rows) == 1
    assert rows[0]["source"] == "seatgeek"
    assert rows[0]["event_category"] == "major_sports"


def test_events_client_returns_empty_on_error():
    from src.refresh.events_client import fetch_ticketmaster_events
    resp = MagicMock()
    resp.status_code = 401
    rows = fetch_ticketmaster_events(
        "New York-Newark", "NY",
        start_date="2026-08-01", end_date="2026-09-30",
        api_key="bad_key",
        _fetch=lambda url, **kw: resp,
    )
    assert rows == []
```

- [ ] **Step 3: Run to confirm failures**

```bash
pytest tests/test_refresh.py -k "ticketmaster or seatgeek" -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 4: Implement events_client.py**

Create `src/refresh/events_client.py`:

```python
import hashlib


def _make_event_id(source: str, metro: str, date_str: str, name: str) -> str:
    raw = f"{source}:{metro}:{date_str}:{name}".encode()
    return hashlib.sha256(raw).hexdigest()[:16]


def fetch_ticketmaster_events(
    metro_name: str,
    state: str,
    start_date: str,
    end_date: str,
    api_key: str,
    _fetch=None,
) -> list[dict]:
    """Fetch major sports + music events from Ticketmaster for a state/date window.

    Returns [] on any HTTP error. _fetch is injectable for hermetic tests.
    """
    import requests

    if _fetch is None:
        _fetch = requests.get

    params = {
        "apikey": api_key,
        "stateCode": state,
        "classificationName": "sports,music",
        "startDateTime": f"{start_date}T00:00:00Z",
        "endDateTime": f"{end_date}T23:59:59Z",
        "size": 200,
        "sort": "relevance,desc",
    }
    resp = _fetch(
        "https://app.ticketmaster.com/discovery/v2/events.json",
        params=params,
        timeout=30,
    )
    if resp.status_code != 200:
        return []

    rows = []
    for event in resp.json().get("_embedded", {}).get("events", []):
        segment = (
            event.get("classifications", [{}])[0].get("segment", {}).get("name", "Other")
        )
        category = "major_sports" if segment == "Sports" else "concert"
        date_str = event.get("dates", {}).get("start", {}).get("localDate", "")
        venues = event.get("_embedded", {}).get("venues", [{}])
        venue_name = venues[0].get("name", "") if venues else ""
        attendance = venues[0].get("upcomingEvents", {}).get("_total", 5000) if venues else 5000
        name = event.get("name", "")
        rows.append({
            "metro_area": metro_name,
            "event_date": date_str,
            "event_id": _make_event_id("ticketmaster", metro_name, date_str, name),
            "event_name": name,
            "event_category": category,
            "venue": venue_name,
            "est_attendance": int(attendance) * 1000,
            "source": "ticketmaster",
        })
    return rows


_SEATGEEK_SPORTS_TYPES = {
    "nba", "nfl", "mlb", "nhl", "ncaa_basketball", "ncaa_football",
    "mls", "concert", "theater",
}

_SEATGEEK_CATEGORY_MAP = {
    "nba": "major_sports", "nfl": "major_sports", "mlb": "major_sports",
    "nhl": "major_sports", "ncaa_basketball": "major_sports",
    "ncaa_football": "major_sports", "mls": "major_sports",
    "concert": "concert", "theater": "concert",
}


def fetch_seatgeek_events(
    metro_name: str,
    state: str,
    start_date: str,
    end_date: str,
    api_key: str,
    _fetch=None,
) -> list[dict]:
    """Fetch major events from SeatGeek for a state/date window.

    Returns [] on any HTTP error. _fetch is injectable for hermetic tests.
    """
    import requests

    if _fetch is None:
        _fetch = requests.get

    params = {
        "client_id": api_key,
        "venue.state": state,
        "datetime_local.gte": f"{start_date}T00:00:00",
        "datetime_local.lte": f"{end_date}T23:59:59",
        "per_page": 200,
    }
    resp = _fetch("https://api.seatgeek.com/2/events", params=params, timeout=30)
    if resp.status_code != 200:
        return []

    rows = []
    for event in resp.json().get("events", []):
        event_type = event.get("type", "")
        if event_type not in _SEATGEEK_SPORTS_TYPES:
            continue
        category = _SEATGEEK_CATEGORY_MAP.get(event_type, "concert")
        date_str = (event.get("datetime_local", "") or "")[:10]
        venue_name = event.get("venue", {}).get("name", "")
        name = event.get("title", "")
        rows.append({
            "metro_area": metro_name,
            "event_date": date_str,
            "event_id": _make_event_id("seatgeek", metro_name, date_str, name),
            "event_name": name,
            "event_category": category,
            "venue": venue_name,
            "est_attendance": 15000,
            "source": "seatgeek",
        })
    return rows
```

- [ ] **Step 5: Run the tests to confirm they pass**

```bash
pytest tests/test_refresh.py -k "ticketmaster or seatgeek" -v
```
Expected: 4 PASS

- [ ] **Step 6: Run the full suite**

```bash
pytest tests/ -q
```
Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add src/refresh/events_client.py tests/fixtures/ticketmaster_events.json tests/fixtures/seatgeek_events.json tests/test_refresh.py
git commit -m "feat(refresh): Ticketmaster + SeatGeek events client (optional, key-gated)"
```

---

## Task 6: Multiplier Engine + Config

The multiplier engine reads `conf/weather_event_multipliers.yml` and maps (weather_condition, alert_level) → (demand_multiplier, channel_shift_delivery) and event_category → event demand multiplier. Composed multipliers are capped at [0.3, 2.5].

**Files:**
- Create: `conf/weather_event_multipliers.yml`
- Create: `src/refresh/multiplier_engine.py`
- Modify: `tests/test_refresh.py`

- [ ] **Step 1: Create the config file**

Create `conf/weather_event_multipliers.yml`:

```yaml
# Demand multipliers for weather conditions.
# Sources: industry QSR studies on weather/delivery correlation.
# delivery_shift is added to 3pd_delivery channel share (carryout absorbs the offset).
weather:
  clear:        {demand: 1.00, delivery_shift: 0.00}
  rain:         {demand: 0.92, delivery_shift: 0.08}   # light rain shifts to delivery
  heavy_rain:   {demand: 0.85, delivery_shift: 0.15}
  snow:         {demand: 0.88, delivery_shift: 0.10}
  heavy_snow:   {demand: 0.70, delivery_shift: 0.20}   # significant foot traffic drop
  extreme_heat: {demand: 0.90, delivery_shift: 0.12}   # delivery preferred over walking
  extreme_cold: {demand: 0.82, delivery_shift: 0.15}
  storm:        {demand: 0.60, delivery_shift: 0.25}   # severe weather suppresses all demand

# NWS alert level adds a demand penalty on top of the weather condition multiplier.
alert_modifiers:
  advisory: {demand_delta: -0.05}
  watch:    {demand_delta: -0.10}
  warning:  {demand_delta: -0.20}  # warnings (e.g. "Blizzard Warning") drive hard cuts

# Event category multipliers — applied multiplicatively with weather.
events:
  major_sports:    {demand: 1.60}  # home game night: delivery surge for watch parties
  concert:         {demand: 1.30}
  festival:        {demand: 1.20}
  civic_holiday:   {demand: 0.85}  # e.g. Memorial Day: families cook at home
  national_holiday: {demand: 0.75} # Christmas, Thanksgiving: hard closures / low traffic

composition:
  max_multiplier: 2.50
  min_multiplier: 0.30
```

- [ ] **Step 2: Add failing tests**

Add to `tests/test_refresh.py`:

```python
# ---------------------------------------------------------------------------
# Multiplier engine
# ---------------------------------------------------------------------------

def test_multiplier_engine_clear_weather():
    from src.refresh.multiplier_engine import compute_weather_multipliers, load_config
    cfg = load_config()
    demand, shift = compute_weather_multipliers("clear", None, cfg)
    assert demand == 1.0
    assert shift == 0.0


def test_multiplier_engine_heavy_rain():
    from src.refresh.multiplier_engine import compute_weather_multipliers, load_config
    cfg = load_config()
    demand, shift = compute_weather_multipliers("heavy_rain", None, cfg)
    assert demand == 0.85
    assert shift == 0.15


def test_multiplier_engine_alert_reduces_demand():
    from src.refresh.multiplier_engine import compute_weather_multipliers, load_config
    cfg = load_config()
    demand_no_alert, _ = compute_weather_multipliers("rain", None, cfg)
    demand_warning, _ = compute_weather_multipliers("rain", "warning", cfg)
    assert demand_warning < demand_no_alert


def test_multiplier_engine_cap():
    from src.refresh.multiplier_engine import compose_multipliers, load_config
    cfg = load_config()
    # 1.6 * 1.6 = 2.56 → capped at 2.5
    result = compose_multipliers(1.6, 1.6, cfg)
    assert result == 2.5


def test_multiplier_engine_floor():
    from src.refresh.multiplier_engine import compose_multipliers, load_config
    cfg = load_config()
    # 0.6 * 0.4 = 0.24 → floored at 0.3
    result = compose_multipliers(0.6, 0.4, cfg)
    assert result == 0.3


def test_multiplier_engine_event_multiplier():
    from src.refresh.multiplier_engine import compute_event_multiplier, load_config
    cfg = load_config()
    assert compute_event_multiplier("major_sports", cfg) == 1.60
    assert compute_event_multiplier(None, cfg) == 1.0
```

- [ ] **Step 3: Run to confirm failures**

```bash
pytest tests/test_refresh.py -k "multiplier" -v
```
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 4: Implement multiplier_engine.py**

Create `src/refresh/multiplier_engine.py`:

```python
from pathlib import Path


def load_config(path: str | None = None) -> dict:
    """Load weather_event_multipliers.yml. Defaults to conf/ relative to project root."""
    import yaml

    if path is None:
        path = Path(__file__).parent.parent.parent / "conf" / "weather_event_multipliers.yml"
    with open(path) as f:
        return yaml.safe_load(f)


def compute_weather_multipliers(
    weather_condition: str,
    alert_level: str | None,
    config: dict,
) -> tuple[float, float]:
    """Returns (demand_multiplier, channel_shift_delivery) for a weather condition + alert."""
    weather_cfg = config["weather"].get(weather_condition, {"demand": 1.0, "delivery_shift": 0.0})
    demand = weather_cfg["demand"]
    delivery_shift = weather_cfg["delivery_shift"]

    if alert_level:
        delta = config["alert_modifiers"].get(alert_level, {}).get("demand_delta", 0.0)
        demand += delta

    limits = config["composition"]
    demand = max(limits["min_multiplier"], min(limits["max_multiplier"], demand))
    return round(demand, 4), round(delivery_shift, 4)


def compute_event_multiplier(event_category: str | None, config: dict) -> float:
    """Returns demand multiplier for an event category, or 1.0 if none."""
    if not event_category:
        return 1.0
    return config["events"].get(event_category, {"demand": 1.0})["demand"]


def compose_multipliers(weather_mult: float, event_mult: float, config: dict) -> float:
    """Multiply weather × event, clamped to [min_multiplier, max_multiplier]."""
    limits = config["composition"]
    composed = weather_mult * event_mult
    return round(max(limits["min_multiplier"], min(limits["max_multiplier"], composed)), 4)
```

- [ ] **Step 5: Install PyYAML if not already present**

```bash
python3 -c "import yaml; print('ok')" 2>/dev/null || pip install pyyaml -q
```

- [ ] **Step 6: Run the tests to confirm they pass**

```bash
pytest tests/test_refresh.py -k "multiplier" -v
```
Expected: 6 PASS

- [ ] **Step 7: Run the full suite**

```bash
pytest tests/ -q
```
Expected: all pass

- [ ] **Step 8: Commit**

```bash
git add conf/weather_event_multipliers.yml src/refresh/multiplier_engine.py tests/test_refresh.py
git commit -m "feat(refresh): multiplier engine reads weather_event_multipliers.yml"
```

---

## Task 7: Wire Weather/Event Data into CausalContext and Runner

`build_context()` gains an optional `weather_event_data: dict | None` param. When present it applies `demand_multiplier` and `channel_shift_delivery` to effective volume and channel mix, and populates the stub fields. `build_tick_rows()` and `backfill_ticks()` gain an optional `weather_event_lookup: dict[tuple, dict] | None` param; the lookup key is `(metro_area: str, date: date)`.

**Files:**
- Modify: `src/generator/causal_context.py`
- Modify: `src/generator/runner.py`
- Modify: `tests/test_causal_context.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/test_causal_context.py`:

```python
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
```

- [ ] **Step 2: Run to confirm failures**

```bash
pytest tests/test_causal_context.py -k "weather_event" -v
```
Expected: FAIL — `TypeError: build_context() got an unexpected keyword argument 'weather_event_data'`

- [ ] **Step 3: Modify causal_context.py**

Replace the `build_context` function in `src/generator/causal_context.py` (lines 100–132):

```python
def build_context(unit_id: int, timestamp: datetime, unit_volume_bias: float,
                  base_orders_per_hour: int = 18,
                  weather_event_data: dict | None = None) -> CausalContext:
    is_holiday, holiday_name, event_mult = _classify_event(timestamp)
    hourly = HOURLY_MULTIPLIERS[timestamp.hour]
    dow = DOW_MULTIPLIERS[timestamp.weekday()]

    weather_demand_mult = 1.0
    channel_shift_delivery = 0.0
    weather_condition = None
    precipitation_inches = None
    temperature_f = None
    local_event_type = None
    local_event_attendance = None

    if weather_event_data:
        weather_demand_mult = weather_event_data.get("demand_multiplier", 1.0) or 1.0
        channel_shift_delivery = weather_event_data.get("channel_shift_delivery", 0.0) or 0.0
        weather_condition = weather_event_data.get("weather_condition")
        precipitation_inches = weather_event_data.get("precipitation_inches")
        temperature_f = weather_event_data.get("high_temp_f")
        local_event_type = weather_event_data.get("event_category")
        local_event_attendance = weather_event_data.get("est_attendance")
        real_event_mult = weather_event_data.get("event_demand_multiplier")
        if real_event_mult:
            event_mult = max(event_mult, real_event_mult)

    effective_volume = (
        base_orders_per_hour * hourly * dow * event_mult * unit_volume_bias * weather_demand_mult
    )

    mix = dict(BASE_CHANNEL_MIX)
    if timestamp.hour >= 22 or timestamp.hour <= 1:
        mix["3pd_delivery"] = min(1.0, mix["3pd_delivery"] + 0.15)
        mix["carryout"] = max(0.0, mix["carryout"] - 0.15)
    if channel_shift_delivery > 0:
        mix["3pd_delivery"] = min(1.0, mix["3pd_delivery"] + channel_shift_delivery)
        mix["carryout"] = max(0.0, mix["carryout"] - channel_shift_delivery)

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
        weather_condition=weather_condition,
        precipitation_inches=precipitation_inches,
        temperature_f=temperature_f,
        local_event_type=local_event_type,
        local_event_attendance=local_event_attendance,
    )
```

- [ ] **Step 4: Run the causal context tests**

```bash
pytest tests/test_causal_context.py -v
```
Expected: all pass (including the 4 new tests)

- [ ] **Step 5: Modify runner.py**

In `src/generator/runner.py`, replace `build_tick_rows` and `backfill_ticks` signatures and bodies:

```python
def build_tick_rows(
    unit_id: int,
    timestamp: datetime,
    registry: EntityRegistry,
    tick_seconds: int = 60,
    base_orders_per_hour: int = 18,
    weather_event_lookup: dict | None = None,
) -> list[dict]:
    """All domain rows for one unit, one tick."""
    unit = registry.unit_by_id(unit_id)

    weather_event_data = None
    if weather_event_lookup:
        key = (unit.get("metro_area"), timestamp.date())
        weather_event_data = weather_event_lookup.get(key)

    ctx = build_context(unit_id, timestamp, unit["unit_volume_bias"], base_orders_per_hour,
                        weather_event_data)
    order_rows = generate_orders_for_tick(ctx, registry, tick_seconds)
    inv_rows = generate_inventory_events(ctx, registry, order_rows)
    loyalty_rows = generate_loyalty_events(ctx, registry, order_rows)
    return order_rows + inv_rows + loyalty_rows


def backfill_ticks(
    registry: EntityRegistry,
    backfill_months: int,
    tick_seconds: int = 3600,
    base_orders_per_hour: int = 18,
    start_dt: datetime | None = None,
    end_dt: datetime | None = None,
    weather_event_lookup: dict | None = None,
) -> Iterator[list[dict]]:
    """Yield batches of rows for all units, one tick at a time."""
    from dateutil.relativedelta import relativedelta

    now = datetime.now().replace(minute=0, second=0, microsecond=0)
    start = start_dt if start_dt is not None else now - relativedelta(months=backfill_months)
    end   = end_dt   if end_dt   is not None else now
    current = start
    while current < end:
        batch = []
        for unit in registry.all_units():
            uid = unit["unit_id"]
            batch.extend(build_tick_rows(uid, current, registry, tick_seconds,
                                         base_orders_per_hour, weather_event_lookup))
            if current.hour == 10 and current.minute == 0:
                batch.extend(
                    generate_shift_events(
                        uid,
                        current.date().isoformat(),
                        projected_orders=base_orders_per_hour * 12,
                        tick_ts=current,
                    )
                )
                batch.extend(generate_new_guest_profiles(uid, current.date().isoformat(), tick_ts=current))
                batch.extend(generate_guest_churn(uid, registry, current.date().isoformat(), tick_ts=current))
                batch.extend(generate_daily_receiving(uid, registry, current.date().isoformat(), tick_ts=current))
        yield batch
        current += timedelta(seconds=tick_seconds)
```

- [ ] **Step 6: Run the full suite**

```bash
pytest tests/ -q
```
Expected: all 75+ pass

- [ ] **Step 7: Commit**

```bash
git add src/generator/causal_context.py src/generator/runner.py tests/test_causal_context.py
git commit -m "feat(causal_context): wire weather/event data into demand multipliers and channel mix"
```

---

## Task 8: Load Lookup in main.py

`main.py` is the Databricks notebook. Add a `_load_weather_event_lookup()` function that reads `ref.weather_conditions` and `ref.local_events` via Spark and returns a `dict[(metro_area, date), dict]`. Call it once before `backfill_ticks()`; pass the result as `weather_event_lookup`. Falls back to `{}` silently if the tables are empty or missing.

**Files:**
- Modify: `src/generator/main.py:29-38` (after registry load, before DOMAIN_TABLE_MAP)

- [ ] **Step 1: Add the lookup loader function after the registry load (line 37)**

In `src/generator/main.py`, after the line `registry = EntityRegistry.from_spark(...)`, add a new `# COMMAND ----------` block:

```python
# COMMAND ----------
def _load_weather_event_lookup():
    """Load (metro_area, date) → weather/event dict from ref tables. Returns {} on any error."""
    try:
        weather_rows = spark.sql(f"""
            SELECT metro_area, forecast_date, weather_condition, high_temp_f, low_temp_f,
                   precipitation_inches, alert_level, demand_multiplier, channel_shift_delivery
            FROM {catalog_name}.{schema_prefix}ref.weather_conditions
        """).collect()

        event_rows = spark.sql(f"""
            SELECT metro_area, event_date, event_category, est_attendance, est_demand_multiplier
            FROM {catalog_name}.{schema_prefix}ref.local_events
        """).collect()

        lookup = {}
        for r in weather_rows:
            fd = r.forecast_date
            d = fd.date() if hasattr(fd, "date") else fd
            key = (r.metro_area, d)
            lookup[key] = {
                "weather_condition": r.weather_condition,
                "high_temp_f": r.high_temp_f,
                "low_temp_f": r.low_temp_f,
                "precipitation_inches": r.precipitation_inches,
                "alert_level": r.alert_level,
                "demand_multiplier": r.demand_multiplier,
                "channel_shift_delivery": r.channel_shift_delivery,
            }
        for r in event_rows:
            ed = r.event_date
            d = ed.date() if hasattr(ed, "date") else ed
            key = (r.metro_area, d)
            entry = lookup.setdefault(key, {})
            entry["event_category"] = r.event_category
            entry["est_attendance"] = r.est_attendance
            entry["event_demand_multiplier"] = r.est_demand_multiplier

        print(f"[INFO] Weather/event lookup: {len(lookup)} (metro, date) entries loaded")
        return lookup
    except Exception as e:
        print(f"[WARN] Weather/event lookup skipped (tables empty or missing): {e}")
        return {}

weather_event_lookup = _load_weather_event_lookup()
```

- [ ] **Step 2: Pass lookup to backfill_ticks in both the backfill and live branches**

In the `if mode == "backfill":` block, change the `backfill_ticks(...)` call:

```python
    for i, batch in enumerate(
        backfill_ticks(
            registry,
            backfill_months,
            tick_seconds=3600,
            base_orders_per_hour=base_orders,
            start_dt=start_dt,
            end_dt=end_dt,
            weather_event_lookup=weather_event_lookup,
        )
    ):
```

In the `else:` (live) block, change the `backfill_ticks(...)` call:

```python
    for batch in backfill_ticks(registry, backfill_months=1, tick_seconds=live_tick_seconds,
                                 base_orders_per_hour=base_orders, start_dt=start_dt, end_dt=end_dt,
                                 weather_event_lookup=weather_event_lookup):
```

- [ ] **Step 3: Run the full test suite to confirm no regressions**

```bash
pytest tests/ -q
```
Expected: all pass

- [ ] **Step 4: Commit**

```bash
git add src/generator/main.py
git commit -m "feat(main): load weather/event lookup from ref tables, pass to backfill_ticks"
```

---

## Task 9: Refresh Notebook + DAB Job Wiring

The refresh notebook reads `ref.unit` to get all 20 distinct metros (lat/lon/state), calls each client, applies multipliers, and MERGEs results into `ref.weather_conditions` and `ref.local_events`. The DAB job runs daily. A new `initial_weather_refresh` task in `setup_job.yml` fires once after `setup` and before `backfill` so the demo has real data on day 1. Two new optional variables let operators configure the Ticketmaster and SeatGeek secret scopes.

**Files:**
- Create: `src/refresh/refresh_notebook.py`
- Create: `resources/refresh_weather_events.yml`
- Modify: `resources/setup_job.yml`
- Modify: `databricks.yml`

- [ ] **Step 1: Create the refresh notebook**

Create `src/refresh/refresh_notebook.py`:

```python
# Databricks notebook source
# COMMAND ----------
import sys

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_notebook_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

# COMMAND ----------
def _widget(name, default):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default

catalog_name             = _widget("catalog_name", "jmrdemo")
schema_prefix            = _widget("schema_prefix", "synth_")
tm_secret_scope          = _widget("ticketmaster_secret_scope", "qsr-synth")
tm_secret_key            = _widget("ticketmaster_secret_key", "ticketmaster_consumer_key")
sg_secret_scope          = _widget("seatgeek_secret_scope", "qsr-synth")
sg_secret_key            = _widget("seatgeek_secret_key", "seatgeek_client_id")

print(f"[INFO] refresh_notebook: catalog={catalog_name}, schema_prefix={schema_prefix}")

# COMMAND ----------
from datetime import datetime, timedelta, date

from src.refresh.openmeteo_client import fetch_metro_weather
from src.refresh.noaa_client import fetch_state_alerts
from src.refresh.nager_client import fetch_us_holidays
from src.refresh.events_client import fetch_ticketmaster_events, fetch_seatgeek_events
from src.refresh.multiplier_engine import load_config, compute_weather_multipliers, compute_event_multiplier

cfg = load_config()
today = date.today()
start_date = (today - timedelta(days=30)).isoformat()
end_date   = (today + timedelta(days=14)).isoformat()
refreshed_at = datetime.now().isoformat()

# COMMAND ----------
# Load distinct metros from ref.unit
metro_rows = spark.sql(f"""
    SELECT DISTINCT metro_area, state,
           AVG(latitude) AS lat, AVG(longitude) AS lon
    FROM {catalog_name}.{schema_prefix}ref.unit
    GROUP BY metro_area, state
""").collect()

print(f"[INFO] Refreshing {len(metro_rows)} metros")

# COMMAND ----------
# Step 1: Fetch weather + apply NOAA alerts per state
weather_by_metro_date = {}  # (metro, date_str) → dict

for m in metro_rows:
    try:
        rows = fetch_metro_weather(m.metro_area, m.lat, m.lon)
        for r in rows:
            weather_by_metro_date[(m.metro_area, r["forecast_date"])] = r
        print(f"[OK] Open-Meteo: {m.metro_area} ({len(rows)} rows)")
    except Exception as e:
        print(f"[WARN] Open-Meteo failed for {m.metro_area}: {e}")

# Apply NOAA alerts — keyed by state, spread across matching date rows
states = list({m.state for m in metro_rows})
for state in states:
    try:
        alerts = fetch_state_alerts(state)
        state_metros = [m.metro_area for m in metro_rows if m.state == state]
        for alert in alerts:
            onset_str  = alert["onset"][:10] if alert["onset"] else ""
            expires_str = alert["expires"][:10] if alert["expires"] else ""
            for metro in state_metros:
                for (m_area, d_str), row in weather_by_metro_date.items():
                    if m_area == metro and onset_str <= d_str <= expires_str:
                        # Use the highest-severity alert level
                        existing = row.get("alert_level")
                        severity_rank = {"warning": 3, "watch": 2, "advisory": 1}
                        new_rank = severity_rank.get(alert["alert_level"], 0)
                        old_rank = severity_rank.get(existing, 0)
                        if new_rank > old_rank:
                            row["alert_level"] = alert["alert_level"]
        if alerts:
            print(f"[OK] NOAA alerts: {state} — {len(alerts)} classifiable alerts")
    except Exception as e:
        print(f"[WARN] NOAA alerts failed for {state}: {e}")

# Compute multipliers and add to each weather row
for row in weather_by_metro_date.values():
    demand_mult, shift = compute_weather_multipliers(
        row["weather_condition"], row.get("alert_level"), cfg
    )
    row["demand_multiplier"] = demand_mult
    row["channel_shift_delivery"] = shift
    row["refreshed_at"] = refreshed_at

# COMMAND ----------
# Step 2: Write weather_conditions via MERGE
from pyspark.sql import Row

weather_rows = list(weather_by_metro_date.values())
if weather_rows:
    weather_df = spark.createDataFrame([Row(**r) for r in weather_rows])
    weather_df.createOrReplaceTempView("_weather_refresh")
    spark.sql(f"""
        MERGE INTO {catalog_name}.{schema_prefix}ref.weather_conditions t
        USING _weather_refresh s
        ON t.metro_area = s.metro_area AND t.forecast_date = s.forecast_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"[OK] Merged {len(weather_rows)} rows into ref.weather_conditions")

# COMMAND ----------
# Step 3: Fetch events — holidays (always) + Ticketmaster + SeatGeek (optional)
event_rows_by_id = {}  # event_id → dict

# Nager.Date holidays for current + next year
for year in [today.year, today.year + 1]:
    for m in metro_rows:
        try:
            rows = fetch_us_holidays(year, m.state)
            for r in rows:
                event_rows_by_id[r["event_id"]] = {**r, "metro_area": m.metro_area,
                                                    "est_demand_multiplier": compute_event_multiplier(r["event_category"], cfg),
                                                    "refreshed_at": refreshed_at}
        except Exception as e:
            print(f"[WARN] Nager holidays failed for {m.state} {year}: {e}")

# Ticketmaster (optional)
try:
    tm_key = dbutils.secrets.get(scope=tm_secret_scope, key=tm_secret_key)
    for m in metro_rows:
        try:
            rows = fetch_ticketmaster_events(m.metro_area, m.state, start_date, end_date, tm_key)
            for r in rows:
                event_rows_by_id[r["event_id"]] = {**r,
                    "est_demand_multiplier": compute_event_multiplier(r["event_category"], cfg),
                    "refreshed_at": refreshed_at}
        except Exception as e:
            print(f"[WARN] Ticketmaster failed for {m.metro_area}: {e}")
    print(f"[OK] Ticketmaster: fetched events for {len(metro_rows)} metros")
except Exception:
    print("[INFO] Ticketmaster secret not configured — skipping (holidays still populated)")

# SeatGeek (optional)
try:
    sg_key = dbutils.secrets.get(scope=sg_secret_scope, key=sg_secret_key)
    for m in metro_rows:
        try:
            rows = fetch_seatgeek_events(m.metro_area, m.state, start_date, end_date, sg_key)
            for r in rows:
                if r["event_id"] not in event_rows_by_id:
                    event_rows_by_id[r["event_id"]] = {**r,
                        "est_demand_multiplier": compute_event_multiplier(r["event_category"], cfg),
                        "refreshed_at": refreshed_at}
        except Exception as e:
            print(f"[WARN] SeatGeek failed for {m.metro_area}: {e}")
    print(f"[OK] SeatGeek: fetched events for {len(metro_rows)} metros")
except Exception:
    print("[INFO] SeatGeek secret not configured — skipping")

# COMMAND ----------
# Step 4: Write local_events via MERGE
event_rows = list(event_rows_by_id.values())
if event_rows:
    event_df = spark.createDataFrame([Row(**r) for r in event_rows])
    event_df.createOrReplaceTempView("_events_refresh")
    spark.sql(f"""
        MERGE INTO {catalog_name}.{schema_prefix}ref.local_events t
        USING _events_refresh s
        ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"[OK] Merged {len(event_rows)} rows into ref.local_events")

print("[INFO] Weather & events refresh complete")
```

- [ ] **Step 2: Create the DAB job resource file**

Create `resources/refresh_weather_events.yml`:

```yaml
resources:
  jobs:
    weather_events_refresh_job:
      name: "[${bundle.target} ${workspace.current_user.userName}] Weather & Events Refresh [${bundle.target}]"
      description: "Daily refresh of ref.weather_conditions and ref.local_events from Open-Meteo, NOAA alerts, Nager.Date, Ticketmaster, and SeatGeek."
      schedule:
        quartz_cron_expression: "${var.weather_refresh_cron}"
        timezone_id: "UTC"
        pause_status: UNPAUSED
      tasks:
        - task_key: refresh_weather_events
          notebook_task:
            notebook_path: ../src/refresh/refresh_notebook.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              schema_prefix: ${var.schema_prefix}
              ticketmaster_secret_scope: ${var.ticketmaster_secret_scope}
              ticketmaster_secret_key: "ticketmaster_consumer_key"
              seatgeek_secret_scope: ${var.seatgeek_secret_scope}
              seatgeek_secret_key: "seatgeek_client_id"
          libraries:
            - pypi:
                package: requests
            - pypi:
                package: pyyaml
```

- [ ] **Step 3: Add variables to databricks.yml**

In `databricks.yml`, add to the `variables:` section:

```yaml
  weather_refresh_cron:
    default: "0 0 5 * * ?"
    description: "Quartz cron for daily weather/events refresh job (default 05:00 UTC)."
  ticketmaster_secret_scope:
    default: "qsr-synth"
    description: "Databricks secret scope containing ticketmaster_consumer_key. Leave default if not using Ticketmaster."
  seatgeek_secret_scope:
    default: "qsr-synth"
    description: "Databricks secret scope containing seatgeek_client_id. Leave default if not using SeatGeek."
```

- [ ] **Step 4: Add initial_weather_refresh task to setup_job.yml**

In `resources/setup_job.yml`, add a new task after the `setup` task definition, and update `backfill` to depend on it:

```yaml
        - task_key: initial_weather_refresh
          depends_on:
            - task_key: setup
          notebook_task:
            notebook_path: ../src/refresh/refresh_notebook.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              schema_prefix: ${var.schema_prefix}
              ticketmaster_secret_scope: ${var.ticketmaster_secret_scope}
              ticketmaster_secret_key: "ticketmaster_consumer_key"
              seatgeek_secret_scope: ${var.seatgeek_secret_scope}
              seatgeek_secret_key: "seatgeek_client_id"
          libraries:
            - pypi:
                package: requests
            - pypi:
                package: pyyaml
```

Change `backfill`'s `depends_on` to also include `initial_weather_refresh`:

```yaml
        - task_key: backfill
          depends_on:
            - task_key: setup
            - task_key: initial_weather_refresh
```

- [ ] **Step 5: Validate the bundle**

```bash
databricks bundle validate -p DEFAULT 2>&1 | tail -5
```
Expected: `Validation OK` (or equivalent — no errors)

- [ ] **Step 6: Run the full test suite**

```bash
pytest tests/ -q
```
Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add src/refresh/refresh_notebook.py resources/refresh_weather_events.yml resources/setup_job.yml databricks.yml
git commit -m "feat(refresh): daily weather/events refresh job wired into setup DAG"
```

---

## Task 10: Demand Risk Forecast Metric View

Add a `demand_risk_forecast` view to `create_metric_views.py`. It joins `ref.unit × ref.weather_conditions × ref.local_events` for the next 14 days, computes a `combined_demand_multiplier`, and labels each (unit, date) as `demand_risk` (<0.8), `capacity_risk` (>1.4), or `normal`.

**Files:**
- Modify: `src/setup/create_metric_views.py`

- [ ] **Step 1: Add the view at the end of create_metric_views.py**

Append a new `# COMMAND ----------` block to `src/setup/create_metric_views.py`:

```python
# COMMAND ----------
# 5. Demand Risk Forecast — (unit, date) risk signal for next 14 days based on weather + events
spark.sql(f"""
    CREATE OR REPLACE VIEW {c}.{schema_prefix}metrics.demand_risk_forecast AS
    SELECT
        u.unit_id,
        u.metro_area,
        u.franchisee_id,
        u.region_id,
        w.forecast_date,
        w.observation_type,
        w.weather_condition,
        w.alert_level,
        w.high_temp_f,
        w.low_temp_f,
        w.precipitation_inches,
        w.demand_multiplier                             AS weather_demand_multiplier,
        w.channel_shift_delivery,
        e.event_name,
        e.event_category,
        e.venue,
        e.est_attendance,
        e.est_demand_multiplier                         AS event_demand_multiplier,
        e.source                                        AS event_source,
        ROUND(LEAST(2.5, GREATEST(0.3,
            COALESCE(w.demand_multiplier, 1.0) * COALESCE(e.est_demand_multiplier, 1.0)
        )), 4)                                          AS combined_demand_multiplier,
        CASE
            WHEN LEAST(2.5, GREATEST(0.3,
                COALESCE(w.demand_multiplier, 1.0) * COALESCE(e.est_demand_multiplier, 1.0)
            )) < 0.8  THEN 'demand_risk'
            WHEN LEAST(2.5, GREATEST(0.3,
                COALESCE(w.demand_multiplier, 1.0) * COALESCE(e.est_demand_multiplier, 1.0)
            )) > 1.4  THEN 'capacity_risk'
            ELSE 'normal'
        END                                             AS risk_level
    FROM {c}.{schema_prefix}ref.unit u
    JOIN {c}.{schema_prefix}ref.weather_conditions w
        ON  u.metro_area   = w.metro_area
        AND w.forecast_date BETWEEN CURRENT_DATE AND DATE_ADD(CURRENT_DATE, 14)
    LEFT JOIN {c}.{schema_prefix}ref.local_events e
        ON  u.metro_area = e.metro_area
        AND w.forecast_date = e.event_date
""")
print(f"[INFO] View ready: {c}.{schema_prefix}metrics.demand_risk_forecast")
```

- [ ] **Step 2: Run the full test suite**

```bash
pytest tests/ -q
```
Expected: all pass

- [ ] **Step 3: Commit**

```bash
git add src/setup/create_metric_views.py
git commit -m "feat(metrics): demand_risk_forecast view — 14-day weather+events risk signal per unit"
```

---

## Task 11: Update Docs

**Files:**
- Modify: `docs/roadmap.md`
- Modify: `docs/handoff.md`

- [ ] **Step 1: Update roadmap.md — mark Phase 3 as in progress and add implementation details**

In `docs/roadmap.md`, replace the Phase 3 section:

```markdown
## 🚧 Phase 3 — External Signal Integration

> Status: In progress

### 3.1 Weather Data (Open-Meteo + NOAA Alerts) ✅
`src/refresh/openmeteo_client.py` fetches 30-day historical + 14-day forecast from Open-Meteo (no key). `src/refresh/noaa_client.py` fetches active NWS alerts per state. Combined into `ref.weather_conditions` with `demand_multiplier` and `channel_shift_delivery` pre-computed from `conf/weather_event_multipliers.yml`.

### 3.2 Local Events (Nager.Date + Ticketmaster + SeatGeek) ✅
`src/refresh/nager_client.py` provides federal/state holidays (no key). `src/refresh/events_client.py` fetches major sports + concerts from Ticketmaster and SeatGeek (optional — key-gated, graceful skip if absent). Events land in `ref.local_events` with `est_demand_multiplier`.

### 3.3 Demand Model Integration ✅
`CausalContext.build_context()` accepts optional `weather_event_data` dict. `runner.backfill_ticks()` accepts optional `weather_event_lookup: dict[(metro_area, date), dict]`. `main.py` loads the lookup from ref tables once per run and passes it through. No-data fallback is silent (multiplier=1.0).

### 3.4 Demand Risk Forecast View ✅
`metrics.demand_risk_forecast` joins units × weather × events for the next 14 days. Labels each (unit, date) as `demand_risk`, `capacity_risk`, or `normal`. Queryable from Genie Space: "Which units have the highest demand risk this week?"

### 3.5 Daily Refresh Job ✅
`resources/refresh_weather_events.yml` declares a DAB-managed job on daily cron (05:00 UTC). `setup_job.yml` adds `initial_weather_refresh` task (after `setup`, before `backfill`) so data is available on day 1.

### Remaining Phase 3 Work
- Marketing domain: campaigns, promotions, loyalty program configuration
- Causal model upgrade: weather + events as statistically calibrated multipliers (current values are informed estimates)
```

- [ ] **Step 2: Add a "Weather & Events" section to handoff.md**

Add after the "Access Control Model" section:

```markdown
## Weather & Events Integration

### Refresh Job
`Weather & Events Refresh` runs daily at 05:00 UTC. It fetches:
- **Open-Meteo** (no key): 30-day historical + 14-day forecast per metro — temperature, precipitation, WMO weather code
- **NOAA NWS alerts** (no key): active advisory/watch/warning per state, applied to matching forecast dates
- **Nager.Date** (no key): US federal + state holidays
- **Ticketmaster** (optional, `secrets/qsr-synth/ticketmaster_consumer_key`): major sports + concerts
- **SeatGeek** (optional, `secrets/qsr-synth/seatgeek_client_id`): supplemental sports + music events

Results are MERGEd into `ref.weather_conditions` (keyed by metro+date) and `ref.local_events` (keyed by event_id). Missing keys gracefully skip the source and retain prior data.

### Multiplier Config
`conf/weather_event_multipliers.yml` controls all demand adjustments. Edit it to tune weather sensitivity without code changes.

### Demand Model
The generator loads a `(metro_area, date) → dict` lookup from the ref tables at startup. Each unit tick looks up its metro+date and applies `demand_multiplier` (overall volume shift) and `channel_shift_delivery` (carryout → delivery share shift). The lookup falls back to multiplier=1.0 if the tables are empty.

### Risk Forecast View
`metrics.demand_risk_forecast` provides a 14-day forward risk signal:
- `demand_risk`: combined multiplier < 0.8 (storm, blizzard warning, holiday closure)
- `capacity_risk`: combined multiplier > 1.4 (playoff game + clear weather, NYE)
- `normal`: everything else

Genie Space prompt: *"Which units have the highest capacity risk next 7 days?"*

### Adding Event API Keys (One-Time)
```bash
# Ticketmaster
databricks secrets create-scope qsr-synth -p DEFAULT 2>/dev/null || true
databricks secrets put-secret qsr-synth ticketmaster_consumer_key -p DEFAULT

# SeatGeek
databricks secrets put-secret qsr-synth seatgeek_client_id -p DEFAULT
```
```

- [ ] **Step 3: Run the full test suite one final time**

```bash
pytest tests/ -q
```
Expected: all pass

- [ ] **Step 4: Commit**

```bash
git add docs/roadmap.md docs/handoff.md
git commit -m "docs: Phase 3 weather/events integration documented in roadmap and handoff"
```
