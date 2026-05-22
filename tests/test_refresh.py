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
