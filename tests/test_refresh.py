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
