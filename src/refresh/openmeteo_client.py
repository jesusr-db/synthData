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
