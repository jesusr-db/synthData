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
