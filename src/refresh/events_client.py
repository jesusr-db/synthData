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
