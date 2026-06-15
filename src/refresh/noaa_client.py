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
