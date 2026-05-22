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
