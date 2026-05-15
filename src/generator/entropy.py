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
