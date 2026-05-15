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
