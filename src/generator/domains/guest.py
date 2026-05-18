import random
from datetime import datetime
from faker import Faker

_fake = Faker()
_guest_counter = 0

def _next_guest_id() -> int:
    global _guest_counter
    _guest_counter += 1
    return _guest_counter

def generate_new_guest_profiles(unit_id: int, date_str: str,
                                 growth_rate: float = 0.008,
                                 base_pool: int = 500,
                                 tick_ts: datetime | None = None) -> list[dict]:
    """~0.8% of base_pool guests are new per day per unit."""
    n = max(0, round(base_pool * growth_rate * random.gauss(1.0, 0.3)))
    rows = []
    for _ in range(n):
        gid = _next_guest_id()
        rows.append({
            "event_type": "guest_profile",
            "event_id": gid,
            "event_ts": tick_ts,
            "guest_profile_id": gid,
            "unit_id": unit_id,
            "first_name": _fake.first_name(),
            "last_name": _fake.last_name(),
            "email": _fake.email(),
            "phone": _fake.phone_number()[:20],
            "zip_code": _fake.zipcode(),
            "created_date": date_str,
            "digital_account_id": gid,
            "account_status": (
                "suspended" if random.random() < 0.005
                else "inactive" if random.random() < 0.035
                else "active"
            ),
        })
    return rows


def generate_guest_churn(unit_id: int, registry, date_str: str,
                          churn_rate: float = 0.002,
                          tick_ts: datetime | None = None) -> list[dict]:
    """Emit ~0.2% of guest pool per unit per day as account deactivations."""
    pool = registry.guest_ids_for_unit(unit_id)
    n = max(0, round(len(pool) * churn_rate * random.gauss(1.0, 0.3)))
    rows = []
    for gid in random.sample(pool, min(n, len(pool))):
        rows.append({
            "event_type": "guest_profile",
            "event_id": _next_guest_id(),
            "event_ts": tick_ts,
            "guest_profile_id": gid,
            "unit_id": unit_id,
            "first_name": None,
            "last_name": None,
            "email": None,
            "phone": None,
            "zip_code": None,
            "created_date": date_str,
            "digital_account_id": gid,
            "account_status": "inactive",
        })
    return rows
