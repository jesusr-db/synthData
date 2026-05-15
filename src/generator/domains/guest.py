import random
from faker import Faker

_fake = Faker()
_guest_counter = 0

def _next_guest_id() -> int:
    global _guest_counter
    _guest_counter += 1
    return _guest_counter

def generate_new_guest_profiles(unit_id: int, date_str: str,
                                 growth_rate: float = 0.008,
                                 base_pool: int = 500) -> list[dict]:
    """~0.8% of base_pool guests are new per day per unit."""
    n = max(0, round(base_pool * growth_rate * random.gauss(1.0, 0.3)))
    rows = []
    for _ in range(n):
        gid = _next_guest_id()
        rows.append({
            "event_type": "guest_profile",
            "guest_profile_id": gid,
            "unit_id": unit_id,
            "first_name": _fake.first_name(),
            "last_name": _fake.last_name(),
            "email": _fake.email(),
            "phone": _fake.phone_number()[:20],
            "zip_code": _fake.zipcode(),
            "created_date": date_str,
            "digital_account_id": gid,  # 1:1 for simplicity
            "account_status": "active",
        })
    return rows
