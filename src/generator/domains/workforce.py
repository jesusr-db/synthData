import random
from datetime import datetime, timedelta

_SHIFT_WINDOWS = [
    ("open",    "10:00", "16:00"),
    ("mid",     "14:00", "21:00"),
    ("close",   "17:00", "23:00"),
    ("overlap", "12:00", "20:00"),
]

def _staff_count(projected_orders: int) -> int:
    """1 staff per ~25 orders/day, minimum 3, maximum 15."""
    return min(15, max(3, projected_orders // 25))

def generate_shift_events(unit_id: int, date_str: str,
                          projected_orders: int = 80,
                          tick_ts: datetime | None = None) -> list[dict]:
    rows = []
    n_staff = _staff_count(projected_orders)
    shifts = (_SHIFT_WINDOWS * 4)[:n_staff]
    for i, (label, start_str, end_str) in enumerate(shifts):
        shift_id = unit_id * 10000 + i
        emp_id = unit_id * 1000 + i + 1
        shift_start = datetime.strptime(f"{date_str} {start_str}", "%Y-%m-%d %H:%M")
        shift_end = datetime.strptime(f"{date_str} {end_str}", "%Y-%m-%d %H:%M")

        no_show = random.random() < 0.04
        rows.append({
            "event_type": "shift",
            "event_id": shift_id,
            "event_ts": tick_ts or shift_start,
            "shift_id": shift_id,
            "unit_id": unit_id,
            "employee_id": emp_id,
            "shift_label": label,
            "shift_start": shift_start,
            "shift_end": shift_end,
            "status": "no_show" if no_show else "completed",
            "date": date_str,
        })
        if not no_show:
            punch_in = shift_start + timedelta(minutes=random.randint(-2, 8))
            punch_out = shift_end + timedelta(minutes=random.randint(-5, 15))
            punch_id = unit_id * 20000 + i
            rows.append({
                "event_type": "time_punch",
                "event_id": punch_id,
                "event_ts": tick_ts or punch_in,
                "time_punch_id": punch_id,
                "employee_id": emp_id,
                "unit_id": unit_id,
                "punch_in": punch_in,
                "punch_out": punch_out,
                "hours_worked": round((punch_out - punch_in).seconds / 3600, 2),
            })
    return rows
