import random
from datetime import datetime, timedelta

_emp_counter = 0

def _next_emp_id(unit_id: int, shift_num: int) -> int:
    return unit_id * 1000 + shift_num

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
                          projected_orders: int = 80) -> list[dict]:
    rows = []
    n_staff = _staff_count(projected_orders)
    shifts = (_SHIFT_WINDOWS * 4)[:n_staff]
    for i, (label, start_str, end_str) in enumerate(shifts):
        emp_id = _next_emp_id(unit_id, i + 1)
        base_date = datetime.strptime(date_str, "%Y-%m-%d")
        shift_start = datetime.strptime(f"{date_str} {start_str}", "%Y-%m-%d %H:%M")
        shift_end = datetime.strptime(f"{date_str} {end_str}", "%Y-%m-%d %H:%M")

        no_show = random.random() < 0.04
        rows.append({
            "event_type": "shift",
            "shift_id": unit_id * 10000 + i,
            "unit_id": unit_id,
            "employee_id": emp_id,
            "shift_label": label,
            "shift_start": shift_start.isoformat(),
            "shift_end": shift_end.isoformat(),
            "status": "no_show" if no_show else "completed",
            "date": date_str,
        })
        if not no_show:
            # Punch in/out with small variance
            punch_in = shift_start + timedelta(minutes=random.randint(-2, 8))
            punch_out = shift_end + timedelta(minutes=random.randint(-5, 15))
            rows.append({
                "event_type": "time_punch",
                "time_punch_id": unit_id * 20000 + i,
                "employee_id": emp_id,
                "unit_id": unit_id,
                "punch_in": punch_in.isoformat(),
                "punch_out": punch_out.isoformat(),
                "hours_worked": round((punch_out - punch_in).seconds / 3600, 2),
            })
    return rows
