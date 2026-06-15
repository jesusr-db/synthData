"""Pure-Python customer feature computation (no Spark).

Inputs are lists of dict rows (one per silver row) so the logic is hermetically
testable. The Spark notebook in src/setup/build_feature_tables.py converts
DataFrame rows to these dicts and writes the result to a UC feature table.
"""
from datetime import datetime

CATEGORIES = ["pizza", "wings", "sides", "salads", "drinks", "desserts"]


def compute_customer_features(orders, items, tiers, menu, as_of: datetime):
    """Return one feature record per non-null guest_profile_id.

    orders: rows with profile_id, guest_order_id, total_amount, placed_at
    items:  rows with guest_order_id, menu_item_id, quantity, line_net_amount
    tiers:  dict profile_id -> tier string (latest known)
    menu:   dict menu_item_id -> (category, subcategory, name)
    """
    # Map order -> owning profile (skip anonymous orders).
    order_owner = {o["guest_order_id"]: o["profile_id"] for o in orders if o.get("profile_id") is not None}

    # Per-profile aggregates.
    agg: dict[int, dict] = {}
    for o in orders:
        pid = o.get("profile_id")
        if pid is None:
            continue
        a = agg.setdefault(pid, {"orders": 0, "monetary": 0.0, "last": None,
                                 "cat_spend": {c: 0.0 for c in CATEGORIES}})
        a["orders"] += 1
        a["monetary"] += float(o.get("total_amount") or 0.0)
        placed = o.get("placed_at")
        if placed is not None and (a["last"] is None or placed > a["last"]):
            a["last"] = placed

    for it in items:
        pid = order_owner.get(it["guest_order_id"])
        if pid is None:
            continue
        row = menu.get(int(it["menu_item_id"]))
        if not row:
            continue
        cat = row[0]
        if cat in agg[pid]["cat_spend"]:
            agg[pid]["cat_spend"][cat] += float(it.get("line_net_amount") or 0.0)

    records = []
    for pid, a in agg.items():
        total_cat = sum(a["cat_spend"].values())
        rec = {
            "guest_profile_id": int(pid),
            "total_orders": a["orders"],
            "monetary_total": round(a["monetary"], 4),
            "aov": round(a["monetary"] / a["orders"], 4) if a["orders"] else 0.0,
            "recency_days": (as_of.date() - a["last"].date()).days if a["last"] else -1,
            "tier": tiers.get(pid, "none"),
        }
        for c in CATEGORIES:
            rec[f"affinity_{c}"] = round(a["cat_spend"][c] / total_cat, 6) if total_cat else 0.0
        records.append(rec)
    return records
