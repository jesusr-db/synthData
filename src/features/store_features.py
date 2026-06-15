"""Pure-Python store feature computation (no Spark)."""
from src.features.customer_features import CATEGORIES


def compute_store_features(orders, items, units, menu):
    """Return one feature record per unit_id.

    orders: rows with guest_order_id, unit_id, total_amount
    items:  rows with guest_order_id, unit_id, menu_item_id, quantity, line_net_amount
    units:  dict unit_id -> {metro_area, region_id, franchisee_id}
    menu:   dict menu_item_id -> (category, subcategory, name)
    """
    order_agg: dict[int, dict] = {}
    for o in orders:
        uid = o["unit_id"]
        a = order_agg.setdefault(uid, {"orders": 0, "revenue": 0.0})
        a["orders"] += 1
        a["revenue"] += float(o.get("total_amount") or 0.0)

    qty: dict[int, dict[int, int]] = {}     # unit -> item -> qty
    cat_qty: dict[int, dict[str, dict]] = {}  # unit -> cat -> {item: qty}
    for it in items:
        uid = it["unit_id"]
        iid = int(it["menu_item_id"])
        q = int(it.get("quantity") or 0)
        qty.setdefault(uid, {}).setdefault(iid, 0)
        qty[uid][iid] += q
        row = menu.get(iid)
        if row:
            cat = row[0]
            cat_qty.setdefault(uid, {}).setdefault(cat, {}).setdefault(iid, 0)
            cat_qty[uid][cat][iid] += q

    records = []
    for uid, a in order_agg.items():
        attrs = units.get(uid, {})
        item_qty = qty.get(uid, {})
        max_q = max(item_qty.values(), default=1) or 1
        popularity = {iid: round(q / max_q, 6) for iid, q in item_qty.items()}
        top_item = {}
        for cat in CATEGORIES:
            items_in_cat = cat_qty.get(uid, {}).get(cat, {})
            if items_in_cat:
                top_item[cat] = max(items_in_cat, key=items_in_cat.get)
        records.append({
            "unit_id": int(uid),
            "metro_area": attrs.get("metro_area", "unknown"),
            "region_id": attrs.get("region_id", -1),
            "franchisee_id": attrs.get("franchisee_id", -1),
            "store_orders": a["orders"],
            "store_aov": round(a["revenue"] / a["orders"], 4) if a["orders"] else 0.0,
            "popularity": popularity,
            "top_item_per_category": top_item,
        })
    return records
