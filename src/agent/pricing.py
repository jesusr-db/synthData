"""Deterministic cart pricing. No LLM in the number path — prices come from UC.

Used to compute the indicative prices the agent shows on the propose_order confirm
card. The web BFF re-prices authoritatively at place_order (contract §3.1).
"""


def price_items(items, price_lookup, tax_rate=0.09):
    priced = []
    subtotal = 0.0
    for it in items:
        mid = int(it["menu_item_id"])
        qty = int(it.get("quantity", 1))
        unit_price = round(float(price_lookup.get(mid, 0.0)), 2)
        subtotal += unit_price * qty
        priced.append({
            "menu_item_id": mid,
            "item_name": it.get("item_name", ""),
            "quantity": qty,
            "unit_price": unit_price,
        })
    subtotal = round(subtotal, 2)
    tax_estimate = round(subtotal * tax_rate, 2)
    total = round(subtotal + tax_estimate, 2)
    return {
        "items": priced,
        "subtotal": subtotal,
        "tax_estimate": tax_estimate,
        "total": total,
        "currency": "USD",
    }
