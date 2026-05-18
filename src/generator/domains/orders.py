import random
import uuid
from datetime import datetime, timedelta
from typing import Optional
from src.generator.causal_context import CausalContext
from src.generator.entity_registry import EntityRegistry
from src.generator.demand_model import orders_for_tick, channel_for_order, tender_for_order
from src.generator.entropy import prep_time_seconds, should_breach_sos, should_cancel

_TAX_RATE = 0.085
_order_counter = 0

def _next_order_id() -> int:
    global _order_counter
    _order_counter += 1
    return _order_counter

def _build_order(ctx: CausalContext, registry: EntityRegistry,
                 order_id: int, channel: str) -> list[dict]:
    rows = []
    placed_at = ctx.timestamp + timedelta(seconds=random.randint(0, 55))
    guest_id = registry.random_guest_profile_id(ctx.unit_id)
    member_id = registry.random_member_id(guest_id)
    is_member = member_id is not None
    fp_id = registry.financial_period_for_date(placed_at.date())
    is_cancelled = should_cancel(ctx.cancellation_rate, channel)
    status = "cancelled" if is_cancelled else "fulfilled"
    late_night = ctx.hour_of_day >= 20

    num_items = random.choices([1, 2, 3, 4, 5], weights=[20, 35, 25, 15, 5])[0]
    item_rows = []
    subtotal = 0.0
    for i in range(num_items):
        menu_item = registry.random_menu_item(placed_at.hour)
        mid = menu_item["menu_item_id"]
        qty = 1 if menu_item["category"] != "drinks" else random.choice([1, 2])
        unit_price = registry.get_menu_item(mid)["base_price"]
        if channel == "3pd_delivery":
            unit_price += 0.75
        line_gross = round(unit_price * qty, 2)
        subtotal += line_gross
        item_id = order_id * 10 + i

        if is_cancelled:
            item_status = "cancelled"
        elif random.random() < 0.01:
            item_status = "refunded"
        else:
            item_status = "fulfilled"

        item_rows.append({
            "event_type": "order_item",
            "event_id": item_id,
            "event_ts": ctx.timestamp,
            "order_item_id": item_id,
            "guest_order_id": order_id,
            "unit_id": ctx.unit_id,
            "menu_item_id": mid,
            "quantity": qty,
            "unit_price": unit_price,
            "line_gross_amount": line_gross,
            "line_net_amount": line_gross,
            "line_discount_amount": 0.0,
            "item_status": item_status,
            "waste_flag": random.random() < (0.15 if is_cancelled else (0.03 if late_night else 0.02)),
            "placed_at": placed_at,
        })

    subtotal = round(subtotal, 2)
    tax = round(subtotal * _TAX_RATE, 2)
    total = round(subtotal + tax, 2)

    prep_secs = prep_time_seconds(channel)
    ready_at = placed_at + timedelta(seconds=prep_secs)
    sos_breach = should_breach_sos(ctx.sos_breach_probability)

    rows.extend(item_rows)  # always emit items (Fix 2)

    rows.append({
        "event_type": "guest_order",
        "event_id": order_id,
        "event_ts": ctx.timestamp,
        "guest_order_id": order_id,
        "unit_id": ctx.unit_id,
        "channel": channel,
        "order_type": "delivery" if "delivery" in channel else channel,
        "order_status": status,
        "profile_id": guest_id,
        "member_id": member_id,
        "subtotal": subtotal,
        "discount_amount": 0.0,
        "tax_amount": tax,
        "total_amount": total if not is_cancelled else 0.0,
        "placed_at": placed_at,
        "ready_at": ready_at if not is_cancelled else None,
        "fulfilled_at": ready_at + timedelta(seconds=random.randint(60, 300))
                        if not is_cancelled else None,
        "cancelled_at": placed_at if is_cancelled else None,
        "financial_period_id": fp_id,
        "sos_breach": sos_breach,
    })

    if not is_cancelled:
        for j, (state_from, state_to, delta_secs) in enumerate([
            ("placed", "preparing", 60),
            ("preparing", "ready", prep_secs),
            ("ready", "fulfilled", 120),
        ]):
            rows.append({
                "event_type": "status_event",
                "event_id": order_id * 10 + j,
                "event_ts": ctx.timestamp,
                "status_event_id": order_id * 10 + j,
                "guest_order_id": order_id,
                "unit_id": ctx.unit_id,
                "prior_state": state_from,
                "current_state": state_to,
                "event_timestamp": placed_at + timedelta(seconds=delta_secs),
                "elapsed_seconds_in_prior_state": delta_secs,
                "sos_target_seconds": 720 if channel == "carryout" else 1800,
                "is_sos_breach": sos_breach and state_to == "ready",
            })
        tender = tender_for_order(ctx, is_member)
        rows.append({
            "event_type": "payment",
            "event_id": order_id,
            "event_ts": ctx.timestamp,
            "payment_id": order_id,
            "guest_order_id": order_id,
            "unit_id": ctx.unit_id,
            "tender_type": tender,
            "amount": total,
            "settlement_date": placed_at.date().isoformat(),
            "paid_at": placed_at,
        })
        if "delivery" in channel:
            rows.append({
                "event_type": "delivery_order",
                "event_id": order_id,
                "event_ts": ctx.timestamp,
                "delivery_order_id": order_id,
                "guest_order_id": order_id,
                "unit_id": ctx.unit_id,
                "platform_order_reference": str(uuid.uuid4())[:8] if channel == "3pd_delivery" else None,
                "estimated_delivery_seconds": prep_secs + 900,
                "actual_delivery_seconds": prep_secs + random.randint(600, 1800),
                "delivery_status": "delivered",
            })

    return rows

def generate_orders_for_tick(ctx: CausalContext, registry: EntityRegistry,
                              tick_seconds: int = 60) -> list[dict]:
    """Generate all order-domain rows for one unit, one tick."""
    n = orders_for_tick(ctx, tick_seconds)
    rows = []
    for _ in range(n):
        order_id = _next_order_id()
        channel = channel_for_order(ctx)
        rows.extend(_build_order(ctx, registry, order_id, channel))
    return rows
