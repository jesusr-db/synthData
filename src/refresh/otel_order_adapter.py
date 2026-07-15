"""OTel Live-Order Adapter — pure reshape core (no spark / dbutils / network).

Converts flattened otel_logs + otel_spans rows into the same event-envelope
dicts that the synth generator produces, so they flow through the existing
DLT silver/gold pipeline unchanged.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Optional

from src.generator.id_utils import make_id

# Reuse the same tax rate as orders.py
_TAX_RATE = 0.085

# Synth menu_item_id range (inclusive) — matches otel span leading int
_SKU_MIN = 1
_SKU_MAX = 75

# Synth customer-key range (inclusive). guest_order.member_id / profile_id and
# customer_features.profile_id all live in [1, 50000]. A web-injected
# app.order.member_id in this range reconciles to an existing synth customer.
_MEMBER_MIN = 1
_MEMBER_MAX = 50000

# OTel channel vocab → synth channel vocab
_CHANNEL_MAP = {
    "delivery": "own_delivery",
    "carryout": "carryout",
}

# User-id prefixes that flag synthetic / load-gen noise rows
_NOISE_PREFIXES = ("fee-test", "c2-verify")

# Stage-span name → synth status-event vocabulary
_STAGE_STATE_MAP = {
    "prep":              ("preparing", False),
    "bake":              ("preparing", False),
    "qualitycheck":      ("preparing", False),
    "readyforpickup":    ("ready",     True),
    "outfordelivery":    ("ready",     True),
    "delivered":         ("fulfilled", False),
}


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def parse_skus(skus_raw: Any) -> list[tuple[int, int]]:
    """Parse otel SKU string into (menu_item_id, qty) tuples.

    Accepts string like '["41 x5","7 x10"]' or already-decoded list.
    Returns [] on garbage or None input.
    Clamps menu_item_id to [1, 75]; drops qty <= 0.
    """
    if skus_raw is None:
        return []
    raw = skus_raw if isinstance(skus_raw, str) else str(skus_raw)
    pairs = re.findall(r"(\d+)\s*[xX]\s*(\d+)", raw)
    result = []
    for mid_str, qty_str in pairs:
        mid = int(mid_str)
        qty = int(qty_str)
        if qty <= 0:
            continue
        if not (_SKU_MIN <= mid <= _SKU_MAX):
            continue
        result.append((mid, qty))
    return result


def parse_member_id(raw: Any) -> Optional[int]:
    """Parse a web-injected app.order.member_id into a synth customer key.

    Returns an int in [1, 50000] (the synth guest_order.member_id /
    customer_features.profile_id space) so real orders reconcile to an existing
    synth customer. Returns None on missing / non-numeric / out-of-range input
    (order stays anonymous, matching the pre-injection behaviour).
    """
    if raw is None:
        return None
    try:
        mid = int(str(raw).strip())
    except (ValueError, TypeError):
        return None
    if _MEMBER_MIN <= mid <= _MEMBER_MAX:
        return mid
    return None


def map_store_to_unit(
    store_id: Any,
    state: Optional[str],
    unit_ids_by_state: dict[str, list[int]],
    all_unit_ids: list[int],
) -> int:
    """Deterministically hash store_id into the synth unit_id pool.

    State-biased: uses state pool if available, otherwise all_unit_ids.
    Pure (pools are passed in, never loaded here).
    """
    pool = unit_ids_by_state.get(state or "") or all_unit_ids
    return pool[make_id("otel-store", store_id) % len(pool)]


def reshape_otel_orders(
    log_rows: list[dict],
    span_rows: list[dict],
    unit_ids_by_state: dict[str, list[int]],
    all_unit_ids: list[int],
    since_ts: Optional[datetime] = None,
) -> list[dict]:
    """Reshape otel log+span rows into synth-compatible event-envelope dicts.

    Returns a flat list of dicts with event_type / event_id / event_ts /
    source='otel' and all domain-specific columns that the DLT pipeline expects.
    Returns [] if inputs are empty or all rows are filtered.
    """
    if not log_rows and not span_rows:
        return []

    # -------------------------------------------------------------------
    # 1. Index spans
    # -------------------------------------------------------------------
    # Primary: order-tracker received order spans keyed by trace_id
    order_spans: dict[str, dict] = {}
    # Stage spans: trace_id → list of stage names (normalised lower)
    stage_spans: dict[str, list[str]] = {}

    for span in span_rows:
        trace_id = span.get("trace_id")
        if not trace_id:
            continue
        name = span.get("name", "")
        if name == "order-tracker received order":
            order_spans[trace_id] = span
        elif name.startswith("stage:"):
            raw_stage = name.split(":", 1)[1].strip().lower().replace(" ", "")
            stage_spans.setdefault(trace_id, []).append(raw_stage)

    # -------------------------------------------------------------------
    # 2. Process log rows
    # -------------------------------------------------------------------
    result: list[dict] = []

    for log in log_rows:
        # --- filter load-gen (amount None or <= 0) ---
        amount = log.get("app.order.amount")
        if amount is None or amount <= 0:
            continue

        # --- filter noise user IDs ---
        user_id = str(log.get("user.id") or "")
        if any(user_id.startswith(prefix) for prefix in _NOISE_PREFIXES):
            continue

        # --- parse event_ts ---
        raw_ts = log.get("event_ts")
        if isinstance(raw_ts, datetime):
            event_ts = raw_ts
        elif isinstance(raw_ts, str):
            event_ts = datetime.fromisoformat(raw_ts)
        else:
            # Fall back to time_unix_nano
            nano = log.get("time_unix_nano")
            if nano is None:
                continue
            event_ts = datetime.utcfromtimestamp(int(nano) / 1e9)

        # --- since_ts filter ---
        if since_ts is not None and event_ts <= since_ts:
            continue

        trace_id = log.get("trace_id", "")
        span = order_spans.get(trace_id)

        # --- derive channel & order_type ---
        span_channel = (span or {}).get("order.channel", "carryout")
        synth_channel = _CHANNEL_MAP.get(span_channel, "carryout")
        order_type = "delivery" if "delivery" in synth_channel else synth_channel

        # --- unit mapping (state from span) ---
        state = (span or {}).get("order.location.state")
        store_id = (span or {}).get("order.store_id", trace_id)
        unit_id = map_store_to_unit(store_id, state, unit_ids_by_state, all_unit_ids)

        # --- financials ---
        total_amount = round(float(amount), 2)
        subtotal = round(total_amount / (1 + _TAX_RATE), 2)
        tax_amount = round(total_amount - subtotal, 2)

        # --- IDs ---
        guest_order_id = make_id("otel", trace_id)
        payment_id = make_id("otel-pay", trace_id)

        # --- customer identity (web-injected app.order.member_id) ---
        # Synth semantics: member_id == profile_id when a customer is present,
        # both NULL when anonymous. A valid member_id (1..50000) reconciles the
        # real order to an existing synth customer / customer_features row.
        member_id = parse_member_id(log.get("app.order.member_id"))
        profile_id = member_id

        # --- SOS ---
        prep_seconds = int((span or {}).get("order.prep_seconds") or 0)
        sos_target = int((span or {}).get("sos.target_seconds") or 1800)
        sos_breach = prep_seconds > sos_target

        # -----------------------------------------------------------
        # guest_order
        # -----------------------------------------------------------
        result.append({
            "event_type": "guest_order",
            "event_id": guest_order_id,
            "event_ts": event_ts,
            "guest_order_id": guest_order_id,
            "unit_id": unit_id,
            "channel": synth_channel,
            "order_type": order_type,
            "order_status": "fulfilled",
            "profile_id": profile_id,
            "member_id": member_id,
            "subtotal": subtotal,
            "discount_amount": 0.0,
            "tax_amount": tax_amount,
            "total_amount": total_amount,
            "placed_at": event_ts,
            "ready_at": None,
            "fulfilled_at": None,
            "cancelled_at": None,
            "financial_period_id": None,
            "sos_breach": sos_breach,
            "source": "otel",
        })

        # -----------------------------------------------------------
        # order_item (from parse_skus)
        # -----------------------------------------------------------
        skus = parse_skus((span or {}).get("order.skus"))
        if skus:
            total_qty = sum(qty for _, qty in skus)
            for i, (menu_item_id, qty) in enumerate(skus):
                order_item_id = make_id("otel-item", trace_id, i)
                # Distribute subtotal proportionally by qty; floor at 0.01
                unit_price = max(0.01, round(subtotal * qty / total_qty / qty, 2))
                line_amount = max(0.01, round(subtotal * qty / total_qty, 2))
                result.append({
                    "event_type": "order_item",
                    "event_id": order_item_id,
                    "event_ts": event_ts,
                    "order_item_id": order_item_id,
                    "guest_order_id": guest_order_id,
                    "unit_id": unit_id,
                    "menu_item_id": menu_item_id,
                    "quantity": qty,
                    "unit_price": unit_price,
                    "line_gross_amount": line_amount,
                    "line_net_amount": line_amount,
                    "line_discount_amount": 0.0,
                    "item_status": "fulfilled",
                    "waste_flag": False,
                    "placed_at": event_ts,
                    "source": "otel",
                })

        # -----------------------------------------------------------
        # payment
        # -----------------------------------------------------------
        result.append({
            "event_type": "payment",
            "event_id": payment_id,
            "event_ts": event_ts,
            "payment_id": payment_id,
            "guest_order_id": guest_order_id,
            "unit_id": unit_id,
            "tender_type": "card",
            "amount": total_amount,
            "settlement_date": event_ts.date().isoformat(),
            "paid_at": event_ts,
            "source": "otel",
        })

        # -----------------------------------------------------------
        # status_event (from stage spans or synth-style triple)
        # -----------------------------------------------------------
        stages = stage_spans.get(trace_id, [])
        if stages:
            for stage_raw in stages:
                state_to, is_ready = _STAGE_STATE_MAP.get(stage_raw, ("preparing", False))
                status_event_id = make_id("otel-status", trace_id, stage_raw)
                result.append({
                    "event_type": "status_event",
                    "event_id": status_event_id,
                    "event_ts": event_ts,
                    "status_event_id": status_event_id,
                    "guest_order_id": guest_order_id,
                    "unit_id": unit_id,
                    "prior_state": "placed",
                    "current_state": state_to,
                    "event_timestamp": event_ts,
                    "elapsed_seconds_in_prior_state": prep_seconds,
                    "sos_target_seconds": sos_target,
                    "is_sos_breach": sos_breach and is_ready,
                    "source": "otel",
                })
        else:
            # Synth-style fallback triple: placed→preparing→ready→fulfilled
            for j, (prior, current) in enumerate([
                ("placed", "preparing"),
                ("preparing", "ready"),
                ("ready", "fulfilled"),
            ]):
                status_event_id = make_id("otel-status", trace_id, prior)
                result.append({
                    "event_type": "status_event",
                    "event_id": status_event_id,
                    "event_ts": event_ts,
                    "status_event_id": status_event_id,
                    "guest_order_id": guest_order_id,
                    "unit_id": unit_id,
                    "prior_state": prior,
                    "current_state": current,
                    "event_timestamp": event_ts,
                    "elapsed_seconds_in_prior_state": prep_seconds,
                    "sos_target_seconds": sos_target,
                    "is_sos_breach": sos_breach and current == "ready",
                    "source": "otel",
                })

        # -----------------------------------------------------------
        # delivery_order (only for delivery channel)
        # -----------------------------------------------------------
        if order_type == "delivery":
            delivery_order_id = make_id("otel-deliv", trace_id)
            tracking_id = log.get("app.shipping.tracking.id")
            result.append({
                "event_type": "delivery_order",
                "event_id": delivery_order_id,
                "event_ts": event_ts,
                "delivery_order_id": delivery_order_id,
                "guest_order_id": guest_order_id,
                "unit_id": unit_id,
                "platform_order_reference": tracking_id,
                "estimated_delivery_seconds": prep_seconds + 900,
                "actual_delivery_seconds": prep_seconds + 900,
                "delivery_status": "delivered",
                "source": "otel",
            })

    return result
