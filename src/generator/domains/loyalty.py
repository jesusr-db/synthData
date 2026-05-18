import random
from src.generator.causal_context import CausalContext
from src.generator.entity_registry import EntityRegistry

_loyalty_counter = 0

def _next_id() -> int:
    global _loyalty_counter
    _loyalty_counter += 1
    return _loyalty_counter

_TIER_THRESHOLDS = {"bronze": 0, "silver": 500, "gold": 1500, "platinum": 3500}

def _tier_for_spend(lifetime_spend: float) -> str:
    tier = "bronze"
    for t, threshold in _TIER_THRESHOLDS.items():
        if lifetime_spend >= threshold:
            tier = t
    return tier

def _points_multiplier(tier: str) -> float:
    return {"bronze": 1.0, "silver": 1.25, "gold": 1.5, "platinum": 2.0}[tier]

def generate_loyalty_events(ctx: CausalContext, registry: EntityRegistry,
                             order_rows: list[dict]) -> list[dict]:
    rows = []
    for order in order_rows:
        if order["event_type"] != "guest_order":
            continue
        mid = order.get("member_id")
        if not mid:
            continue
        total = order.get("total_amount", 0.0)
        if total <= 0:
            continue

        lifetime_spend = (mid * 47) % 4000
        tier = _tier_for_spend(lifetime_spend)
        multiplier = _points_multiplier(tier)
        points_earned = int(total * 10 * multiplier)

        lt_id = _next_id()
        rows.append({
            "event_type": "loyalty_transaction",
            "event_id": lt_id,
            "event_ts": ctx.timestamp,
            "loyalty_transaction_id": lt_id,
            "member_id": mid,
            "guest_order_id": order["guest_order_id"],
            "unit_id": ctx.unit_id,
            "transaction_type": "earn",
            "points_delta": points_earned,
            "transaction_at": order["placed_at"],
            "tier": tier,
        })

        if random.random() < 0.08:
            redeem_points = random.choice([100, 200, 500])
            rr_id = _next_id()
            rows.append({
                "event_type": "reward_redemption",
                "event_id": rr_id,
                "event_ts": ctx.timestamp,
                "reward_redemption_id": rr_id,
                "member_id": mid,
                "guest_order_id": order["guest_order_id"],
                "unit_id": ctx.unit_id,
                "points_redeemed": redeem_points,
                "reward_value": round(redeem_points / 100, 2),
                "redeemed_at": order["placed_at"],
            })
            dt_id = _next_id()
            rows.append({
                "event_type": "loyalty_transaction",
                "event_id": dt_id,
                "event_ts": ctx.timestamp,
                "loyalty_transaction_id": dt_id,
                "member_id": mid,
                "guest_order_id": order["guest_order_id"],
                "unit_id": ctx.unit_id,
                "transaction_type": "redeem",
                "points_delta": -redeem_points,
                "transaction_at": order["placed_at"],
                "tier": tier,
            })

    return rows
