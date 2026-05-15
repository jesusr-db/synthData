import random
from src.generator.causal_context import CausalContext
from src.generator.entropy import gaussian_noise

def orders_for_tick(ctx: CausalContext, tick_seconds: int = 60) -> int:
    """Number of new orders to generate for this location in this tick."""
    tick_fraction = tick_seconds / 3600
    raw = ctx.effective_order_volume * tick_fraction
    noisy = raw * gaussian_noise(1.0, 0.15)
    return max(0, round(noisy))

def channel_for_order(ctx: CausalContext) -> str:
    """Sample a channel from the causal context channel mix."""
    r = random.random()
    cumulative = 0.0
    for channel, weight in ctx.channel_mix.items():
        cumulative += weight
        if r < cumulative:
            return channel
    return "carryout"

def tender_for_order(ctx: CausalContext, is_loyalty_member: bool) -> str:
    """Sample a tender type; loyalty members skew toward loyalty_redemption."""
    mix = dict(ctx.tender_mix)
    if is_loyalty_member:
        mix["loyalty_redemption"] = min(0.30, mix["loyalty_redemption"] * 2)
        # renormalize
        total = sum(mix.values())
        mix = {k: v / total for k, v in mix.items()}
    r = random.random()
    cumulative = 0.0
    for tender, weight in mix.items():
        cumulative += weight
        if r < cumulative:
            return tender
    return "credit_card"
