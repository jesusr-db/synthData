import random
import math
from src.generator.causal_context import CausalContext
from src.generator.entropy import gaussian_noise


def _poisson(lam: float) -> int:
    """Knuth Poisson sampler — correct for sub-1 expected counts (e.g. 60s ticks)."""
    if lam <= 0:
        return 0
    L = math.exp(-lam)
    k, p = 0, 1.0
    while p > L:
        k += 1
        p *= random.random()
    return k - 1


def orders_for_tick(ctx: CausalContext, tick_seconds: int = 60) -> int:
    """Number of new orders to generate for this location in this tick."""
    tick_fraction = tick_seconds / 3600
    lam = ctx.effective_order_volume * tick_fraction * gaussian_noise(1.0, 0.15)
    return _poisson(max(0.0, lam))

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
