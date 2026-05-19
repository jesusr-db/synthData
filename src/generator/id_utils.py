import hashlib


def make_id(*parts) -> int:
    """Deterministic, collision-resistant int ID from arbitrary string parts.

    Produces a 56-bit positive integer (fits int64, safely multiplied by 10+
    for child IDs without overflow). Same inputs always produce the same ID,
    so re-runs of the same tick window are idempotent.
    """
    key = ":".join(str(p) for p in parts)
    return int(hashlib.sha256(key.encode()).hexdigest()[:14], 16)
