"""Single source of truth for the learned model's per-candidate feature vector.

Used by training (src/ml/train_recommender.py) and by the serving pyfunc's learned
score_fn so the feature ordering is identical in both places.
"""
from src.features.affinity import complement_score

_CATS = ["pizza", "wings", "sides", "salads", "drinks", "desserts"]
_TIERS = {"none": 0, "bronze": 1, "silver": 2, "gold": 3, "platinum": 4}

FEATURE_NAMES = (
    ["complement_score", "cust_cat_affinity", "store_popularity", "cust_aov", "store_aov", "tier_ord"]
    + [f"is_cat_{c}" for c in _CATS]
)


def build_feature_vector(cand_id, cand_cat, basket_cats, cust, store, cfg, menu):
    """Build the feature vector for a single candidate item.

    Note: 'menu' is currently unused; it is accepted for signature symmetry with the scoring hook.
    """
    comp = complement_score(basket_cats, cand_cat, cfg)
    cust_aff = float(cust.get(f"affinity_{cand_cat}", 0.0)) if cust else 0.0
    pop = float((store or {}).get("popularity", {}).get(int(cand_id), 0.0))
    cust_aov = float(cust.get("aov", 0.0)) if cust else 0.0
    s_aov = float((store or {}).get("store_aov", 0.0))
    tier_ord = float(_TIERS.get((cust or {}).get("tier", "none"), 0))
    onehot = [1.0 if cand_cat == c else 0.0 for c in _CATS]
    return [comp, cust_aff, pop, cust_aov, s_aov, tier_ord] + onehot
