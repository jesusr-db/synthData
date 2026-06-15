"""Pure-Python recommender scoring core (no Spark/MLflow).

Given resolved customer features, store features, the cart, the menu catalog, and
the affinity config, produce a ranked list of add-on recommendations that obeys the
endpoint contract. The trained model (recommender_model.py) can optionally override
the blended-heuristic score with a learned probability via the `score_fn` hook.
"""
from src.features.affinity import complement_score, cart_categories, is_suppressed_subcategory


def _reason(cand_cat, basket_cats, comp, cust):
    if comp >= 0.5 and basket_cats:
        anchor = next(iter(basket_cats))
        base = f"complements {anchor}"
    elif not basket_cats:
        base = "popular at this store"
    else:
        base = "frequently added"
    if cand_cat == "drinks" and basket_cats and "drinks" not in basket_cats:
        base = "complements your order; no drink in cart"
    if cust and cust.get("tier") in ("gold", "platinum"):
        base += f"; {cust['tier']}-tier favorite"
    return base


def heuristic_score(cand_id, cand_cat, basket_cats, cust, store, cfg):
    """Transparent blended score in [0, 1]: complementarity + customer affinity + store popularity."""
    comp = complement_score(basket_cats, cand_cat, cfg)
    affinity = float(cust.get(f"affinity_{cand_cat}", 0.0)) if cust else 0.0
    pop = float((store or {}).get("popularity", {}).get(cand_id, 0.0))
    if basket_cats:
        return round(0.55 * comp + 0.25 * affinity + 0.20 * pop, 6)
    # empty cart: lean on customer affinity + store popularity
    return round(0.55 * affinity + 0.45 * pop, 6)


def rank_recommendations(cart, cust, store, menu, cfg, max_results=5, score_fn=None):
    """Return ranked recommendation dicts obeying the endpoint contract.

    cart: list of menu_item_ids currently in the basket
    cust: dict of customer features, or None for cold-start
    store: dict of store features (popularity, store_aov), or None
    menu: dict menu_item_id -> (category, subcategory, name)
    cfg: affinity config (from load_affinity())
    score_fn: optional callable(cand_id, cand_cat, basket_cats, cust, store, cfg) -> float
              (the trained model injects its probability here; defaults to heuristic)
    """
    max_results = max(1, min(10, int(max_results)))
    cart = [int(c) for c in cart]
    cart_set = set(cart)
    basket_cats = cart_categories(cart, menu)
    scorer = score_fn or heuristic_score

    scored = []
    for cand_id, (cat, subcat, name) in menu.items():
        if cand_id in cart_set:
            continue
        if is_suppressed_subcategory(subcat, cart, menu, cfg):
            continue
        comp = complement_score(basket_cats, cat, cfg)
        score = scorer(cand_id, cat, basket_cats, cust, store, cfg)
        score = max(0.0, min(1.0, float(score)))
        scored.append({
            "menu_item_id": int(cand_id),
            "item_name": name,
            "category": cat,
            "subcategory": subcat,
            "score": round(score, 6),
            "reason": _reason(cat, basket_cats, comp, cust),
        })

    scored.sort(key=lambda r: (r["score"], -r["menu_item_id"]), reverse=True)
    return scored[:max_results]
