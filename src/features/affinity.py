"""Curated basket-affinity matrix: loader + pure scoring helpers.

The generator draws basket items independently, so item co-occurrence carries
little signal. This module supplies a curated category-complementarity prior used
both as a training feature and as a serving-time signal.
"""
from pathlib import Path
from typing import Iterable


def load_affinity(path: str | None = None) -> dict:
    """Load conf/basket_affinity.yml. Defaults to conf/ relative to project root."""
    import yaml

    if path is None:
        path = Path(__file__).parent.parent.parent / "conf" / "basket_affinity.yml"
    with open(path) as f:
        return yaml.safe_load(f)


def complement_score(basket_cats: Iterable[str], candidate_cat: str, cfg: dict) -> float:
    """Mean complement weight of candidate_cat given the categories in the basket.

    Returns 0.0 for an empty basket. Result is clamped to [0, 1].
    """
    basket_cats = list(basket_cats)
    if not basket_cats:
        return 0.0
    comp = cfg["complements"]
    weights = [comp.get(bc, {}).get(candidate_cat, 0.0) for bc in basket_cats]
    score = sum(weights) / len(weights)
    return max(0.0, min(1.0, score))


def cart_categories(cart_item_ids: Iterable[int], menu: dict) -> set[str]:
    """Map cart menu_item_ids -> the set of categories present.

    `menu` maps menu_item_id -> (category, subcategory, name).
    Unknown ids are ignored.
    """
    cats = set()
    for iid in cart_item_ids:
        row = menu.get(int(iid))
        if row:
            cats.add(row[0])
    return cats


def is_suppressed_subcategory(candidate_subcat: str, cart_item_ids, menu: dict, cfg: dict) -> bool:
    """True if candidate_subcat is in the suppression list AND the cart already
    contains an item of that subcategory."""
    suppress = set(cfg.get("suppress_duplicate_subcategories", []))
    if candidate_subcat not in suppress:
        return False
    for iid in cart_item_ids:
        row = menu.get(int(iid))
        if row and row[1] == candidate_subcat:
            return True
    return False
