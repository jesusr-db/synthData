# Customer Feature Store + Basket-Aware Recommendation Model — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a customer feature store (UC feature tables → Online Tables → real-time Feature Serving) and a basket-aware recommendation model (MLflow pyfunc → Model Serving) to the QSR synth-data project, wired into the existing setup/destroy job DAG, exposing one endpoint that PizzaTel calls with `customer_id` + `store_id` + `cart_item_ids` and gets back ranked add-on items.

**Architecture:** Offline feature tables in a new `${schema_prefix}features` schema are computed from existing silver/ref tables and synced to **Online Tables** for low-latency lookup. A **Feature Serving endpoint** exposes the customer/store features directly (fold #1: "real-time customer look"). A **Model Serving endpoint** hosts an MLflow pyfunc recommender (fold #2) that does automatic feature lookup by `customer_id`/`store_id`, derives basket signals from the cart using a curated category-affinity matrix, scores all candidate menu items, suppresses items already in the cart, and returns ranked recommendations. The generator is **not modified** — complementarity comes from a curated `conf/basket_affinity.yml` matrix used as both a training feature and a serving signal, because the generator draws basket items independently. Everything is created by the setup job and torn down by the destroy job (CLAUDE.md automation standard). Feature tables refresh **weekly**; serving is **always live**.

**Tech Stack:** Python 3.11, PySpark (serverless), Databricks Feature Engineering (`databricks-feature-engineering`), MLflow (pyfunc + UC model registry), scikit-learn (GradientBoostingClassifier), Databricks SDK (`databricks-sdk`), Online Tables, Feature Serving + Model Serving endpoints, Databricks Asset Bundles, PyYAML, pytest.

---

## The Endpoint Contract (interface spec — reconciled with the website team, v2)

This is the canonical request/response the Model Serving endpoint accepts. It matches the website team's draft at `opentelemetry-demo/docs/integration/recommendation-endpoint-contract.md` (field names + **integer** types adopted from their version). PizzaTel is wired against this (PizzaTel changes are out of scope here — owned by the user). Because PizzaTel `product_id == str(menu_item_id)`, no translation is needed.

**Request** (standard Model Serving `dataframe_records` envelope; the website sends one record per call):

```json
{
  "dataframe_records": [
    {
      "profile_id": 1234,
      "member_id": 5678,
      "store_id": 42,
      "cart_product_ids": [1, 14],
      "viewed_product_id": 8,
      "num_recommendations": 5
    }
  ]
}
```

- `profile_id` (bigint, required): synthData guest profile id. Website sends a default guest id for anonymous sessions; an id not present in the online customer table ⇒ **cold-start** (store-popularity path, `personalized:false`).
- `member_id` (bigint, nullable): loyalty member id. Accepted; in this dataset tier already derives from the profile, so v1 only uses it as a fallback lookup when `profile_id` is absent.
- `store_id` (bigint, required): synthData `unit_id`. Unknown ⇒ global-popularity fallback.
- `cart_product_ids` (array<bigint>, required, may be empty): `menu_item_id`s in the live cart (== PizzaTel product IDs).
- `viewed_product_id` (bigint, nullable): item being viewed on a product page. Folded into the complementarity signal and excluded from results; ignored if null.
- `num_recommendations` (int, optional, default 5): clamped 1–10.

**Types:** all IDs are **integers** in and out. The website sends ints and `str()`s the returned `menu_item_id`s for its catalog/UI.

**Response** (standard Model Serving `predictions` envelope; one element per input row — MLflow-correct for N-row inputs):

```json
{
  "predictions": [
    {
      "personalized": true,
      "recommendations": [
        {"menu_item_id": 53, "score": 0.94, "item_name": "20oz Coca-Cola", "category": "drinks", "subcategory": "soda", "reason": "complements pizza; no drink in cart"},
        {"menu_item_id": 61, "score": 0.81, "item_name": "Garlic Dipping Cup", "category": "sides", "subcategory": "dip", "reason": "pairs with wings"},
        {"menu_item_id": 70, "score": 0.62, "item_name": "Chocolate Lava Cake", "category": "desserts", "subcategory": "cake", "reason": "gold-tier add-on"}
      ]
    }
  ]
}
```

- Website reads `predictions[0].recommendations[*].menu_item_id` (+ `score`); extra fields (`item_name`/`category`/`subcategory`/`reason`/`personalized`) are optional to consume.
- `personalized` is `true` when customer features were found, `false` on cold-start.
- `recommendations` excludes any `menu_item_id` in `cart_product_ids` (and `viewed_product_id`), is sorted by `score` descending, and has length ≤ `num_recommendations`.
- **Alternative shape on request:** if the website prefers the bare flat list from their draft (`predictions: [{menu_item_id, score}, ...]`), the pyfunc can emit that instead at low cost — but the row-object form above is recommended because it carries the cold-start flag and is correct for multi-row inputs.

### Model-team answers to the website's checklist
- **Endpoint name / URL:** `synth_qsr-recommender` (tracks `schema_prefix`) → `https://<jmrdemo-host>/serving-endpoints/synth_qsr-recommender/invocations`. Optional direct feature look: Feature Serving endpoint `synth_qsr-customer-features`.
- **Auth:** PAT/principal with `CAN_QUERY` (Task 8 Step 4 grants it — requires the website principal identity as a deploy input `recommender_query_principal`). OAuth M2M later.
- **Online feature lookup:** confirmed (FE `FeatureLookup` over Online Tables).
- **Entity keys:** `profile_id` → customer feature table PK values; `store_id` → store feature table PK values; `member_id` optional. (Confirm the literal PK column name in `synth_silver.guest_profile` — values align regardless.)
- **Item-ID type:** `menu_item_id` int in/out — confirmed.
- **Latency/cold-start:** warm p50 ~ low-hundreds ms; first hit after idle (scale-to-zero) a few seconds. For demos keep warm (scale-to-zero off) or set ~5s client timeout + flagd fallback. Cold-start ⇒ `personalized:false` + store-popular items.

---

## File Structure

**New files:**
- `conf/basket_affinity.yml` — curated category→category complement weights + same-subcategory suppression list.
- `src/features/__init__.py` — package marker.
- `src/features/affinity.py` — loads `basket_affinity.yml`; pure-python helpers for complement scoring and cart-category extraction.
- `src/features/customer_features.py` — pure transform: silver rows → customer feature records.
- `src/features/store_features.py` — pure transform: silver rows → store feature records.
- `src/ml/__init__.py` — package marker.
- `src/ml/scoring.py` — pure-python recommender scoring core (no Spark/MLflow); the ranking + cart-suppression logic lives here and is fully unit-tested.
- `src/ml/recommender_model.py` — `mlflow.pyfunc.PythonModel` wrapper that calls `scoring.py` with looked-up + on-demand features.
- `src/setup/build_feature_tables.py` — notebook: compute + write UC feature tables, create/refresh Online Tables, create Feature Serving endpoint.
- `src/ml/train_recommender.py` — notebook: build training set via FeatureLookup, train sklearn model, log via `fe.log_model`, register in UC, create/update Model Serving endpoint.
- `src/ml/demo_client.py` — notebook: example cart → recommendations round-trip against the live endpoint; documents the contract.
- `resources/feature_refresh_job.yml` — weekly scheduled job that re-runs `build_feature_tables.py`.
- `tests/test_affinity.py`, `tests/test_customer_features.py`, `tests/test_store_features.py`, `tests/test_scoring.py`, `tests/test_recommender_model.py` — hermetic unit tests.

**Modified files:**
- `databricks.yml` — add feature-store/ML variables.
- `resources/setup_job.yml` — insert `build_feature_tables`, `train_recommender` tasks into the DAG.
- `src/setup/destroy_notebook.py` — tear down endpoints, online tables, registered model, feature tables, features schema.
- `docs/` (architecture/gotchas/quickstart, whatever the living-docs set is) — document the new feature store, endpoints, and contract.

**Schemas/objects created at runtime:**
- Schema `${catalog}.${schema_prefix}features` holding `customer_features`, `store_features`.
- Online tables `customer_features_online`, `store_features_online`.
- Feature Serving endpoint `${schema_prefix}qsr-customer-features`.
- UC model `${catalog}.${schema_prefix}features.qsr_recommender`.
- Model Serving endpoint `${schema_prefix}qsr-recommender`.

---

## Conventions (match existing code exactly)

- **Param reading in notebooks** (from `setup_notebook.py:12-24`):

```python
try:
    catalog_name = dbutils.widgets.get("catalog_name")
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    catalog_name = "jmrdemo"
    schema_prefix = "synth_"
```

- **Full table names:** `f"{catalog_name}.{schema_prefix}silver.guest_order"`, `f"{catalog_name}.{schema_prefix}features.customer_features"`, etc.
- **Serverless token retrieval** (from `apply_ontos.py:40-44`):

```python
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
token = _ctx.apiToken().get()
```

- **Best-effort teardown** with `[WARN]` (from `destroy_notebook.py:28-36`): wrap each delete in `try/except` and print `[WARN] ... skipped: {e}`.
- **Config loader pattern** (from `src/refresh/multiplier_engine.py:4-11`): resolve path via `Path(__file__).parent.parent.parent / "conf" / ...`.
- **Tests are hermetic** (pure Python, no Spark/Databricks). Run all tests with `pytest -q`. Baseline is **120 tests**; this plan adds tests and the count must only grow.

---

## Pre-flight (do once before Task 1)

- [ ] **Create the feature branch** (CLAUDE.md: any change touching 2+ files goes on a branch):

```bash
cd /Users/jesus.rodriguez/Documents/ItsAVibe/gitrepos_FY27/synthData
git checkout main && git pull
git checkout -b feat/customer-feature-store-recommender
```

- [ ] **Confirm baseline tests pass:**

Run: `pytest -q`
Expected: `120 passed` (or more), in ~1.5s.

---

## Phase 1 — Curated affinity config + offline feature transforms

### Task 1: Curated basket-affinity config + loader

**Files:**
- Create: `conf/basket_affinity.yml`
- Create: `src/features/__init__.py`
- Create: `src/features/affinity.py`
- Test: `tests/test_affinity.py`

- [ ] **Step 1: Write the config file**

Create `conf/basket_affinity.yml`:

```yaml
# Curated category complementarity weights for QSR baskets.
# The generator draws basket items independently, so co-occurrence signal is weak;
# this matrix supplies the "what goes with what" prior used as a model feature AND
# a serving-time signal. Weight = strength of "if basket has <row>, recommend <col>".
# Categories must match ref.menu_item.category values:
#   pizza, wings, sides, salads, drinks, desserts
complements:
  pizza:    {drinks: 0.9, sides: 0.7, desserts: 0.5, wings: 0.4, salads: 0.2}
  wings:    {drinks: 0.8, sides: 0.8, desserts: 0.3, pizza: 0.4, salads: 0.2}
  sides:    {drinks: 0.7, pizza: 0.4, desserts: 0.3, wings: 0.3, salads: 0.1}
  salads:   {drinks: 0.7, pizza: 0.3, sides: 0.2, desserts: 0.2, wings: 0.1}
  drinks:   {pizza: 0.3, desserts: 0.4, sides: 0.3, wings: 0.2, salads: 0.1}
  desserts: {drinks: 0.6, pizza: 0.2, sides: 0.1, wings: 0.1, salads: 0.1}

# Empty-cart default: which categories to surface when the cart is empty,
# in priority order (anchor items first).
empty_cart_priority: [pizza, wings, salads, sides, drinks, desserts]

# Same-subcategory suppression: if the cart already contains an item whose
# subcategory is listed here, do not recommend another item of that subcategory.
suppress_duplicate_subcategories: [soda, water, pepperoni, cheese, boneless, traditional]
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_affinity.py`:

```python
from src.features.affinity import (
    load_affinity,
    complement_score,
    cart_categories,
    is_suppressed_subcategory,
)


def test_load_affinity_has_all_categories():
    cfg = load_affinity()
    cats = {"pizza", "wings", "sides", "salads", "drinks", "desserts"}
    assert set(cfg["complements"].keys()) == cats


def test_complement_score_pizza_pulls_drinks_strongly():
    cfg = load_affinity()
    # basket of pizza -> drinks should outscore pizza -> salads
    drink = complement_score(["pizza"], "drinks", cfg)
    salad = complement_score(["pizza"], "salads", cfg)
    assert drink > salad
    assert 0.0 <= drink <= 1.0


def test_complement_score_aggregates_over_basket_categories():
    cfg = load_affinity()
    # pizza+wings should pull sides at least as hard as pizza alone
    only_pizza = complement_score(["pizza"], "sides", cfg)
    pizza_wings = complement_score(["pizza", "wings"], "sides", cfg)
    assert pizza_wings >= only_pizza


def test_complement_score_empty_cart_is_zero():
    cfg = load_affinity()
    assert complement_score([], "drinks", cfg) == 0.0


def test_cart_categories_maps_item_ids_to_categories():
    # menu lookup: id -> (category, subcategory, name)
    menu = {1: ("pizza", "pepperoni", "Large Pepperoni"),
            53: ("drinks", "soda", "20oz Coca-Cola")}
    assert cart_categories([1, 53], menu) == {"pizza", "drinks"}


def test_is_suppressed_subcategory_true_for_soda_when_soda_in_cart():
    cfg = load_affinity()
    menu = {53: ("drinks", "soda", "20oz Coke")}
    assert is_suppressed_subcategory("soda", [53], menu, cfg) is True


def test_is_suppressed_subcategory_false_for_dessert():
    cfg = load_affinity()
    menu = {53: ("drinks", "soda", "20oz Coke")}
    assert is_suppressed_subcategory("cake", [53], menu, cfg) is False
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `pytest tests/test_affinity.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.features'`.

- [ ] **Step 4: Implement the loader + helpers**

Create empty `src/features/__init__.py`.

Create `src/features/affinity.py`:

```python
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
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `pytest tests/test_affinity.py -q`
Expected: PASS (7 passed).

- [ ] **Step 6: Commit**

```bash
git add conf/basket_affinity.yml src/features/__init__.py src/features/affinity.py tests/test_affinity.py
git commit -m "feat: curated basket-affinity config + scoring helpers"
```

---

### Task 2: Customer feature transform (pure Python)

This computes per-`guest_profile_id` features from already-collected silver rows. It takes plain Python dict rows so it is hermetically testable; the Spark wiring (Task 4) just feeds it `df.collect()`-style dicts and writes the result.

**Files:**
- Create: `src/features/customer_features.py`
- Test: `tests/test_customer_features.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_customer_features.py`:

```python
from datetime import datetime
from src.features.customer_features import compute_customer_features

# menu: id -> (category, subcategory, name)
MENU = {
    1: ("pizza", "pepperoni", "Large Pepperoni"),
    30: ("wings", "boneless", "8pc Boneless"),
    53: ("drinks", "soda", "20oz Coke"),
}

ORDERS = [
    # guest_profile_id, guest_order_id, total_amount, placed_at
    {"profile_id": 10, "guest_order_id": 100, "total_amount": 25.0, "placed_at": datetime(2026, 6, 1, 19, 0)},
    {"profile_id": 10, "guest_order_id": 101, "total_amount": 15.0, "placed_at": datetime(2026, 6, 8, 12, 0)},
]
ITEMS = [
    # guest_order_id, menu_item_id, quantity, line_net_amount
    {"guest_order_id": 100, "menu_item_id": 1, "quantity": 1, "line_net_amount": 18.0},
    {"guest_order_id": 100, "menu_item_id": 53, "quantity": 1, "line_net_amount": 2.0},
    {"guest_order_id": 101, "menu_item_id": 30, "quantity": 1, "line_net_amount": 9.0},
]
TIERS = {10: "gold"}  # member_id/profile_id -> tier (latest)
AS_OF = datetime(2026, 6, 14)


def test_computes_one_record_per_customer():
    recs = compute_customer_features(ORDERS, ITEMS, TIERS, MENU, as_of=AS_OF)
    assert len(recs) == 1
    assert recs[0]["guest_profile_id"] == 10


def test_rfm_fields():
    rec = compute_customer_features(ORDERS, ITEMS, TIERS, MENU, as_of=AS_OF)[0]
    assert rec["total_orders"] == 2
    assert rec["recency_days"] == 6  # last order 2026-06-08 -> 2026-06-14
    assert abs(rec["monetary_total"] - 40.0) < 1e-6
    assert abs(rec["aov"] - 20.0) < 1e-6


def test_tier_and_category_affinity():
    rec = compute_customer_features(ORDERS, ITEMS, TIERS, MENU, as_of=AS_OF)[0]
    assert rec["tier"] == "gold"
    # spend: pizza 18, drinks 2, wings 9 -> total 29
    assert abs(rec["affinity_pizza"] - (18.0 / 29.0)) < 1e-6
    assert abs(rec["affinity_wings"] - (9.0 / 29.0)) < 1e-6
    assert abs(rec["affinity_drinks"] - (2.0 / 29.0)) < 1e-6
    assert rec["affinity_desserts"] == 0.0


def test_unknown_tier_defaults_to_none_string():
    rec = compute_customer_features(ORDERS, ITEMS, {}, MENU, as_of=AS_OF)[0]
    assert rec["tier"] == "none"


def test_skips_orders_with_null_profile():
    orders = ORDERS + [{"profile_id": None, "guest_order_id": 200, "total_amount": 9.0,
                        "placed_at": datetime(2026, 6, 10)}]
    recs = compute_customer_features(orders, ITEMS, TIERS, MENU, as_of=AS_OF)
    assert {r["guest_profile_id"] for r in recs} == {10}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/test_customer_features.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.features.customer_features'`.

- [ ] **Step 3: Implement the transform**

Create `src/features/customer_features.py`:

```python
"""Pure-Python customer feature computation (no Spark).

Inputs are lists of dict rows (one per silver row) so the logic is hermetically
testable. The Spark notebook in src/setup/build_feature_tables.py converts
DataFrame rows to these dicts and writes the result to a UC feature table.
"""
from datetime import datetime

CATEGORIES = ["pizza", "wings", "sides", "salads", "drinks", "desserts"]


def compute_customer_features(orders, items, tiers, menu, as_of: datetime):
    """Return one feature record per non-null guest_profile_id.

    orders: rows with profile_id, guest_order_id, total_amount, placed_at
    items:  rows with guest_order_id, menu_item_id, quantity, line_net_amount
    tiers:  dict profile_id -> tier string (latest known)
    menu:   dict menu_item_id -> (category, subcategory, name)
    """
    # Map order -> owning profile (skip anonymous orders).
    order_owner = {o["guest_order_id"]: o["profile_id"] for o in orders if o.get("profile_id") is not None}

    # Per-profile aggregates.
    agg: dict[int, dict] = {}
    for o in orders:
        pid = o.get("profile_id")
        if pid is None:
            continue
        a = agg.setdefault(pid, {"orders": 0, "monetary": 0.0, "last": None,
                                 "cat_spend": {c: 0.0 for c in CATEGORIES}})
        a["orders"] += 1
        a["monetary"] += float(o.get("total_amount") or 0.0)
        placed = o.get("placed_at")
        if placed is not None and (a["last"] is None or placed > a["last"]):
            a["last"] = placed

    for it in items:
        pid = order_owner.get(it["guest_order_id"])
        if pid is None:
            continue
        row = menu.get(int(it["menu_item_id"]))
        if not row:
            continue
        cat = row[0]
        if cat in agg[pid]["cat_spend"]:
            agg[pid]["cat_spend"][cat] += float(it.get("line_net_amount") or 0.0)

    records = []
    for pid, a in agg.items():
        total_cat = sum(a["cat_spend"].values())
        rec = {
            "guest_profile_id": int(pid),
            "total_orders": a["orders"],
            "monetary_total": round(a["monetary"], 4),
            "aov": round(a["monetary"] / a["orders"], 4) if a["orders"] else 0.0,
            "recency_days": (as_of - a["last"]).days if a["last"] else -1,
            "tier": tiers.get(pid, "none"),
        }
        for c in CATEGORIES:
            rec[f"affinity_{c}"] = round(a["cat_spend"][c] / total_cat, 6) if total_cat else 0.0
        records.append(rec)
    return records
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest tests/test_customer_features.py -q`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
git add src/features/customer_features.py tests/test_customer_features.py
git commit -m "feat: customer feature transform (RFM + tier + category affinity)"
```

---

### Task 3: Store feature transform (pure Python)

**Files:**
- Create: `src/features/store_features.py`
- Test: `tests/test_store_features.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_store_features.py`:

```python
from src.features.store_features import compute_store_features

MENU = {
    1: ("pizza", "pepperoni", "Large Pepperoni"),
    30: ("wings", "boneless", "8pc Boneless"),
    53: ("drinks", "soda", "20oz Coke"),
}
# units: unit_id -> attributes
UNITS = {
    42: {"metro_area": "New York-Newark", "region_id": 3, "franchisee_id": 7},
}
ORDERS = [
    {"guest_order_id": 100, "unit_id": 42, "total_amount": 25.0},
    {"guest_order_id": 101, "unit_id": 42, "total_amount": 15.0},
]
ITEMS = [
    {"guest_order_id": 100, "unit_id": 42, "menu_item_id": 1, "quantity": 2, "line_net_amount": 36.0},
    {"guest_order_id": 100, "unit_id": 42, "menu_item_id": 53, "quantity": 1, "line_net_amount": 2.0},
    {"guest_order_id": 101, "unit_id": 42, "menu_item_id": 1, "quantity": 1, "line_net_amount": 18.0},
]


def test_one_record_per_store_with_unit_attrs():
    recs = compute_store_features(ORDERS, ITEMS, UNITS, MENU)
    assert len(recs) == 1
    r = recs[0]
    assert r["unit_id"] == 42
    assert r["metro_area"] == "New York-Newark"
    assert r["region_id"] == 3
    assert r["franchisee_id"] == 7


def test_store_aov_and_popularity():
    r = compute_store_features(ORDERS, ITEMS, UNITS, MENU)[0]
    assert abs(r["store_aov"] - 20.0) < 1e-6  # (25+15)/2
    # item 1 ordered qty 3, item 53 qty 1 -> popularity normalized by max qty
    assert r["popularity"][1] == 1.0
    assert abs(r["popularity"][53] - (1 / 3)) < 1e-6


def test_top_item_per_category():
    r = compute_store_features(ORDERS, ITEMS, UNITS, MENU)[0]
    assert r["top_item_per_category"]["pizza"] == 1
    assert r["top_item_per_category"]["drinks"] == 53


def test_unknown_unit_attrs_default():
    recs = compute_store_features(ORDERS, ITEMS, {}, MENU)
    r = recs[0]
    assert r["metro_area"] == "unknown"
    assert r["region_id"] == -1
    assert r["franchisee_id"] == -1
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/test_store_features.py -q`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Implement the transform**

Create `src/features/store_features.py`:

```python
"""Pure-Python store feature computation (no Spark)."""
from src.features.customer_features import CATEGORIES


def compute_store_features(orders, items, units, menu):
    """Return one feature record per unit_id.

    orders: rows with guest_order_id, unit_id, total_amount
    items:  rows with guest_order_id, unit_id, menu_item_id, quantity, line_net_amount
    units:  dict unit_id -> {metro_area, region_id, franchisee_id}
    menu:   dict menu_item_id -> (category, subcategory, name)
    """
    order_agg: dict[int, dict] = {}
    for o in orders:
        uid = o["unit_id"]
        a = order_agg.setdefault(uid, {"orders": 0, "revenue": 0.0})
        a["orders"] += 1
        a["revenue"] += float(o.get("total_amount") or 0.0)

    qty: dict[int, dict[int, int]] = {}     # unit -> item -> qty
    cat_qty: dict[int, dict[str, dict]] = {}  # unit -> cat -> {item: qty}
    for it in items:
        uid = it["unit_id"]
        iid = int(it["menu_item_id"])
        q = int(it.get("quantity") or 0)
        qty.setdefault(uid, {}).setdefault(iid, 0)
        qty[uid][iid] += q
        row = menu.get(iid)
        if row:
            cat = row[0]
            cat_qty.setdefault(uid, {}).setdefault(cat, {}).setdefault(iid, 0)
            cat_qty[uid][cat][iid] += q

    records = []
    for uid, a in order_agg.items():
        attrs = units.get(uid, {})
        item_qty = qty.get(uid, {})
        max_q = max(item_qty.values()) if item_qty else 1
        popularity = {iid: round(q / max_q, 6) for iid, q in item_qty.items()}
        top_item = {}
        for cat in CATEGORIES:
            items_in_cat = cat_qty.get(uid, {}).get(cat, {})
            if items_in_cat:
                top_item[cat] = max(items_in_cat, key=items_in_cat.get)
        records.append({
            "unit_id": int(uid),
            "metro_area": attrs.get("metro_area", "unknown"),
            "region_id": attrs.get("region_id", -1),
            "franchisee_id": attrs.get("franchisee_id", -1),
            "store_orders": a["orders"],
            "store_aov": round(a["revenue"] / a["orders"], 4) if a["orders"] else 0.0,
            "popularity": popularity,
            "top_item_per_category": top_item,
        })
    return records
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest tests/test_store_features.py -q`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add src/features/store_features.py tests/test_store_features.py
git commit -m "feat: store feature transform (aov + popularity + top item per category)"
```

> **Note on serving shape:** `popularity` and `top_item_per_category` are dict-typed and convenient for the pure transform. When written to the UC feature table (Task 4) they are stored as `MAP<INT,DOUBLE>` / `MAP<STRING,INT>` so the online table can serve them.

---

## Phase 2 — Recommender scoring core + model

### Task 4: Build & sync feature tables notebook

**Files:**
- Create: `src/setup/build_feature_tables.py`

> This task has no hermetic unit test (it requires Spark + a workspace). It is verified by `databricks bundle validate` (Task 11) and the live setup run / demo notebook (Task 10). Keep all transform logic in the Task 2/3 pure modules; this notebook is thin glue.

- [ ] **Step 1: Write the notebook**

Create `src/setup/build_feature_tables.py`:

```python
# Databricks notebook source
# Build customer & store feature tables, sync to Online Tables, expose a Feature Serving endpoint.
# Reads silver/ref tables, computes features via the pure transforms in src.features.*,
# writes UC Delta feature tables, and (re)creates online tables + a feature serving endpoint.
from datetime import datetime, timezone

try:
    catalog_name = dbutils.widgets.get("catalog_name")
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    catalog_name = "jmrdemo"
    schema_prefix = "synth_"

features_schema = f"{schema_prefix}features"
fq = lambda t: f"{catalog_name}.{features_schema}.{t}"  # noqa: E731

from src.features.customer_features import compute_customer_features, CATEGORIES
from src.features.store_features import compute_store_features

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{features_schema}")

# --- Load source rows (collect to driver; synthetic dataset is small) ---
sp = f"{catalog_name}.{schema_prefix}"
menu_rows = spark.read.table(f"{sp}ref.menu_item").select(
    "menu_item_id", "category", "subcategory", "item_name").collect()
menu = {int(r["menu_item_id"]): (r["category"], r["subcategory"], r["item_name"]) for r in menu_rows}

unit_rows = spark.read.table(f"{sp}ref.unit").select(
    "unit_id", "metro_area", "region_id", "franchisee_id").collect()
units = {int(r["unit_id"]): {"metro_area": r["metro_area"], "region_id": r["region_id"],
                             "franchisee_id": r["franchisee_id"]} for r in unit_rows}

orders = [r.asDict() for r in spark.read.table(f"{sp}silver.guest_order").select(
    "guest_order_id", "unit_id", "profile_id", "total_amount", "placed_at").collect()]
items = [r.asDict() for r in spark.read.table(f"{sp}silver.order_item").select(
    "guest_order_id", "unit_id", "menu_item_id", "quantity", "line_net_amount").collect()]

# latest tier per profile from loyalty_transaction
tier_rows = spark.sql(f"""
    SELECT member_id, tier FROM (
      SELECT member_id, tier,
             ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY transaction_at DESC) rn
      FROM {sp}silver.loyalty_transaction WHERE member_id IS NOT NULL
    ) WHERE rn = 1
""").collect()
tiers = {int(r["member_id"]): r["tier"] for r in tier_rows}

as_of = datetime.now(timezone.utc).replace(tzinfo=None)
cust = compute_customer_features(orders, items, tiers, menu, as_of=as_of)
store = compute_store_features(orders, items, units, menu)
print(f"[INFO] computed {len(cust)} customer rows, {len(store)} store rows")

# --- Write UC feature tables via Feature Engineering ---
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()

import pandas as pd
cust_pdf = pd.DataFrame(cust)
cust_sdf = spark.createDataFrame(cust_pdf)
store_pdf = pd.DataFrame([{
    **{k: v for k, v in s.items() if k not in ("popularity", "top_item_per_category")},
    "popularity": {int(k): float(v) for k, v in s["popularity"].items()},
    "top_item_per_category": {k: int(v) for k, v in s["top_item_per_category"].items()},
} for s in store])
store_sdf = spark.createDataFrame(store_pdf)

for name, sdf, pk in [("customer_features", cust_sdf, "guest_profile_id"),
                      ("store_features", store_sdf, "unit_id")]:
    table = fq(name)
    try:
        fe.create_table(name=table, primary_keys=pk, df=sdf,
                        description=f"QSR {name} for personalization")
        print(f"[INFO] created feature table {table}")
    except Exception as e:
        print(f"[INFO] feature table {table} exists, writing merge: {e}")
        fe.write_table(name=table, df=sdf, mode="merge")

# --- Online tables (idempotent) ---
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    OnlineTable, OnlineTableSpec, OnlineTableSpecTriggeredSchedulingPolicy)
w = WorkspaceClient()

def ensure_online_table(source_table: str, online_name: str, pk: str):
    online_fq = fq(online_name)
    spec = OnlineTableSpec(
        source_table_full_name=source_table,
        primary_key_columns=[pk],
        run_triggered=OnlineTableSpecTriggeredSchedulingPolicy.from_dict({"triggered": "true"}),
        perform_full_copy=True,
    )
    try:
        w.online_tables.create(table=OnlineTable(name=online_fq, spec=spec))
        print(f"[INFO] created online table {online_fq}")
    except Exception as e:
        # already exists -> trigger a refresh to pick up new feature values
        print(f"[INFO] online table {online_fq} exists ({e}); triggering pipeline refresh")
        try:
            ot = w.online_tables.get(name=online_fq)
            if ot.spec and ot.spec.pipeline_id:
                w.pipelines.start_update(pipeline_id=ot.spec.pipeline_id, full_refresh=True)
        except Exception as e2:
            print(f"[WARN] online refresh skipped: {e2}")

ensure_online_table(fq("customer_features"), "customer_features_online", "guest_profile_id")
ensure_online_table(fq("store_features"), "store_features_online", "unit_id")

# --- Feature Serving endpoint (fold #1: real-time customer look) ---
from databricks.feature_engineering import FeatureLookup
from databricks.feature_engineering.entities.feature_serving_endpoint import (
    EndpointCoreConfig, ServedEntity)

spec_name = fq("customer_store_spec")
features = [
    FeatureLookup(table_name=fq("customer_features"), lookup_key="profile_id",
                  feature_names=["tier", "aov", "recency_days", "total_orders"]
                  + [f"affinity_{c}" for c in CATEGORIES]),
    FeatureLookup(table_name=fq("store_features"), lookup_key="store_id",
                  feature_names=["metro_area", "store_aov", "store_orders"]),
]
try:
    fe.create_feature_spec(name=spec_name, features=features)
    print(f"[INFO] created feature spec {spec_name}")
except Exception as e:
    print(f"[INFO] feature spec exists: {e}")

fs_endpoint = f"{schema_prefix}qsr-customer-features"
try:
    fe.create_feature_serving_endpoint(
        name=fs_endpoint,
        config=EndpointCoreConfig(served_entities=ServedEntity(
            feature_spec_name=spec_name, scale_to_zero_enabled=True)))
    print(f"[INFO] created feature serving endpoint {fs_endpoint}")
except Exception as e:
    print(f"[INFO] feature serving endpoint exists: {e}")

print("[DONE] build_feature_tables complete")
```

- [ ] **Step 2: Syntax-check the notebook**

Run: `python -c "import ast; ast.parse(open('src/setup/build_feature_tables.py').read())"`
Expected: no output (parses cleanly).

- [ ] **Step 3: Commit**

```bash
git add src/setup/build_feature_tables.py
git commit -m "feat: build_feature_tables notebook (UC feature tables + online tables + feature serving)"
```

---

### Task 5: Recommender scoring core (pure Python)

This is the heart of the recommender and is fully unit-tested. It takes already-resolved customer features, store features, the cart, the menu, and the affinity config, and returns ranked recommendations. The model (Task 6/7) and the demo all call this.

**Files:**
- Create: `src/ml/__init__.py`
- Create: `src/ml/scoring.py`
- Test: `tests/test_scoring.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_scoring.py`:

```python
from src.features.affinity import load_affinity
from src.ml.scoring import rank_recommendations

CFG = load_affinity()
# menu: id -> (category, subcategory, name)
MENU = {
    1: ("pizza", "pepperoni", "Large Pepperoni"),
    2: ("pizza", "cheese", "Large Cheese"),
    30: ("wings", "boneless", "8pc Boneless"),
    53: ("drinks", "soda", "20oz Coke"),
    54: ("drinks", "soda", "20oz Diet Coke"),
    55: ("drinks", "water", "20oz Water"),
    70: ("desserts", "cake", "Lava Cake"),
}
# customer features (resolved); None => cold start
CUST = {"tier": "gold", "aov": 22.0,
        "affinity_pizza": 0.5, "affinity_wings": 0.2, "affinity_drinks": 0.1,
        "affinity_sides": 0.1, "affinity_salads": 0.0, "affinity_desserts": 0.1}
STORE = {"popularity": {1: 1.0, 53: 0.8, 70: 0.3}, "store_aov": 20.0}


def _ids(recs):
    return [r["menu_item_id"] for r in recs]


def test_excludes_items_already_in_cart():
    recs = rank_recommendations(cart=[1], cust=CUST, store=STORE, menu=MENU, cfg=CFG, max_results=10)
    assert 1 not in _ids(recs)


def test_pizza_cart_recommends_a_drink_near_top():
    recs = rank_recommendations(cart=[1], cust=CUST, store=STORE, menu=MENU, cfg=CFG, max_results=3)
    top_cats = [MENU[r["menu_item_id"]][0] for r in recs]
    assert "drinks" in top_cats


def test_suppresses_second_soda_when_soda_in_cart():
    recs = rank_recommendations(cart=[1, 53], cust=CUST, store=STORE, menu=MENU, cfg=CFG, max_results=10)
    # 53 in cart; 54 is another soda -> suppressed; 55 (water) allowed
    ids = _ids(recs)
    assert 54 not in ids
    assert 53 not in ids


def test_respects_max_results():
    recs = rank_recommendations(cart=[1], cust=CUST, store=STORE, menu=MENU, cfg=CFG, max_results=2)
    assert len(recs) == 2


def test_results_sorted_by_score_desc():
    recs = rank_recommendations(cart=[1], cust=CUST, store=STORE, menu=MENU, cfg=CFG, max_results=5)
    scores = [r["score"] for r in recs]
    assert scores == sorted(scores, reverse=True)


def test_cold_start_uses_store_popularity_without_customer():
    recs = rank_recommendations(cart=[], cust=None, store=STORE, menu=MENU, cfg=CFG, max_results=3)
    assert len(recs) > 0
    # empty cart + cold start -> popular items surface; item 1 is most popular
    assert 1 in _ids(recs)


def test_every_rec_has_contract_fields():
    recs = rank_recommendations(cart=[1], cust=CUST, store=STORE, menu=MENU, cfg=CFG, max_results=3)
    for r in recs:
        assert set(r.keys()) >= {"menu_item_id", "item_name", "category", "subcategory", "score", "reason"}
        assert isinstance(r["score"], float)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/test_scoring.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.ml'`.

- [ ] **Step 3: Implement the scoring core**

Create empty `src/ml/__init__.py`.

Create `src/ml/scoring.py`:

```python
"""Pure-Python recommender scoring core (no Spark/MLflow).

Given resolved customer features, store features, the cart, the menu catalog, and
the affinity config, produce a ranked list of add-on recommendations that obeys the
endpoint contract. The trained model (recommender_model.py) can optionally override
the blended-heuristic score with a learned probability via the `score_fn` hook.
"""
from src.features.affinity import complement_score, cart_categories, is_suppressed_subcategory

CATEGORIES = ["pizza", "wings", "sides", "salads", "drinks", "desserts"]


def _reason(cand_cat, basket_cats, comp, cust):
    if comp >= 0.5 and basket_cats:
        anchor = next(iter(basket_cats))
        base = f"complements {anchor}"
    elif not basket_cats:
        base = "popular at this store"
    else:
        base = "frequently added"
    if cust and cust.get("tier") in ("gold", "platinum"):
        base += f"; {cust['tier']}-tier favorite"
    if cand_cat == "drinks" and basket_cats and "drinks" not in basket_cats:
        base = "complements your order; no drink in cart"
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
        score = scorer(cand_id, cat, basket_cats, cust, store, cfg)
        scored.append({
            "menu_item_id": int(cand_id),
            "item_name": name,
            "category": cat,
            "subcategory": subcat,
            "score": float(round(score, 6)),
            "reason": _reason(cat, basket_cats, complement_score(basket_cats, cat, cfg), cust),
        })

    scored.sort(key=lambda r: (r["score"], -r["menu_item_id"]), reverse=True)
    return scored[:max_results]
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest tests/test_scoring.py -q`
Expected: PASS (7 passed).

- [ ] **Step 5: Commit**

```bash
git add src/ml/__init__.py src/ml/scoring.py tests/test_scoring.py
git commit -m "feat: recommender scoring core (complementarity + affinity + popularity blend)"
```

---

### Task 6: MLflow pyfunc recommender model

The pyfunc wraps `scoring.py`. At serving time it receives a DataFrame whose rows carry `customer_id`, `store_id`, `cart_item_ids`, `max_results` **plus** the customer/store feature columns joined by Feature Engineering automatic lookup. It bundles the menu catalog, the affinity config, and (optionally) the trained estimator as artifacts.

**Files:**
- Create: `src/ml/recommender_model.py`
- Test: `tests/test_recommender_model.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_recommender_model.py`:

```python
import json
import pandas as pd
from src.ml.recommender_model import RecommenderModel

MENU = {
    1: ["pizza", "pepperoni", "Large Pepperoni"],
    53: ["drinks", "soda", "20oz Coke"],
    54: ["drinks", "soda", "20oz Diet Coke"],
    70: ["desserts", "cake", "Lava Cake"],
}
AFFINITY = {
    "complements": {
        "pizza": {"drinks": 0.9, "desserts": 0.5, "sides": 0.7, "wings": 0.4, "salads": 0.2},
        "drinks": {"pizza": 0.3, "desserts": 0.4, "sides": 0.3, "wings": 0.2, "salads": 0.1},
        "desserts": {"drinks": 0.6, "pizza": 0.2, "sides": 0.1, "wings": 0.1, "salads": 0.1},
        "wings": {"drinks": 0.8, "sides": 0.8, "pizza": 0.4, "desserts": 0.3, "salads": 0.2},
        "sides": {"drinks": 0.7, "pizza": 0.4, "desserts": 0.3, "wings": 0.3, "salads": 0.1},
        "salads": {"drinks": 0.7, "pizza": 0.3, "sides": 0.2, "desserts": 0.2, "wings": 0.1},
    },
    "empty_cart_priority": ["pizza", "wings", "salads", "sides", "drinks", "desserts"],
    "suppress_duplicate_subcategories": ["soda", "water"],
}


def _model():
    m = RecommenderModel()
    m._load(menu=MENU, affinity=AFFINITY, estimator=None)  # test hook bypassing artifact load
    return m


def test_predict_returns_one_result_per_row():
    m = _model()
    df = pd.DataFrame([
        {"profile_id": 10, "member_id": 10, "store_id": 42, "cart_product_ids": [1],
         "viewed_product_id": None, "num_recommendations": 3,
         "tier": "gold", "affinity_pizza": 0.5, "affinity_drinks": 0.1, "affinity_wings": 0.1,
         "affinity_sides": 0.1, "affinity_salads": 0.0, "affinity_desserts": 0.2,
         "aov": 22.0, "store_aov": 20.0},
    ])
    out = m.predict(None, df)
    assert len(out) == 1
    rec0 = out[0]
    assert rec0["personalized"] is True
    assert len(rec0["recommendations"]) <= 3
    ids = [r["menu_item_id"] for r in rec0["recommendations"]]
    assert 1 not in ids
    assert all(isinstance(i, int) for i in ids)  # ints out


def test_cold_start_when_customer_features_missing():
    m = _model()
    df = pd.DataFrame([
        {"profile_id": -1, "member_id": None, "store_id": 42, "cart_product_ids": [],
         "viewed_product_id": None, "num_recommendations": 2,
         "tier": None, "aov": None, "store_aov": 20.0},
    ])
    out = m.predict(None, df)
    assert out[0]["personalized"] is False
    assert len(out[0]["recommendations"]) == 2


def test_accepts_json_string_cart_and_viewed_item():
    m = _model()
    df = pd.DataFrame([
        {"profile_id": 10, "member_id": 10, "store_id": 42,
         "cart_product_ids": json.dumps([1, 53]), "viewed_product_id": 2,
         "num_recommendations": 5, "tier": "gold", "affinity_pizza": 0.5, "affinity_drinks": 0.1,
         "affinity_wings": 0.0, "affinity_sides": 0.0, "affinity_salads": 0.0, "affinity_desserts": 0.0,
         "aov": 22.0, "store_aov": 20.0},
    ])
    out = m.predict(None, df)
    ids = [r["menu_item_id"] for r in out[0]["recommendations"]]
    assert 54 not in ids  # second soda suppressed (soda already in cart)
    assert 1 not in ids and 53 not in ids  # cart items excluded
    assert 2 not in ids  # viewed item excluded
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/test_recommender_model.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.ml.recommender_model'`.

- [ ] **Step 3: Implement the pyfunc model**

Create `src/ml/recommender_model.py`:

```python
"""MLflow pyfunc recommender. Wraps src.ml.scoring with looked-up + on-demand features.

At serving time, Feature Engineering automatic lookup joins customer/store feature
columns onto each request row by customer_id/store_id. This pyfunc reads those
columns plus the raw request fields, calls the scoring core, and returns the
contract response per row.
"""
import json
import mlflow.pyfunc

from src.features.affinity import CATEGORIES if False else None  # placeholder; see _load
from src.ml.scoring import rank_recommendations, heuristic_score

_CATS = ["pizza", "wings", "sides", "salads", "drinks", "desserts"]


class RecommenderModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        with open(context.artifacts["menu"]) as f:
            raw = json.load(f)
        menu = {int(k): tuple(v) for k, v in raw.items()}
        import yaml
        with open(context.artifacts["affinity"]) as f:
            affinity = yaml.safe_load(f)
        estimator = None
        if "estimator" in context.artifacts:
            import joblib
            estimator = joblib.load(context.artifacts["estimator"])
        self._load(menu=menu, affinity=affinity, estimator=estimator)

    def _load(self, menu, affinity, estimator):
        # test hook + shared init. menu values may be list or tuple.
        self.menu = {int(k): tuple(v) for k, v in menu.items()}
        self.affinity = affinity
        self.estimator = estimator

    def _parse_cart(self, raw):
        if raw is None:
            return []
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = [x for x in raw.split(",") if x]
        return [int(x) for x in raw]

    def _customer(self, row):
        # cold-start if no tier/affinity present
        has_cust = row.get("tier") not in (None, "", "none") or any(
            row.get(f"affinity_{c}") not in (None,) for c in _CATS)
        if not has_cust:
            return None
        cust = {"tier": row.get("tier") or "none", "aov": row.get("aov") or 0.0}
        for c in _CATS:
            v = row.get(f"affinity_{c}")
            cust[f"affinity_{c}"] = float(v) if v is not None else 0.0
        return cust

    def _store(self, row):
        pop = row.get("popularity") or {}
        if isinstance(pop, str):
            try:
                pop = json.loads(pop)
            except Exception:
                pop = {}
        pop = {int(k): float(v) for k, v in pop.items()}
        return {"popularity": pop, "store_aov": row.get("store_aov") or 0.0}

    def _score_fn(self):
        if self.estimator is None:
            return None
        est = self.estimator

        def fn(cand_id, cand_cat, basket_cats, cust, store, cfg):
            from src.ml.features_vector import build_feature_vector
            x = build_feature_vector(cand_id, cand_cat, basket_cats, cust, store, cfg, self.menu)
            try:
                return float(est.predict_proba([x])[0][1])
            except Exception:
                return heuristic_score(cand_id, cand_cat, basket_cats, cust, store, cfg)
        return fn

    def predict(self, context, model_input):
        rows = model_input.to_dict(orient="records")
        score_fn = self._score_fn()
        out = []
        for row in rows:
            cart = self._parse_cart(row.get("cart_product_ids"))
            # fold the viewed item into the basket context (it shapes complementarity
            # and is excluded from results, like cart items)
            viewed = row.get("viewed_product_id")
            if viewed not in (None, "", 0):
                try:
                    cart = cart + [int(viewed)]
                except Exception:
                    pass
            cust = self._customer(row)
            store = self._store(row)
            n = row.get("num_recommendations") or 5
            recs = rank_recommendations(
                cart=cart, cust=cust, store=store, menu=self.menu,
                cfg=self.affinity, max_results=n, score_fn=score_fn)
            out.append({
                "personalized": cust is not None,
                "recommendations": recs,  # menu_item_id is int (scoring core coerces)
            })
        return out
```

> **Fix the placeholder import line** — replace the nonsensical `from src.features.affinity import CATEGORIES if False else None ...` with nothing (delete that line); `_CATS` is defined locally below it. (It is intentionally flagged here so the engineer removes it.)

- [ ] **Step 4: Remove the placeholder import**

Delete the line `from src.features.affinity import CATEGORIES if False else None  # placeholder; see _load` from `src/ml/recommender_model.py`.

- [ ] **Step 5: Run the test to verify it passes**

Run: `pytest tests/test_recommender_model.py -q`
Expected: PASS (3 passed).

- [ ] **Step 6: Commit**

```bash
git add src/ml/recommender_model.py tests/test_recommender_model.py
git commit -m "feat: MLflow pyfunc recommender wrapping scoring core"
```

---

### Task 7: Learned feature-vector builder (shared by training + serving)

Both training and the pyfunc's learned `score_fn` must build the **same** per-candidate feature vector. Extract it into one tested module.

**Files:**
- Create: `src/ml/features_vector.py`
- Test: add to `tests/test_scoring.py` (new test function)

- [ ] **Step 1: Write the failing test (append to `tests/test_scoring.py`)**

Add at the end of `tests/test_scoring.py`:

```python
from src.ml.features_vector import build_feature_vector, FEATURE_NAMES


def test_feature_vector_length_matches_names():
    cust = {"tier": "gold", "aov": 22.0, "affinity_pizza": 0.5, "affinity_drinks": 0.1,
            "affinity_wings": 0.0, "affinity_sides": 0.0, "affinity_salads": 0.0,
            "affinity_desserts": 0.0}
    store = {"popularity": {53: 0.8}, "store_aov": 20.0}
    x = build_feature_vector(53, "drinks", {"pizza"}, cust, store, CFG, MENU)
    assert len(x) == len(FEATURE_NAMES)
    assert all(isinstance(v, float) for v in x)


def test_feature_vector_cold_start_no_customer():
    store = {"popularity": {53: 0.8}, "store_aov": 20.0}
    x = build_feature_vector(53, "drinks", set(), None, store, CFG, MENU)
    assert len(x) == len(FEATURE_NAMES)
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/test_scoring.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.ml.features_vector'`.

- [ ] **Step 3: Implement the feature-vector builder**

Create `src/ml/features_vector.py`:

```python
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
    comp = complement_score(basket_cats, cand_cat, cfg)
    cust_aff = float(cust.get(f"affinity_{cand_cat}", 0.0)) if cust else 0.0
    pop = float((store or {}).get("popularity", {}).get(int(cand_id), 0.0))
    cust_aov = float(cust.get("aov", 0.0)) if cust else 0.0
    s_aov = float((store or {}).get("store_aov", 0.0))
    tier_ord = float(_TIERS.get((cust or {}).get("tier", "none"), 0))
    onehot = [1.0 if cand_cat == c else 0.0 for c in _CATS]
    return [comp, cust_aff, pop, cust_aov, s_aov, tier_ord] + onehot
```

- [ ] **Step 4: Run to verify it passes**

Run: `pytest tests/test_scoring.py -q`
Expected: PASS (9 passed — the 7 original + 2 new).

- [ ] **Step 5: Commit**

```bash
git add src/ml/features_vector.py tests/test_scoring.py
git commit -m "feat: shared per-candidate feature-vector builder for training + serving"
```

---

### Task 8: Train + register model notebook

**Files:**
- Create: `src/ml/train_recommender.py`

> No hermetic test (needs Spark + MLflow + workspace). Verified by `bundle validate` and the live run.

- [ ] **Step 1: Write the training notebook**

Create `src/ml/train_recommender.py`:

```python
# Databricks notebook source
# Train the basket-aware recommender, log via Feature Engineering (automatic lookup),
# register in UC, and (re)create the Model Serving endpoint.
import json
import random
import tempfile

try:
    catalog_name = dbutils.widgets.get("catalog_name")
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    catalog_name = "jmrdemo"
    schema_prefix = "synth_"

sp = f"{catalog_name}.{schema_prefix}"
features_schema = f"{schema_prefix}features"
fq = lambda t: f"{catalog_name}.{features_schema}.{t}"  # noqa: E731
random.seed(42)

from src.features.affinity import load_affinity, cart_categories
from src.ml.features_vector import build_feature_vector, FEATURE_NAMES

# --- Menu + affinity artifacts ---
menu_rows = spark.read.table(f"{sp}ref.menu_item").select(
    "menu_item_id", "category", "subcategory", "item_name").collect()
menu = {int(r["menu_item_id"]): (r["category"], r["subcategory"], r["item_name"]) for r in menu_rows}
cfg = load_affinity()
all_item_ids = list(menu.keys())

# --- Build training examples from historical orders (positives = items in order;
#     negatives = sampled items not in order, scored against the partial basket). ---
orders = spark.read.table(f"{sp}silver.guest_order").select(
    "guest_order_id", "profile_id", "unit_id").collect()
items_by_order = {}
for r in spark.read.table(f"{sp}silver.order_item").select("guest_order_id", "menu_item_id").collect():
    items_by_order.setdefault(r["guest_order_id"], []).append(int(r["menu_item_id"]))

# customer + store feature lookups (read feature tables to driver; small dataset)
cust_feat = {int(r["guest_profile_id"]): r.asDict()
             for r in spark.read.table(fq("customer_features")).collect()}
store_feat = {int(r["unit_id"]): r.asDict()
              for r in spark.read.table(fq("store_features")).collect()}

def store_dict(uid):
    s = store_feat.get(uid)
    if not s:
        return {"popularity": {}, "store_aov": 0.0}
    pop = s.get("popularity") or {}
    return {"popularity": {int(k): float(v) for k, v in pop.items()},
            "store_aov": float(s.get("store_aov") or 0.0)}

X, y = [], []
for o in orders:
    order_items = items_by_order.get(o["guest_order_id"], [])
    if len(order_items) < 2:
        continue
    cust = cust_feat.get(o["profile_id"]) if o["profile_id"] is not None else None
    store = store_dict(o["unit_id"])
    # simulate a partial basket: hold out one item as the "added" positive
    held = order_items[-1]
    basket = order_items[:-1]
    basket_cats = cart_categories(basket, menu)
    # positive
    cat, _, _ = menu[held]
    X.append(build_feature_vector(held, cat, basket_cats, cust, store, cfg, menu)); y.append(1)
    # 3 negatives
    negs = [i for i in all_item_ids if i not in order_items]
    for ni in random.sample(negs, min(3, len(negs))):
        ncat, _, _ = menu[ni]
        X.append(build_feature_vector(ni, ncat, basket_cats, cust, store, cfg, menu)); y.append(0)

print(f"[INFO] training rows: {len(X)} (pos={sum(y)})")

from sklearn.ensemble import GradientBoostingClassifier
clf = GradientBoostingClassifier(random_state=42, n_estimators=100, max_depth=3)
clf.fit(X, y)
print(f"[INFO] train accuracy: {clf.score(X, y):.3f}")

# --- Log via Feature Engineering with FeatureLookups (automatic serving lookup) ---
import mlflow
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from src.ml.recommender_model import RecommenderModel

mlflow.set_registry_uri("databricks-uc")
fe = FeatureEngineeringClient()
CATS = ["pizza", "wings", "sides", "salads", "drinks", "desserts"]

# Build a tiny training_set DF carrying the lookup keys so FE records the lookups.
# Lookup-key column names match the live request fields (profile_id, store_id);
# FE maps them positionally to the feature-table PKs (guest_profile_id, unit_id).
import pandas as pd
keys_pdf = pd.DataFrame([{"profile_id": int(o["profile_id"]) if o["profile_id"] else -1,
                          "store_id": int(o["unit_id"]), "label": 1} for o in orders[:500]])
keys_sdf = spark.createDataFrame(keys_pdf)
training_set = fe.create_training_set(
    df=keys_sdf,
    feature_lookups=[
        FeatureLookup(table_name=fq("customer_features"), lookup_key="profile_id",
                      feature_names=["tier", "aov"] + [f"affinity_{c}" for c in CATS]),
        FeatureLookup(table_name=fq("store_features"), lookup_key="store_id",
                      feature_names=["store_aov", "popularity"]),
    ],
    label="label",
    exclude_columns=["profile_id", "store_id"],
)

# Persist artifacts (menu, affinity, estimator) for the pyfunc.
import joblib, yaml
tmp = tempfile.mkdtemp()
menu_path = f"{tmp}/menu.json"; aff_path = f"{tmp}/basket_affinity.yml"; est_path = f"{tmp}/estimator.joblib"
with open(menu_path, "w") as f:
    json.dump({str(k): list(v) for k, v in menu.items()}, f)
with open(aff_path, "w") as f:
    yaml.safe_dump(cfg, f)
joblib.dump(clf, est_path)

model_name = fq("qsr_recommender")
with mlflow.start_run(run_name="qsr_recommender"):
    fe.log_model(
        model=RecommenderModel(),
        artifact_path="recommender",
        flavor=mlflow.pyfunc,
        training_set=training_set,
        registered_model_name=model_name,
        artifacts={"menu": menu_path, "affinity": aff_path, "estimator": est_path},
        pip_requirements=["scikit-learn", "pyyaml", "joblib", "mlflow", "pandas"],
    )
print(f"[INFO] registered {model_name}")

# --- (Re)create Model Serving endpoint pointing at latest version ---
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput, ServedEntityInput)
w = WorkspaceClient()
latest = max(int(v.version) for v in w.model_versions.list(full_name=model_name))
endpoint = f"{schema_prefix}qsr-recommender"
served = ServedEntityInput(entity_name=model_name, entity_version=str(latest),
                           scale_to_zero_enabled=True, workload_size="Small")
try:
    w.serving_endpoints.create(name=endpoint,
                               config=EndpointCoreConfigInput(served_entities=[served]))
    print(f"[INFO] created serving endpoint {endpoint}")
except Exception as e:
    print(f"[INFO] endpoint exists, updating config: {e}")
    w.serving_endpoints.update_config(name=endpoint, served_entities=[served])

# Grant CAN_QUERY to the website principal (PAT/SP) so PizzaTel can call the endpoint.
try:
    query_principal = dbutils.widgets.get("recommender_query_principal")
except Exception:
    query_principal = ""
if query_principal:
    from databricks.sdk.service.serving import (
        ServingEndpointAccessControlRequest, ServingEndpointPermissionLevel)
    try:
        w.serving_endpoints.set_permissions(
            serving_endpoint_id=w.serving_endpoints.get(name=endpoint).id,
            access_control_list=[ServingEndpointAccessControlRequest(
                service_principal_name=query_principal,
                permission_level=ServingEndpointPermissionLevel.CAN_QUERY)])
        print(f"[INFO] granted CAN_QUERY on {endpoint} to {query_principal}")
    except Exception as e:
        print(f"[WARN] grant CAN_QUERY skipped: {e}")
else:
    print("[INFO] no recommender_query_principal set; skipping CAN_QUERY grant")
print("[DONE] train_recommender complete")
```

> If the website authenticates with a user PAT rather than a service principal, swap `service_principal_name=query_principal` for `user_name=query_principal`.

- [ ] **Step 2: Syntax-check**

Run: `python -c "import ast; ast.parse(open('src/ml/train_recommender.py').read())"`
Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add src/ml/train_recommender.py
git commit -m "feat: train_recommender notebook (FE log_model + UC register + model serving)"
```

---

## Phase 3 — DAB wiring, weekly refresh, demo, destroy, docs

### Task 9: DAB variables

**Files:**
- Modify: `databricks.yml` (variables block, after `ontos_enabled`)

- [ ] **Step 1: Add variables**

In `databricks.yml`, append to the `variables:` block:

```yaml
  features_enabled:
    default: "true"
    description: "Set to 'false' to skip feature store + recommender setup/destroy steps."
  feature_refresh_cron:
    default: "0 0 6 ? * SUN"
    description: "Quartz cron for weekly feature-table refresh (default Sundays 06:00 UTC)."
  recommender_query_principal:
    default: ""
    description: "Service principal name (or user PAT principal) granted CAN_QUERY on the recommender endpoint so PizzaTel can call it. Empty = skip the grant."
```

- [ ] **Step 2: Validate**

Run: `databricks bundle validate -p DEFAULT`
Expected: `Validation OK`.

- [ ] **Step 3: Commit**

```bash
git add databricks.yml
git commit -m "feat: add features_enabled + feature_refresh_cron DAB variables"
```

---

### Task 10: Wire setup-job tasks into the DAG

The new tasks must run after silver tables exist (`start_pipeline`) and feed online infra. Order: `start_pipeline` → `build_feature_tables` → `train_recommender`. `unpause_generator` should additionally depend on `train_recommender` so the demo is ready when setup completes.

**Files:**
- Modify: `resources/setup_job.yml`

- [ ] **Step 1: Add the two tasks**

In `resources/setup_job.yml`, add these tasks (mirror the existing `environment_key`/serverless notebook pattern; `build_feature_tables` and `train_recommender` need the FE + sklearn libs — reuse or define an environment with `databricks-feature-engineering`, `scikit-learn`, `joblib`, `pandas`, `pyyaml`):

```yaml
      - task_key: build_feature_tables
        depends_on:
          - task_key: start_pipeline
        notebook_task:
          notebook_path: ../src/setup/build_feature_tables.py
          base_parameters:
            catalog_name: ${var.catalog_name}
            schema_prefix: ${var.schema_prefix}
        environment_key: ml

      - task_key: train_recommender
        depends_on:
          - task_key: build_feature_tables
        notebook_task:
          notebook_path: ../src/ml/train_recommender.py
          base_parameters:
            catalog_name: ${var.catalog_name}
            schema_prefix: ${var.schema_prefix}
            recommender_query_principal: ${var.recommender_query_principal}
        environment_key: ml
```

- [ ] **Step 2: Add the `ml` environment**

In the same file's `environments:` list (mirror the existing `refresh`/`generator` environment blocks), add:

```yaml
      - environment_key: ml
        spec:
          client: "2"
          dependencies:
            - databricks-feature-engineering
            - scikit-learn
            - joblib
            - pandas
            - pyyaml
```

- [ ] **Step 3: Make `unpause_generator` wait for the recommender**

Find the `unpause_generator` task's `depends_on:` and add `train_recommender`:

```yaml
      - task_key: unpause_generator
        depends_on:
          - task_key: backfill
          - task_key: create_genie_space
          - task_key: apply_ontos
          - task_key: train_recommender
```

- [ ] **Step 4: Validate**

Run: `databricks bundle validate -p DEFAULT`
Expected: `Validation OK`.

- [ ] **Step 5: Commit**

```bash
git add resources/setup_job.yml
git commit -m "feat: wire build_feature_tables + train_recommender into setup DAG"
```

---

### Task 11: Weekly feature-refresh job

**Files:**
- Create: `resources/feature_refresh_job.yml`

- [ ] **Step 1: Write the job**

Create `resources/feature_refresh_job.yml` (mirror `resources/refresh_weather_events.yml`'s structure — name, schedule with quartz cron, serverless notebook task, environment):

```yaml
resources:
  jobs:
    feature_refresh_job:
      name: "QSR Feature Refresh [${bundle.target}]"
      schedule:
        quartz_cron_expression: ${var.feature_refresh_cron}
        timezone_id: UTC
        pause_status: UNPAUSED
      tasks:
        - task_key: refresh_features
          notebook_task:
            notebook_path: ../src/setup/build_feature_tables.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              schema_prefix: ${var.schema_prefix}
          environment_key: ml
      environments:
        - environment_key: ml
          spec:
            client: "2"
            dependencies:
              - databricks-feature-engineering
              - scikit-learn
              - joblib
              - pandas
              - pyyaml
      tags:
        project: qsr-synth-data-generator
```

- [ ] **Step 2: Validate**

Run: `databricks bundle validate -p DEFAULT`
Expected: `Validation OK`.

- [ ] **Step 3: Commit**

```bash
git add resources/feature_refresh_job.yml
git commit -m "feat: weekly feature-refresh job (re-runs build_feature_tables)"
```

> **Note:** `build_feature_tables.py` recomputes feature tables AND triggers an online-table refresh, so the weekly job keeps both Delta and online stores current. The model is not retrained weekly (recommendations stay stable); retraining can be added later by appending `train_recommender` as a second task here.

---

### Task 12: Demo client notebook

**Files:**
- Create: `src/ml/demo_client.py`

- [ ] **Step 1: Write the demo notebook**

Create `src/ml/demo_client.py`:

```python
# Databricks notebook source
# Demo: call the live recommender endpoint with a cart and print recommendations.
# This documents the exact request/response contract PizzaTel uses.
import json

try:
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    schema_prefix = "synth_"

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
endpoint = f"{schema_prefix}qsr-recommender"

def recommend(profile_id, store_id, cart_product_ids, viewed_product_id=None, num_recommendations=4):
    resp = w.serving_endpoints.query(
        name=endpoint,
        dataframe_records=[{
            "profile_id": int(profile_id),
            "member_id": int(profile_id) if profile_id and int(profile_id) > 0 else None,
            "store_id": int(store_id),
            "cart_product_ids": [int(c) for c in cart_product_ids],
            "viewed_product_id": int(viewed_product_id) if viewed_product_id else None,
            "num_recommendations": num_recommendations,
        }],
    )
    return resp.predictions[0]

# Example 1: known customer, pizza in cart -> expect a drink near the top
print("== pizza cart, known gold customer ==")
print(json.dumps(recommend(profile_id=10231, store_id=42, cart_product_ids=[1]), indent=2))

# Example 2: soda already in cart -> no second soda
print("== pizza + soda cart -> soda suppressed ==")
print(json.dumps(recommend(profile_id=10231, store_id=42, cart_product_ids=[1, 53]), indent=2))

# Example 3: cold start (guest profile), empty cart -> store popular items
print("== cold start, empty cart ==")
print(json.dumps(recommend(profile_id=-1, store_id=42, cart_product_ids=[]), indent=2))
```

- [ ] **Step 2: Syntax-check**

Run: `python -c "import ast; ast.parse(open('src/ml/demo_client.py').read())"`
Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add src/ml/demo_client.py
git commit -m "docs: demo client notebook documenting the recommender contract"
```

---

### Task 13: Destroy-job teardown

Tear down in reverse dependency order, BEFORE the existing schema drops. Endpoints and online tables are billable — they MUST be removed. Use the established best-effort `[WARN]` pattern.

**Files:**
- Modify: `src/setup/destroy_notebook.py`

- [ ] **Step 1: Read the destroy notebook's existing structure**

Run: `grep -n "Step 0\|Step 1\|schema_prefix\|WorkspaceClient" src/setup/destroy_notebook.py`
Expected: shows the step ordering and where `w = WorkspaceClient()` is available.

- [ ] **Step 2: Insert the feature-store teardown block**

In `src/setup/destroy_notebook.py`, immediately after the param/`WorkspaceClient` setup and BEFORE "Step 1" (the metrics-view drops), insert:

```python
# Step 0h: Tear down feature store + recommender (billable endpoints first).
features_schema = f"{schema_prefix}features"
ffq = lambda t: f"{catalog_name}.{features_schema}.{t}"  # noqa: E731
_fs_endpoint = f"{schema_prefix}qsr-customer-features"
_model_endpoint = f"{schema_prefix}qsr-recommender"
_model_name = ffq("qsr_recommender")

# 0h-1: serving endpoints
for _ep in [_model_endpoint, _fs_endpoint]:
    try:
        w.serving_endpoints.delete(name=_ep)
        print(f"[INFO] deleted serving endpoint {_ep}")
    except Exception as e:
        print(f"[WARN] delete serving endpoint {_ep} skipped: {e}")

# 0h-2: feature spec
try:
    from databricks.feature_engineering import FeatureEngineeringClient
    _fe = FeatureEngineeringClient()
    _fe.delete_feature_spec(name=ffq("customer_store_spec"))
    print("[INFO] deleted feature spec")
except Exception as e:
    print(f"[WARN] delete feature spec skipped: {e}")

# 0h-3: online tables
for _ot in ["customer_features_online", "store_features_online"]:
    try:
        w.online_tables.delete(name=ffq(_ot))
        print(f"[INFO] deleted online table {ffq(_ot)}")
    except Exception as e:
        print(f"[WARN] delete online table {ffq(_ot)} skipped: {e}")

# 0h-4: registered model (all versions)
try:
    w.registered_models.delete(full_name=_model_name)
    print(f"[INFO] deleted registered model {_model_name}")
except Exception as e:
    print(f"[WARN] delete registered model {_model_name} skipped: {e}")

# 0h-5: feature tables + schema
for _t in ["customer_features", "store_features"]:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {ffq(_t)}")
        print(f"[INFO] dropped feature table {ffq(_t)}")
    except Exception as e:
        print(f"[WARN] drop feature table {ffq(_t)} skipped: {e}")
try:
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{features_schema} CASCADE")
    print(f"[INFO] dropped schema {catalog_name}.{features_schema}")
except Exception as e:
    print(f"[WARN] drop features schema skipped: {e}")
```

- [ ] **Step 3: Syntax-check**

Run: `python -c "import ast; ast.parse(open('src/setup/destroy_notebook.py').read())"`
Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add src/setup/destroy_notebook.py
git commit -m "feat: destroy feature store + recommender endpoints/tables/model"
```

---

### Task 14: Full test sweep + docs

**Files:**
- Modify: relevant `docs/` files (the living-docs architecture/gotchas/quickstart set used by this project)

- [ ] **Step 1: Run the full hermetic suite**

Run: `pytest -q`
Expected: all pass — baseline 120 + new (7 affinity + 5 customer + 4 store + 9 scoring + 3 model = 28) = **148 passed**.

- [ ] **Step 2: Validate the bundle end-to-end**

Run: `databricks bundle validate -p DEFAULT`
Expected: `Validation OK`.

- [ ] **Step 3: Update docs**

Add a "Customer Feature Store & Recommender" section to the project docs covering: the two new endpoints (Feature Serving `${schema_prefix}qsr-customer-features`, Model Serving `${schema_prefix}qsr-recommender`), the `${schema_prefix}features` schema + tables, the request/response contract (copy the "Endpoint Contract" section from this plan), the weekly refresh cadence, the no-generator-change / curated-affinity design decision, and the cost/teardown note. Mirror the gotchas already captured for weather-events/ontos.

- [ ] **Step 4: Commit**

```bash
git add docs/
git commit -m "docs: document customer feature store, recommender endpoints, and contract"
```

---

### Task 15: Live deploy + smoke (requires workspace)

> Run only when ready to deploy. This is the integration verification the unit tests cannot do.

- [ ] **Step 1: Deploy the bundle**

Run: `databricks bundle deploy -p DEFAULT`
Expected: deploy succeeds; jobs + pipeline appear in the workspace.

- [ ] **Step 2: Run setup and watch the new tasks**

Run: `databricks bundle run setup_job -p DEFAULT` (or trigger via UI)
Expected: `build_feature_tables` prints computed row counts + creates online tables + feature serving endpoint; `train_recommender` prints training rows/accuracy + registers the model + creates the serving endpoint; both endpoints reach `READY`.

- [ ] **Step 3: Smoke the contract**

Run the `src/ml/demo_client.py` notebook (or `databricks bundle run` a one-off).
Expected: Example 1 returns a drink near the top for a pizza cart; Example 2 returns NO second soda; Example 3 (cold start) returns store-popular items with `personalized: false`.

- [ ] **Step 4: Verify teardown (optional, in a disposable workspace)**

Run: `databricks bundle run destroy_job -p DEFAULT` then confirm both serving endpoints and both online tables are gone (`databricks serving-endpoints list`, check `${schema_prefix}features` schema dropped).
Expected: no orphaned billable endpoints/online tables.

- [ ] **Step 5: Finish the branch**

Use superpowers:finishing-a-development-branch to open a PR / merge.

---

## Self-Review (completed during planning)

- **Spec coverage:** ✅ Feature store (Tasks 2–4), real-time feature serving / fold #1 (Task 4 feature serving endpoint + online tables), recommendation model / fold #2 (Tasks 5–8), customer_id+store_id+cart contract (Endpoint Contract section + Tasks 6/12), setup-job creation (Tasks 4/8/10), weekly refresh (Task 11), destroy (Task 13), no-generator-change via curated affinity (Task 1), CLAUDE.md branch + DAB automation (Pre-flight, Tasks 9–13).
- **Placeholder scan:** The one intentional placeholder (the bad import in Task 6 Step 3) is explicitly flagged and removed in Task 6 Step 4 — not a silent gap.
- **Type consistency:** `menu` is consistently `id -> (category, subcategory, name)`; `cart_item_ids` are stringified ints in the contract and `int()`-coerced everywhere; `FEATURE_NAMES`/`build_feature_vector` are the single source of truth shared by training (Task 8) and serving (Task 6 `_score_fn`); `rank_recommendations` signature is identical across scoring core, model, and tests; `fq`/`ffq` helpers build `catalog.schema_prefix+features.table` consistently in build/train/destroy.

---

## Open follow-ups (not in scope, note for later)

- **PizzaTel wiring** (owned by user): point its Python gRPC recommendation service at `${schema_prefix}qsr-recommender`, inject `store_id`, pass `customer_id` + cart `product_ids` as `cart_item_ids`.
- **Phase-stretch (Option D):** express basket signals as registered Feature Functions (on-demand features) for a fully declarative FeatureSpec.
- **Weekly retrain:** add `train_recommender` as a second task in `feature_refresh_job.yml` if recommendations should adapt over time.
