# Phase 2.5 — Generator Realism Fixes

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 7 data quality gaps in the QSR synthetic data generator so that discounts, item status, waste flags, loyalty redemptions, waste categories, guest churn, and order value all have realistic distributions.

**Architecture:** All changes are isolated to `src/generator/domains/` and `src/generator/reference/`. No schema, DLT pipeline, or job YAML changes are needed. Fixes 1–3 and 7b all touch `_build_order()` in `orders.py` — they are grouped into sequential tasks so each builds on the last, showing the cumulative function state. After all fixes are implemented, destroy existing data and run `setup_job` to regenerate a clean 1-month backfill.

**Tech Stack:** Python 3.11, pytest, `src/generator/domains/`, `src/generator/reference/`, `tests/`

---

## File Map

| File | Changes |
|---|---|
| `src/generator/domains/inventory.py` | Fix 5: replace hardcoded `waste_category` with weighted sample |
| `src/generator/domains/loyalty.py` | Fix 4: emit redeem `loyalty_transaction` alongside `reward_redemption` |
| `src/generator/domains/orders.py` | Fix 1 (discounts), Fix 2 (item status), Fix 3 (waste flags), Fix 7b (channel uplift) |
| `src/generator/domains/guest.py` | Fix 6: varied `account_status` on new registrations + `generate_guest_churn()` |
| `src/generator/runner.py` | Fix 6: call `generate_guest_churn()` in daily backfill loop |
| `src/generator/reference/us_locations.py` | Fix 7a: add `market_price_index` to each unit |
| `src/generator/reference/seeder.py` | Fix 7c: add `build_item_price_data()` + seed `ref.item_price` table |
| `src/generator/entity_registry.py` | Fix 7a+7c: add `unit_price_index()`, `guest_ids_for_unit()`, `item_price_multiplier()` methods; add `item_prices` optional constructor param |
| `tests/test_inventory.py` | new: `test_waste_categories_are_diverse` |
| `tests/test_guest_loyalty_workforce.py` | new: loyalty redeem tests + guest churn tests |
| `tests/test_orders.py` | update: fix existing total-amount test; new: item status, waste flag, discount, AOV tests |

---

## Task 1: Fix 5 — Waste Categories (`inventory.py`)

**Files:**
- Modify: `src/generator/domains/inventory.py`
- Test: `tests/test_inventory.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_inventory.py`:

```python
def test_waste_categories_are_diverse():
    from src.generator.domains.orders import generate_orders_for_tick
    ctx = build_context(1, datetime(2025, 9, 19, 21, 0), 2.0)
    reg = _reg()
    order_rows = generate_orders_for_tick(ctx, reg, tick_seconds=3600)
    all_cats = []
    for _ in range(20):
        rows = generate_inventory_events(ctx, reg, order_rows)
        all_cats.extend(r["waste_category"] for r in rows if r["event_type"] == "waste_log")
    valid = {"overproduction", "spoilage", "theft", "expired", "damaged"}
    assert all(c in valid for c in all_cats), f"Invalid category: {set(all_cats) - valid}"
    assert len(set(all_cats)) >= 2, "Expected multiple distinct waste categories across 20 runs"
```

- [ ] **Step 2: Run to confirm it fails**

```bash
pytest tests/test_inventory.py::test_waste_categories_are_diverse -v
```
Expected: FAIL — all values are `"overproduction"`, assertion `len >= 2` fails.

- [ ] **Step 3: Implement — add distribution constants and use them**

At the top of `src/generator/domains/inventory.py`, after the imports, add:
```python
_WASTE_CATS = ["overproduction", "spoilage", "theft", "expired", "damaged"]
_WASTE_WEIGHTS = [50, 25, 10, 10, 5]
```

On the `waste_category` line (currently line 53), replace:
```python
                "waste_category": "overproduction",
```
with:
```python
                "waste_category": random.choices(_WASTE_CATS, weights=_WASTE_WEIGHTS, k=1)[0],
```

- [ ] **Step 4: Run tests to verify**

```bash
pytest tests/test_inventory.py -v
```
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/generator/domains/inventory.py tests/test_inventory.py
git commit -m "fix: sample waste_category from weighted distribution (Fix 5)"
```

---

## Task 2: Fix 4 — Loyalty Redeem Transactions (`loyalty.py`)

**Files:**
- Modify: `src/generator/domains/loyalty.py`
- Test: `tests/test_guest_loyalty_workforce.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_guest_loyalty_workforce.py`:

```python
def test_reward_redemption_has_matching_redeem_transaction():
    from src.generator.domains.orders import generate_orders_for_tick
    ctx = build_context(1, datetime(2025, 9, 19, 19, 0), 2.0)  # high volume
    reg = _reg()
    order_rows = generate_orders_for_tick(ctx, reg, tick_seconds=3600)
    loyalty_rows = generate_loyalty_events(ctx, reg, order_rows)

    redemption_order_ids = {r["guest_order_id"] for r in loyalty_rows if r["event_type"] == "reward_redemption"}
    redeem_txn_order_ids = {r["guest_order_id"] for r in loyalty_rows if r.get("transaction_type") == "redeem"}
    assert redemption_order_ids == redeem_txn_order_ids, \
        "Every reward_redemption must have a matching redeem loyalty_transaction"

def test_redeem_transaction_has_negative_points_delta():
    from src.generator.domains.orders import generate_orders_for_tick
    ctx = build_context(1, datetime(2025, 9, 19, 19, 0), 2.0)
    reg = _reg()
    order_rows = generate_orders_for_tick(ctx, reg, tick_seconds=3600)
    loyalty_rows = generate_loyalty_events(ctx, reg, order_rows)
    for r in loyalty_rows:
        if r.get("transaction_type") == "redeem":
            assert r["points_delta"] < 0, f"Expected negative points_delta, got {r['points_delta']}"
```

- [ ] **Step 2: Run to confirm they fail**

```bash
pytest tests/test_guest_loyalty_workforce.py::test_reward_redemption_has_matching_redeem_transaction tests/test_guest_loyalty_workforce.py::test_redeem_transaction_has_negative_points_delta -v
```
Expected: FAIL — no `transaction_type == "redeem"` rows exist yet.

- [ ] **Step 3: Implement — emit redeem transaction alongside redemption**

In `src/generator/domains/loyalty.py`, inside `generate_loyalty_events`, after the `rows.append({... "event_type": "reward_redemption" ...})` block, add the redeem transaction:

```python
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
```

- [ ] **Step 4: Run tests to verify**

```bash
pytest tests/test_guest_loyalty_workforce.py -v
```
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/generator/domains/loyalty.py tests/test_guest_loyalty_workforce.py
git commit -m "fix: emit redeem loyalty_transaction alongside reward_redemption (Fix 4)"
```

---

## Task 3: Fix 2 — Order Item Status (`orders.py`)

**Files:**
- Modify: `src/generator/domains/orders.py`
- Test: `tests/test_orders.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_orders.py`:

```python
def test_cancelled_orders_emit_items():
    ctx = build_context(1, datetime(2025, 9, 19, 19, 0), 2.0)
    reg = _registry()
    rows = generate_orders_for_tick(ctx, reg, tick_seconds=3600)
    cancelled_ids = {r["guest_order_id"] for r in rows
                     if r["event_type"] == "guest_order" and r["order_status"] == "cancelled"}
    if not cancelled_ids:
        return  # no cancelled orders this tick — non-deterministic, skip
    cancelled_items = [r for r in rows if r["event_type"] == "order_item"
                       and r["guest_order_id"] in cancelled_ids]
    assert len(cancelled_items) > 0, "Cancelled orders must emit order_item rows"
    assert all(r["item_status"] == "cancelled" for r in cancelled_items)

def test_fulfilled_items_have_valid_status():
    ctx = _ctx()
    reg = _registry()
    rows = generate_orders_for_tick(ctx, reg, tick_seconds=3600)
    fulfilled_ids = {r["guest_order_id"] for r in rows
                     if r["event_type"] == "guest_order" and r["order_status"] == "fulfilled"}
    items = [r for r in rows if r["event_type"] == "order_item" and r["guest_order_id"] in fulfilled_ids]
    valid = {"fulfilled", "refunded"}
    assert all(r["item_status"] in valid for r in items)
```

- [ ] **Step 2: Run to confirm they fail**

```bash
pytest tests/test_orders.py::test_cancelled_orders_emit_items tests/test_orders.py::test_fulfilled_items_have_valid_status -v
```
Expected: `test_cancelled_orders_emit_items` FAIL — cancelled orders currently emit zero items.

- [ ] **Step 3: Implement — always emit items, set status per order outcome**

Replace `_build_order` in `src/generator/domains/orders.py` with the following. Note this is the full updated function — subsequent tasks (3, 4, 5) will show cumulative versions. This version adds Fix 2 only:

```python
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
            "waste_flag": False,
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
```

- [ ] **Step 4: Run tests to verify**

```bash
pytest tests/test_orders.py -v
```
Expected: all PASS including the two new tests.

- [ ] **Step 5: Commit**

```bash
git add src/generator/domains/orders.py tests/test_orders.py
git commit -m "fix: cancelled orders emit items with item_status=cancelled; 1% refunded (Fix 2)"
```

---

## Task 4: Fix 3 — Waste Flags on Order Items (`orders.py`)

**Files:**
- Modify: `src/generator/domains/orders.py`
- Test: `tests/test_orders.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_orders.py`:

```python
def test_waste_flag_set_on_some_items_at_late_night():
    ctx = build_context(1, datetime(2025, 9, 19, 21, 0), 2.0)  # late night, high volume
    reg = _registry()
    all_items = []
    for _ in range(15):
        rows = generate_orders_for_tick(ctx, reg, tick_seconds=3600)
        all_items.extend(r for r in rows if r["event_type"] == "order_item")
    if len(all_items) > 20:
        assert any(r["waste_flag"] for r in all_items), \
            "Expected some waste_flag=True at late-night high-volume"

def test_cancelled_items_have_higher_waste_rate_than_fulfilled():
    ctx = build_context(1, datetime(2025, 9, 19, 21, 0), 2.0)
    reg = _registry()
    fulfilled_items, cancelled_items = [], []
    for _ in range(30):
        rows = generate_orders_for_tick(ctx, reg, tick_seconds=3600)
        fulfilled_items.extend(r for r in rows if r["event_type"] == "order_item"
                                and r.get("item_status") == "fulfilled")
        cancelled_items.extend(r for r in rows if r["event_type"] == "order_item"
                                and r.get("item_status") == "cancelled")
    if not fulfilled_items or not cancelled_items:
        return
    fulfilled_rate = sum(1 for r in fulfilled_items if r["waste_flag"]) / len(fulfilled_items)
    cancelled_rate = sum(1 for r in cancelled_items if r["waste_flag"]) / len(cancelled_items)
    assert cancelled_rate > fulfilled_rate, \
        f"Expected cancelled waste rate ({cancelled_rate:.3f}) > fulfilled ({fulfilled_rate:.3f})"
```

- [ ] **Step 2: Run to confirm they fail**

```bash
pytest tests/test_orders.py::test_waste_flag_set_on_some_items_at_late_night tests/test_orders.py::test_cancelled_items_have_higher_waste_rate_than_fulfilled -v
```
Expected: FAIL — `waste_flag` is always `False`.

- [ ] **Step 3: Implement — add waste_flag logic to item construction**

In `_build_order` in `src/generator/domains/orders.py`, replace the item loop section that builds `item_rows`. Only the waste_flag and late_night lines change — add `late_night` before the loop and update the item dict. The diff is:

After `is_cancelled = should_cancel(...)`, add:
```python
    late_night = ctx.hour_of_day >= 20
```

Then in the item loop, replace `"waste_flag": False,` with:
```python
            waste_prob = 0.15 if is_cancelled else (0.03 if late_night else 0.02)
            "waste_flag": random.random() < waste_prob,
```

The full updated item dict inside the loop (showing all fields):
```python
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
```

- [ ] **Step 4: Run tests to verify**

```bash
pytest tests/test_orders.py -v
```
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/generator/domains/orders.py tests/test_orders.py
git commit -m "fix: waste_flag on cancelled (15%), late-night (3%), other (2%) (Fix 3)"
```

---

## Task 5: Fix 1 — Order Discounts (`orders.py`)

**Files:**
- Modify: `src/generator/domains/orders.py`
- Test: `tests/test_orders.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_orders.py`:

```python
def test_some_orders_have_discounts():
    ctx = _ctx()
    reg = _registry()
    all_orders = []
    for _ in range(30):
        all_orders.extend(r for r in generate_orders_for_tick(ctx, reg, tick_seconds=3600)
                          if r["event_type"] == "guest_order" and r["order_status"] == "fulfilled")
    discounted = [o for o in all_orders if o.get("discount_amount", 0) > 0]
    assert len(discounted) > 0, "Expected some discounted orders across 30 ticks"

def test_discounted_order_math_is_correct():
    ctx = _ctx()
    reg = _registry()
    all_rows = []
    for _ in range(30):
        all_rows.extend(generate_orders_for_tick(ctx, reg, tick_seconds=3600))
    discounted_orders = [r for r in all_rows if r["event_type"] == "guest_order"
                         and r.get("discount_amount", 0) > 0]
    for o in discounted_orders:
        assert abs(o["total_amount"] - (o["subtotal"] + o["tax_amount"])) < 0.02, \
            f"total {o['total_amount']} != subtotal {o['subtotal']} + tax {o['tax_amount']}"

def test_line_net_amount_equals_gross_minus_discount():
    ctx = _ctx()
    reg = _registry()
    rows = []
    for _ in range(10):
        rows.extend(generate_orders_for_tick(ctx, reg, tick_seconds=3600))
    for item in rows:
        if item["event_type"] == "order_item":
            expected = round(item["line_gross_amount"] - item["line_discount_amount"], 2)
            assert abs(item["line_net_amount"] - expected) < 0.02, \
                f"line_net {item['line_net_amount']} != gross {item['line_gross_amount']} - disc {item['line_discount_amount']}"
```

Also update the existing `test_total_amount_equals_subtotal_plus_tax` — the subtotal field now reflects net-of-discount, so the equality still holds. No change needed to that test.

- [ ] **Step 2: Run to confirm new tests fail**

```bash
pytest tests/test_orders.py::test_some_orders_have_discounts tests/test_orders.py::test_discounted_order_math_is_correct -v
```
Expected: FAIL — `discount_amount` is always `0.0`.

- [ ] **Step 3: Add `_member_tier` helper and discount logic to `_build_order`**

Add this helper at module level in `src/generator/domains/orders.py`, after the imports:

```python
def _member_tier(member_id: int) -> str:
    spend = (member_id * 47) % 4000
    if spend >= 3500:
        return "platinum"
    if spend >= 1500:
        return "gold"
    if spend >= 500:
        return "silver"
    return "bronze"
```

Then in `_build_order`, after the `subtotal = round(subtotal, 2)` line (after the item loop), add the discount block before computing tax:

```python
    # Apply discount to non-cancelled orders (~8–20% of orders)
    discount_amount = 0.0
    disc_rate = 0.20 if is_member else 0.08
    if not is_cancelled and random.random() < disc_rate:
        disc_type = random.choices(
            ["app_promo", "coupon", "loyalty_promo"], weights=[50, 30, 20]
        )[0]
        if disc_type == "app_promo":
            discount_amount = round(subtotal * 0.10, 2)
        elif disc_type == "coupon":
            discount_amount = round(random.uniform(2.0, 5.0), 2)
        else:
            tier = _member_tier(member_id) if is_member else "bronze"
            pct = 0.15 if tier in ("gold", "platinum") else 0.10
            discount_amount = round(subtotal * pct, 2)
        discount_amount = min(discount_amount, subtotal)

        # Distribute discount proportionally to line items
        for item in item_rows:
            item_disc = round(item["line_gross_amount"] / subtotal * discount_amount, 2) \
                if subtotal > 0 else 0.0
            item["line_discount_amount"] = item_disc
            item["line_net_amount"] = round(item["line_gross_amount"] - item_disc, 2)

        subtotal = round(sum(item["line_net_amount"] for item in item_rows), 2)

    tax = round(subtotal * _TAX_RATE, 2)
    total = round(subtotal + tax, 2)
```

Also update the `guest_order` dict to include `discount_amount`:
```python
        "discount_amount": discount_amount,
```
(it was already `0.0` before — now it reflects the actual discount)

- [ ] **Step 4: Run tests to verify**

```bash
pytest tests/test_orders.py -v
```
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/generator/domains/orders.py tests/test_orders.py
git commit -m "fix: apply discounts to ~8-20% of orders with correct line math (Fix 1)"
```

---

## Task 6: Fix 6 — Guest Account Status + Daily Churn (`guest.py`, `runner.py`)

**Files:**
- Modify: `src/generator/domains/guest.py`
- Modify: `src/generator/runner.py`
- Test: `tests/test_guest_loyalty_workforce.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_guest_loyalty_workforce.py`:

```python
def test_new_guest_registrations_have_varied_status():
    all_rows = []
    for _ in range(200):
        rows = generate_new_guest_profiles(unit_id=1, date_str="2025-09-19",
                                           growth_rate=0.008, base_pool=500)
        all_rows.extend(rows)
    statuses = {r["account_status"] for r in all_rows}
    assert "active" in statuses
    if len(all_rows) > 30:
        assert "inactive" in statuses, "Expected some inactive registrations across 200 runs"

def test_generate_guest_churn_emits_inactive_profiles():
    from src.generator.domains.guest import generate_guest_churn
    reg = _reg()
    rows = generate_guest_churn(unit_id=1, registry=reg, date_str="2025-09-19",
                                 churn_rate=0.05,  # high rate to guarantee output
                                 tick_ts=datetime(2025, 9, 19, 10, 0))
    assert len(rows) > 0
    for r in rows:
        assert r["event_type"] == "guest_profile"
        assert r["account_status"] == "inactive"
```

- [ ] **Step 2: Run to confirm they fail**

```bash
pytest tests/test_guest_loyalty_workforce.py::test_new_guest_registrations_have_varied_status tests/test_guest_loyalty_workforce.py::test_generate_guest_churn_emits_inactive_profiles -v
```
Expected: FAIL — `account_status` is always `"active"`, and `generate_guest_churn` doesn't exist.

- [ ] **Step 3: Implement — vary account_status in registrations**

In `src/generator/domains/guest.py`, replace `"account_status": "active"` with:
```python
            "account_status": (
                "suspended" if random.random() < 0.005
                else "inactive" if random.random() < 0.035
                else "active"
            ),
```

- [ ] **Step 4: Implement — add `generate_guest_churn` function**

Add to the bottom of `src/generator/domains/guest.py`:

```python
def generate_guest_churn(unit_id: int, registry, date_str: str,
                          churn_rate: float = 0.002,
                          tick_ts: datetime | None = None) -> list[dict]:
    """Emit ~0.2% of guest pool per unit per day as account deactivations."""
    pool = registry.guest_ids_for_unit(unit_id)
    n = max(0, round(len(pool) * churn_rate * random.gauss(1.0, 0.3)))
    rows = []
    for gid in random.sample(pool, min(n, len(pool))):
        rows.append({
            "event_type": "guest_profile",
            "event_id": _next_guest_id(),
            "event_ts": tick_ts,
            "guest_profile_id": gid,
            "unit_id": unit_id,
            "first_name": None,
            "last_name": None,
            "email": None,
            "phone": None,
            "zip_code": None,
            "created_date": date_str,
            "digital_account_id": gid,
            "account_status": "inactive",
        })
    return rows
```

- [ ] **Step 5: Add `guest_ids_for_unit` to `EntityRegistry`**

In `src/generator/entity_registry.py`, add this method inside the `EntityRegistry` class:

```python
    def guest_ids_for_unit(self, unit_id: int) -> list[int]:
        return self._guest_pool.get(unit_id, [])
```

- [ ] **Step 6: Wire churn into backfill loop in `runner.py`**

In `src/generator/runner.py`, update the imports at the top:
```python
from src.generator.domains.guest import generate_new_guest_profiles, generate_guest_churn
```

In `backfill_ticks`, inside the `if current.hour == 10:` block, add after `generate_new_guest_profiles`:
```python
                batch.extend(
                    generate_guest_churn(uid, registry, current.date().isoformat(), tick_ts=current)
                )
```

- [ ] **Step 7: Run tests to verify**

```bash
pytest tests/test_guest_loyalty_workforce.py -v
```
Expected: all PASS.

- [ ] **Step 8: Commit**

```bash
git add src/generator/domains/guest.py src/generator/runner.py src/generator/entity_registry.py tests/test_guest_loyalty_workforce.py
git commit -m "fix: varied account_status on registration + daily guest churn events (Fix 6)"
```

---

## Task 7: Fix 7a — Per-unit Price Index (`us_locations.py`, `entity_registry.py`, `orders.py`)

**Files:**
- Modify: `src/generator/reference/us_locations.py`
- Modify: `src/generator/entity_registry.py`
- Modify: `src/generator/domains/orders.py`
- Test: `tests/test_orders.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_orders.py`:

```python
def test_units_have_market_price_index():
    from src.generator.reference.us_locations import generate_units
    units = generate_units(10)
    for u in units:
        assert "market_price_index" in u, "Unit missing market_price_index"
        assert 0.85 <= u["market_price_index"] <= 1.25, \
            f"market_price_index {u['market_price_index']} out of range"

def test_aov_varies_across_units():
    """Units with different market_price_index should produce different average order values."""
    from src.generator.reference.us_locations import generate_units
    from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
    from src.generator.reference.seeder import build_financial_periods_data
    # Force two units with extreme price indices
    units = generate_units(2)
    units[0]["market_price_index"] = 0.85
    units[1]["market_price_index"] = 1.25
    reg = EntityRegistry(units=units, menu_items=get_menu_items(),
                          bom=get_recipe_ingredients(),
                          financial_periods=build_financial_periods_data(1))
    ctx0 = build_context(units[0]["unit_id"], datetime(2025, 9, 19, 19, 0), 1.0)
    ctx1 = build_context(units[1]["unit_id"], datetime(2025, 9, 19, 19, 0), 1.0)

    def mean_aov(ctx):
        totals = []
        for _ in range(20):
            rows = generate_orders_for_tick(ctx, reg, tick_seconds=3600)
            totals.extend(r["total_amount"] for r in rows
                          if r["event_type"] == "guest_order" and r["order_status"] == "fulfilled")
        return sum(totals) / len(totals) if totals else 0

    aov_low = mean_aov(ctx0)
    aov_high = mean_aov(ctx1)
    assert aov_high > aov_low, f"Expected high-index unit AOV ({aov_high:.2f}) > low-index ({aov_low:.2f})"
```

- [ ] **Step 2: Run to confirm they fail**

```bash
pytest tests/test_orders.py::test_units_have_market_price_index tests/test_orders.py::test_aov_varies_across_units -v
```
Expected: FAIL — `market_price_index` key does not exist on units.

- [ ] **Step 3: Add `market_price_index` to unit generation**

In `src/generator/reference/us_locations.py`, inside `generate_units`, add to the `units.append({...})` dict:
```python
            "market_price_index": round(random.uniform(0.85, 1.25), 4),
```

- [ ] **Step 4: Add `unit_price_index` method to `EntityRegistry`**

In `src/generator/entity_registry.py`, add inside the class:
```python
    def unit_price_index(self, unit_id: int) -> float:
        return self._unit_by_id[unit_id].get("market_price_index", 1.0)
```

- [ ] **Step 5: Apply price index in `_build_order`**

In `src/generator/domains/orders.py`, inside the item loop in `_build_order`, replace:
```python
        unit_price = registry.get_menu_item(mid)["base_price"]
        if channel == "3pd_delivery":
            unit_price += 0.75
```
with:
```python
        unit_price = round(registry.get_menu_item(mid)["base_price"]
                           * registry.unit_price_index(ctx.unit_id), 2)
        if channel == "3pd_delivery":
            unit_price = round(unit_price + 1.25, 2)  # also upgrades 3PD markup from $0.75 to $1.25
```

- [ ] **Step 6: Run tests to verify**

```bash
pytest tests/test_orders.py -v
```
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add src/generator/reference/us_locations.py src/generator/entity_registry.py src/generator/domains/orders.py tests/test_orders.py
git commit -m "fix: per-unit market_price_index (0.85-1.25) + 3PD markup raised to \$1.25 (Fix 7a+7b)"
```

---

## Task 8: Fix 7b — Catering AOV Uplift + Fix 7c — Quarterly Price Drift

**Files:**
- Modify: `src/generator/domains/orders.py` (catering num_items multiplier)
- Modify: `src/generator/reference/seeder.py` (build_item_price_data + seed ref.item_price)
- Modify: `src/generator/entity_registry.py` (item_prices optional param + item_price_multiplier method)
- Modify: `src/generator/domains/orders.py` (apply price multiplier per menu_item + period)
- Test: `tests/test_orders.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_orders.py`:

```python
def test_catering_orders_have_higher_aov_than_carryout():
    from src.generator.reference.us_locations import generate_units
    from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
    from src.generator.reference.seeder import build_financial_periods_data
    reg = EntityRegistry(units=generate_units(3), menu_items=get_menu_items(),
                          bom=get_recipe_ingredients(),
                          financial_periods=build_financial_periods_data(1))
    ctx = build_context(1, datetime(2025, 9, 19, 12, 0), 1.0)

    def mean_aov_for_channel(ch):
        totals = []
        for _ in range(100):
            rows = generate_orders_for_tick(ctx, reg, tick_seconds=3600)
            totals.extend(r["total_amount"] for r in rows
                          if r["event_type"] == "guest_order"
                          and r["channel"] == ch and r["order_status"] == "fulfilled")
        return sum(totals) / len(totals) if totals else 0

    aov_catering = mean_aov_for_channel("catering")
    aov_carryout = mean_aov_for_channel("carryout")
    if aov_catering > 0 and aov_carryout > 0:
        assert aov_catering > aov_carryout * 2, \
            f"Catering AOV ({aov_catering:.2f}) should be >2x carryout ({aov_carryout:.2f})"
```

Add to `tests/test_seeder.py` (or create `tests/test_seeder.py` if it doesn't exist with the needed imports):

```python
def test_build_item_price_data_returns_valid_multipliers():
    from src.generator.reference.seeder import build_item_price_data, build_financial_periods_data
    periods = build_financial_periods_data(6)
    rows = build_item_price_data(periods)
    assert len(rows) > 0
    for row in rows:
        assert "menu_item_id" in row
        assert "financial_period_id" in row
        assert 0.7 <= row["price_multiplier"] <= 1.4, \
            f"price_multiplier {row['price_multiplier']} out of bounds"
```

- [ ] **Step 2: Run to confirm they fail**

```bash
pytest tests/test_orders.py::test_catering_orders_have_higher_aov_than_carryout tests/test_seeder.py::test_build_item_price_data_returns_valid_multipliers -v
```
Expected: FAIL.

- [ ] **Step 3: Add catering num_items multiplier to `_build_order`**

In `src/generator/domains/orders.py`, immediately after:
```python
    num_items = random.choices([1, 2, 3, 4, 5], weights=[20, 35, 25, 15, 5])[0]
```
add:
```python
    if channel == "catering":
        num_items *= random.randint(3, 8)
```

- [ ] **Step 4: Add `build_item_price_data` to `seeder.py`**

Add to `src/generator/reference/seeder.py` after `build_suppliers_data`:

```python
def build_item_price_data(financial_periods: list[dict]) -> list[dict]:
    """Per (menu_item, financial_period) price multiplier that drifts ±3-6% per quarter."""
    from src.generator.reference.menu_catalog import get_menu_items
    items = get_menu_items()
    rows = []
    for item in items:
        multiplier = 1.0
        for period in sorted(financial_periods, key=lambda p: p["start_date"]):
            drift = random.uniform(-0.03, 0.06)
            multiplier = round(max(0.7, min(1.4, multiplier * (1 + drift))), 4)
            rows.append({
                "menu_item_id": item["menu_item_id"],
                "financial_period_id": period["financial_period_id"],
                "price_multiplier": multiplier,
            })
    return rows
```

In `seed_all`, add before the stub table creation:
```python
    write(build_item_price_data(build_financial_periods_data(backfill_months)), "item_price")
```

- [ ] **Step 5: Add `item_prices` param and `item_price_multiplier` method to `EntityRegistry`**

In `src/generator/entity_registry.py`, update `__init__` signature:
```python
    def __init__(self, units: list[dict], menu_items: list[dict], bom: list[dict],
                 financial_periods: list[dict], num_guests_per_unit: int = 200,
                 item_prices: list[dict] | None = None):
```

Add to `__init__` body after `self._periods = ...`:
```python
        self._item_price_mult: dict[tuple[int, int], float] = {}
        for row in (item_prices or []):
            self._item_price_mult[(row["menu_item_id"], row["financial_period_id"])] = row["price_multiplier"]
```

Add method:
```python
    def item_price_multiplier(self, menu_item_id: int, financial_period_id) -> float:
        return self._item_price_mult.get((menu_item_id, financial_period_id), 1.0)
```

Update `from_spark` to load the table:
```python
    @classmethod
    def from_spark(cls, spark, catalog: str, backfill_months: int = 12):
        units = [r.asDict() for r in spark.table(f"{catalog}.ref.unit").collect()]
        menu = [r.asDict() for r in spark.table(f"{catalog}.ref.menu_item").collect()]
        bom = [r.asDict() for r in spark.table(f"{catalog}.ref.recipe_ingredient").collect()]
        periods = [r.asDict() for r in spark.table(f"{catalog}.ref.financial_period").collect()]
        item_prices = [r.asDict() for r in spark.table(f"{catalog}.ref.item_price").collect()]
        return cls(units=units, menu_items=menu, bom=bom, financial_periods=periods,
                   item_prices=item_prices)
```

- [ ] **Step 6: Apply price drift multiplier in `_build_order`**

In `src/generator/domains/orders.py`, inside the item loop, update the price line from Task 7 to:
```python
        base_price = registry.get_menu_item(mid)["base_price"]
        price_mult = registry.item_price_multiplier(mid, fp_id)
        market_idx = registry.unit_price_index(ctx.unit_id)
        unit_price = round(base_price * price_mult * market_idx, 2)
        if channel == "3pd_delivery":
            unit_price = round(unit_price + 1.25, 2)
```

- [ ] **Step 7: Run full test suite**

```bash
pytest tests/ -v
```
Expected: all PASS (55+ tests).

- [ ] **Step 8: Commit**

```bash
git add src/generator/domains/orders.py src/generator/reference/seeder.py src/generator/entity_registry.py tests/test_orders.py tests/test_seeder.py
git commit -m "fix: catering AOV 3-8x + quarterly price drift via ref.item_price (Fix 7b+7c)"
```

---

## Task 9: Final Verification + Rebuild Note

- [ ] **Step 1: Run the full test suite clean**

```bash
pytest tests/ -v --tb=short 2>&1 | tail -20
```
Expected: all tests pass, no failures.

- [ ] **Step 2: Verify no regressions in existing tests**

Key tests to confirm still pass:
- `test_total_amount_equals_subtotal_plus_tax` — subtotal is now post-discount, tax is on post-discount subtotal, so equality holds
- `test_order_items_reference_order` — all items (including cancelled) still reference a valid guest_order_id
- `test_payment_references_order` — payments only for non-cancelled, still correct

- [ ] **Step 3: Create branch, merge, rebuild data**

Since this plan touches more than 2 files, ensure changes are on a branch:

```bash
git checkout -b feat/phase-25-generator-realism
# cherry-pick or squash the commits from this plan if needed
```

Then destroy and regenerate clean data by running the Databricks `setup_job`:
- This triggers: setup → start_pipeline (full_refresh) → backfill → unpause_generator
- Backfill generates 1 month of corrected data (~12.6M rows)
- After backfill, verify distributions in silver tables via Databricks SQL:

```sql
-- Verify discounts are present
SELECT COUNT(*) FILTER (WHERE discount_amount > 0) / COUNT(*) AS discount_rate
FROM jmrdemo.silver.guest_order;
-- Expect ~0.10 (10% across mix of members/non-members)

-- Verify waste categories are diverse
SELECT waste_category, COUNT(*) FROM jmrdemo.silver.waste_log GROUP BY 1;
-- Expect 5 categories

-- Verify loyalty has both earn and redeem
SELECT transaction_type, COUNT(*) FROM jmrdemo.silver.loyalty_transaction GROUP BY 1;
-- Expect both "earn" and "redeem"
```
