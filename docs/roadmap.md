# QSR Synthetic Data Generator — Roadmap

## Current State (as of 2026-05-18)

- Backfill complete: 1-month window (Apr 18 – May 18, 2026), ~12.6M rows across 5 staging tables
- DLT pipeline: 19 silver tables populated, triggered per live generator run
- Live generator: running every minute, Poisson demand model, ~2,600 rows/tick
- Setup job: fully automated (setup → pipeline → backfill → unpause_generator)
- Auto-rehydrate: backfill resumes from MAX(event_ts) on re-run

---

## Phase 2 — Catalog Enrichment + Genie Space

Source: [amralieg/vibe-business-data-models/restaurants/mvm_v1](https://github.com/amralieg/vibe-business-data-models/tree/main/restaurants/mvm_v1)

### 2.1 Table & Column Descriptions

**What:** Apply full-sentence descriptions to every silver table and column in Unity Catalog.

**How:** Parse the 14 DDL files from the data model repo. Each column has a `COMMENT` clause. Apply via:
```sql
COMMENT ON TABLE jmrdemo.silver.guest_order IS '...';
ALTER TABLE jmrdemo.silver.guest_order ALTER COLUMN guest_order_id COMMENT '...';
```

**Automate:** New `apply_catalog_metadata` notebook task in setup_job. Runs after `start_pipeline`.

---

### 2.2 PK/FK Constraints

**What:** Register primary key and foreign key relationships on all silver tables so UC lineage and Genie can understand entity relationships.

**How:**
- PKs: named `CONSTRAINT pk_<table> PRIMARY KEY(...)` inline — apply via `ALTER TABLE ... ADD CONSTRAINT`
- FKs: 827 cross-domain constraints in `restaurants_cross_domain_foreign_keys_v1_mvm.sql` — apply via `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY (...) REFERENCES ...`

**Automate:** Same `apply_catalog_metadata` notebook. Run after descriptions.

---

### 2.3 UC Metric Views

**What:** Create `metrics` schema views on top of gold tables for direct use in Genie and BI tools.

**Planned views (from design spec):**
- `metrics.unit_daily_summary` — orders, revenue, SOS compliance per unit per day
- `metrics.loyalty_tier_distribution` — member count and avg spend by tier
- `metrics.inventory_waste_rate` — waste as % of usage by unit/week
- `metrics.staff_utilization` — scheduled vs actual hours, no-show rate
- `metrics.channel_mix_trend` — 3PD vs own delivery vs carryout share over time

**Automate:** New `create_metric_views` notebook task in setup_job. Runs after `apply_catalog_metadata`.

---

### 2.4 Genie Space

**What:** A pre-configured Genie Space scoped to the QSR dataset with curated instructions, verified questions, and suggested queries.

**How:** Genie Spaces are not DAB-manageable — create via REST API:
```
POST /api/2.0/genie/spaces
```
Include:
- Title and description
- Table references (all silver + metrics views)
- Curated instructions (domain context: Domino's-style QSR, channel mix, loyalty tiers)
- Seed questions: "Which units have the highest SOS breach rate?", "Show me loyalty tier distribution this month", etc.

**Automate:** New `create_genie_space` notebook task in setup_job. Runs last (after metric views exist).

---

## Updated Setup Job Task Order (Phase 2)

```
setup
  └── start_pipeline (full_refresh=true)
  └── backfill
        └── apply_catalog_metadata   ← NEW: descriptions + PK/FK
              └── create_metric_views  ← NEW: UC metric views on gold
                    └── create_genie_space  ← NEW: Genie Space via REST API
                          └── unpause_generator
```

---

## Phase 2.5 — Data Quality Fixes (Generator Realism)

Six data quality gaps identified via silver table analysis. All fixes are in the generator layer — no DLT or schema changes needed.

### Fix 1 — Order Discounts (`orders.py`)

**Gap:** `line_discount_amount` and `discount_amount` are 0.00 on 100% of records.

**Fix:** ~12% of orders receive a discount (app promo, coupon, loyalty promo):
- Pick discount type: app (10% off subtotal), coupon (flat $2–$5), loyalty promo (15% off for gold/platinum)
- Apply proportionally to `line_discount_amount` on each item; set `discount_amount` on `guest_order`
- Recalculate `line_net_amount = line_gross - line_discount`, `subtotal`, `total_amount`
- Discount rate higher for loyalty members (20% vs 8% for non-members)

---

### Fix 2 — Order Item Status (`orders.py`)

**Gap:** `item_status` is "fulfilled" for 100% of records. Cancelled orders still emit items as fulfilled.

**Fix:**
- If parent order is cancelled → `item_status = "cancelled"` on all items
- ~1% of fulfilled-order items get `item_status = "refunded"` (post-fulfillment refund)
- Remaining fulfilled as before

---

### Fix 3 — Waste Flags on Order Items (`orders.py`)

**Gap:** `waste_flag = false` on all 4M+ items despite 35K waste_log events in inventory.

**Fix:** ~2% of order items get `waste_flag = True`:
- Higher rate on cancelled orders (~15% of cancelled items → waste)
- Higher rate at end of day (hour >= 20): +1.5× multiplier (matches `should_waste` in entropy.py)
- Correlates item-level waste with the existing inventory waste_log events

---

### Fix 4 — Loyalty Redemption (Burn) Transactions (`loyalty.py`)

**Gap:** `loyalty_transaction.transaction_type` is exclusively "earn". Reward redemptions exist in `reward_redemption` table but no corresponding debit in `loyalty_transaction`.

**Fix:** When generating a `reward_redemption` event, also emit a `loyalty_transaction` with:
- `transaction_type = "redeem"`
- `points_delta = -redeem_points` (negative — points deducted)
- Same `member_id`, `guest_order_id`, `transaction_at`

Result: loyalty_transaction will have both earn and redeem records; burn rate ~8% of orders with members.

---

### Fix 5 — Waste Categories (`inventory.py`)

**Gap:** `waste_category` is "overproduction" for 100% of waste events.

**Fix:** Sample from realistic category distribution:
| Category | Weight |
|---|---|
| overproduction | 50% |
| spoilage | 25% |
| theft | 10% |
| expired | 10% |
| damaged | 5% |

---

### Fix 6 — Guest Account Status (`guest.py`)

**Gap:** `account_status = "active"` for 100% of guest profiles.

**Fix (two parts):**
1. New guest registrations: ~3% created as `"inactive"` (email unverified), ~0.5% as `"suspended"` (fraud flag)
2. Daily churn events: ~0.2% of existing guest pool per unit per day generates a profile update event with `account_status = "inactive"` — models natural churn/account deletion

---

### Implementation Notes

- All 6 fixes are isolated to `src/generator/domains/` — no schema, DLT, or job changes
- Fixes 1–5 apply to both backfill and live modes (same code path)
- Fix 6 applies to `generate_new_guest_profiles()` (new registrations) and a new `generate_guest_churn()` daily function in `runner.py`
- After implementing: destroy existing data, run setup_job to regenerate clean 1-month backfill with correct distributions

---

## Phase 3 — External Signal Integration (from original design spec)

- `ref.weather_conditions` — currently empty stub; populate with weather API
- `ref.local_events` — currently empty stub; populate with events API
- Causal model upgrade: weather + local events feed into `CausalContext` as real inputs
- Marketing domain: campaigns, promotions, loyalty program config

---

## Open Issues / Known Gaps

| Issue | Status |
|---|---|
| `shift`/`time_punch` not generated in live ticks (only backfill daily @ 10:00) | Known — by design; workforce is daily |
| `digital_account` events never generated | Gap — `guest.py` generates `guest_profile` but not `digital_account` |
| `order_modifier` event type in DOMAIN_TABLE_MAP but never generated | Gap — no modifier generator implemented |
| `stock_transfer` and `adjustment` in DOMAIN_TABLE_MAP but never generated | Gap — inventory domain incomplete |
| `receiving_order` only generated at 10:00 AM daily | By design — daily receiving window |
| DLT pipeline in triggered mode (dev target forces non-continuous) | Known limitation — pipeline triggered per generator run |
