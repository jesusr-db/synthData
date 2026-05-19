# QSR Synthetic Data Generator — Roadmap

## Current State (as of 2026-05-18)

- Backfill complete: 1-month window, ~12.6M rows across 5 staging tables
- DLT pipeline: 14 silver + 4 gold tables, triggered per live generator run
- Live generator: running every minute, Poisson demand model
- Setup job: 7-task graph, fully automated end-to-end
- **Phase 2.5 complete:** 7 generator realism fixes — discounts, item status, waste flags, loyalty redeem, waste categories, guest churn, AOV variance
- **Phase 2 complete:** Catalog metadata (comments + PK/FK), 5 metric views, Genie Space

---

## ✅ Phase 2.5 — Generator Realism Fixes

> Branch: `feat/phase-25-generator-realism` — 8 commits, 71 tests

All fixes are in `src/generator/domains/` and `src/generator/reference/`. No schema or DLT changes.

### Fix 1 — Order Discounts (`orders.py`) ✅

~12% of fulfilled orders receive a discount (app promo, coupon, or loyalty promo). Discount distributed proportionally across line items. `line_net_amount` and `subtotal` recalculated correctly. Members get 20% discount rate vs 8% for non-members.

### Fix 2 — Order Item Status (`orders.py`) ✅

Cancelled orders emit all items with `item_status = "cancelled"`. ~1% of fulfilled-order items get `item_status = "refunded"`. Previously all items were `"fulfilled"` regardless of order outcome.

### Fix 3 — Waste Flags on Order Items (`orders.py`) ✅

`waste_flag` now set on ~2% of items; ~15% of cancelled-order items; ~3% at late night (hour ≥ 20). Previously always `false`.

### Fix 4 — Loyalty Redemption Transactions (`loyalty.py`) ✅

Every `reward_redemption` event now emits a paired `loyalty_transaction` with `transaction_type = "redeem"` and `points_delta < 0`. Previously all loyalty transactions were earn-only.

### Fix 5 — Waste Categories (`inventory.py`) ✅

`waste_category` sampled from weighted distribution: overproduction 50%, spoilage 25%, theft 10%, expired 10%, damaged 5%. Previously always `"overproduction"`.

### Fix 6 — Guest Account Status (`guest.py`, `runner.py`) ✅

New registrations: ~3% `"inactive"`, ~0.5% `"suspended"`, remainder `"active"`. Daily churn: ~0.2% of guest pool per unit emits profile update to `"inactive"`. Previously 100% `"active"`.

### Fix 7 — AOV Variance (`orders.py`, `entity_registry.py`, `us_locations.py`, `seeder.py`) ✅

Three levers implemented:
- **7a:** `market_price_index` (0.85–1.25) per unit applied to all item prices; 3PD markup raised to $1.25
- **7b:** Catering orders multiply `num_items` by 3–8× for realistic bulk-order AOV
- **7c:** `ref.item_price` table seeds per-(menu_item, period) price multiplier drifting ±3–6% per quarter

---

## ✅ Phase 2 — Catalog Enrichment + Genie Space

> Branch: `feat/phase-2-catalog-enrichment` — 5 commits

### 2.1 Table & Column Descriptions ✅

`src/setup/apply_catalog_metadata.py` applies `COMMENT ON TABLE` and `ALTER COLUMN COMMENT` to all 14 silver tables. Subset of highest-value columns annotated with business-friendly descriptions sourced from the MVM v1 data model.

### 2.2 PK/FK Constraints ✅

Same notebook adds informational (NOT ENFORCED) primary key and foreign key constraints to all silver tables. Unity Catalog uses these for lineage and Genie relationship inference.

### 2.3 UC Metric Views ✅

`src/setup/create_metric_views.py` creates 5 views in `jmrdemo.metrics`:

| View | Description |
|---|---|
| `unit_daily_summary` | Orders, revenue, AOV, SOS breach % per unit per day |
| `loyalty_tier_distribution` | Member counts, earn/redeem by tier and month |
| `inventory_waste_rate` | Waste qty/cost as % of inventory usage by unit/week/SKU |
| `staff_utilization` | Scheduled vs actual hours, no-show rate per unit per day |
| `channel_mix_trend` | Order share and revenue share by channel per unit per week |

All views use `CREATE OR REPLACE VIEW` — safe to re-run.

### 2.4 Genie Space ✅

`src/setup/create_genie_space.py` creates a Genie Space via `POST /api/2.0/genie/spaces`. Idempotent (skips if space with same title exists). Includes:
- 14 silver tables + 5 metrics views as table references
- Domain instructions (channel mix, loyalty tiers, SOS targets, price drift, waste patterns)
- 10 seed questions covering SOS, loyalty, channel mix, waste, staffing, AOV

### Updated Setup Job ✅

```
setup → start_pipeline → apply_catalog_metadata → create_metric_views → create_genie_space
setup → backfill ──────────────────────────────────────────────────────────────────────────┐
                                                                                            └── unpause_generator
```

---

## Deployment Notes

See `docs/handoff.md` for full deploy + test instructions. Summary:

- **Phase 2.5 requires a full rebuild** — `ref.item_price` is a new table that `EntityRegistry.from_spark()` loads; the generator job will fail if it doesn't exist.
- **Phase 2 is additive** — can be applied to a live workspace without destroying data.
- **Both together:** merge → deploy → destroy_job → setup_job.

---

## Phase 3 — External Signal Integration

> Status: Not started

- `ref.weather_conditions` — empty stub; populate with NOAA or weather API
- `ref.local_events` — empty stub; populate with events API or manual seeding
- Causal model upgrade: weather + local events feed into `CausalContext` as real demand multipliers
- Marketing domain: campaigns, promotions, loyalty program configuration

---

## Open Issues / Known Gaps

| Issue | Status |
|---|---|
| `digital_account` event type never generated | Gap — `guest.py` generates `guest_profile` but not `digital_account` |
| `order_modifier` event type in DOMAIN_TABLE_MAP but never generated | Gap — no modifier generator |
| `stock_transfer` and `adjustment` in DOMAIN_TABLE_MAP but never generated | Gap — inventory domain incomplete |
| `receiving_order` only generated at 10:00 AM daily | By design — daily receiving window |
| `shift`/`time_punch` only generated in backfill daily ticks, not live | By design — workforce is daily |
| DLT pipeline in triggered mode | Known — DAB dev target forces non-continuous; use prod target for continuous |
| Genie Space API endpoint may require workspace preview flag | Verify before prod deploy |
