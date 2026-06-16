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

> Status: In progress

### 3.1 Weather Data (Open-Meteo + NOAA Alerts) (done)
`src/refresh/openmeteo_client.py` fetches 30-day historical + 14-day forecast from Open-Meteo (no key). `src/refresh/noaa_client.py` fetches active NWS alerts per state. Combined into `ref.weather_conditions` with `demand_multiplier` and `channel_shift_delivery` pre-computed from `conf/weather_event_multipliers.yml`.

### 3.2 Local Events (Nager.Date + Ticketmaster + SeatGeek) (done)
`src/refresh/nager_client.py` provides federal/state holidays (no key). `src/refresh/events_client.py` fetches major sports + concerts from Ticketmaster and SeatGeek (optional — key-gated, graceful skip if absent). Events land in `ref.local_events` with `est_demand_multiplier`.

### 3.3 Demand Model Integration (done)
`CausalContext.build_context()` accepts optional `weather_event_data` dict. `runner.backfill_ticks()` accepts optional `weather_event_lookup: dict[(metro_area, date), dict]`. `main.py` loads the lookup from ref tables once per run and passes it through. No-data fallback is silent (multiplier=1.0).

### 3.4 Demand Risk Forecast View (done)
`metrics.demand_risk_forecast` joins units × weather × events for the next 14 days. Labels each (unit, date) as `demand_risk`, `capacity_risk`, or `normal`. Queryable from Genie Space: "Which units have the highest demand risk this week?"

### 3.5 Daily Refresh Job (done)
`resources/refresh_weather_events.yml` declares a DAB-managed job on daily cron (05:00 UTC). `setup_job.yml` adds `initial_weather_refresh` task (after `setup`, before `backfill`) so data is available on day 1.

### Remaining Phase 3 Work
- Marketing domain: campaigns, promotions, loyalty program configuration
- Causal model upgrade: weather + events as statistically calibrated multipliers (current values are informed estimates)

---

## Phase 4 — Driver Tracking & Last-Mile Delivery

> Status: Proposed (brainstorm complete — see `research/driver-data-integration_2026-06-15.md`)

Add a **driver entity** (profiles) plus **driver location during delivery** (GPS ping tracks) and blend it into the existing tick-based, deterministic, domain-based model. Goal: a last-mile / delivery-ops narrative (live driver map, ETA accuracy, on-time %, driver utilization) that can also feed the **twins** digital-twin app.

### Architectural fit — validated by the twins app

The twins app (`gitrepos_FY27/twins`) already implements driver tracking on the *same* stateless pattern this generator uses. `twins/datagen/generators/generate_canonical_dataset.py` eagerly emits a delivery's **entire** event sequence in one pass with forward timestamps:

```
order_created → started → finished → ready → driver_arrived
   → picked_up (carries route_json polyline)
   → N × driver_ping  (lat/lon interpolated along route at PING_INTERVAL_SEC)
   → delivered
```

There is **no stateful tick simulator** — the "real-time map" is a serving-layer illusion (Lakebase continuous sync + a FastAPI endpoint filtering pings by `ts` where `delivered_at IS NULL`). This is exactly the "eager full-track generation at order-creation time" approach, which keeps synthData's stateless / idempotent (`make_id`) per-tick contract intact. **Twins' geo helpers (`generate_jittered_route`, `random_customer_location`, `haversine_miles`, Manhattan road-factor, land-polygon rejection sampling) can be ported directly** rather than reinvented.

### What the deliveries route between (real entities vs. synthesized)

| Leg | Today in synthData | Action |
|---|---|---|
| **Store origin** | ✅ Real — `us_locations.py` units have `lat`/`lon`, `unit_name`, `metro_area` | Use as-is (note: metro centroid + ±0.3° jitter, not literal street address) |
| **Customer identity** | ✅ Real — orders carry `profile_id` + `member_id` (`orders.py:30-31`) | Ride on existing linkage |
| **Customer destination** | ❌ Gap — `guest_profile` has only `zip_code`, and it's a raw `Faker.zipcode()` not geo-consistent with the store metro (`guest.py:27`) | **Prerequisite:** persist a stable `lat`/`lon` (+ realistic address, metro-consistent zip) on `guest_profile`, derived deterministically from the home store + seeded offset via `make_id` |

Persisting a stable customer location makes deliveries coherent (real store → real customer → that customer's fixed address) *and* fixes the today-broken random zip as a side benefit.

### Recommended approach — Option A (new `drivers.py` domain, profiles + interpolated GPS pings)

- New `src/generator/domains/drivers.py` — `generate_driver_profiles()` on the daily 10:00 hook (like `generate_shift_events`); `generate_driver_pings_for_delivery()` emits the eager ping track per `own_delivery` order.
- **Scope driver/GPS to `own_delivery` only** (~16% of channel mix) — leave opaque `3pd_delivery` alone. Realistic *and* controls ping-table volume.
- New event types `driver_profile` + `driver_location` routed via `DOMAIN_TABLE_MAP` to a **dedicated `staging.driver_events` table** (don't widen the sparse `order_events`).
- New silver tables in `mvm_pipeline.py`: `driver_profile` (CDC SCD-1, like `guest_profile`), `driver_location` (streaming append, partition by date). Optional gold: `delivery_tracking_summary` (ping count, distance, ETA error, on-time flag), `driver_utilization_daily`.
- `entity_registry.py` gains a per-unit `driver_pool` + `random_driver_id(unit_id)`, analogous to the guest pool.

### Suggested phasing

- **Phase 4.1** — Customer geo prerequisite (`guest.py` + seeder) + ported `geo.py` + `drivers.py` profiles + `driver_id` on `delivery_order` + eager ping track with a coarse cadence + `driver`/`driver_profile`/`driver_location` plumbing. Shippable, demoable from Delta tables.
- **Phase 4.2** — Richer interpolation, ETA-error gold table, geofence-arrival events, Genie space / metric view for the delivery-ops narrative.
- **Phase 4.3 (optional)** — Lakebase sync target (`orders_current_state` / `driver_locations`) + wall-clock live alignment so the existing twins app can point directly at synthData. synthData already syncs to Lakebase (`build_feature_tables.py`), so this reuses an established path.
- **Deferred** — stateful in-flight deliveries, multi-order batching, real road-network routing (breaks the stateless tick contract; YAGNI for a synthetic demo).

### Top risks

1. **Ping-table row-volume blow-up** — `driver_location` will be the largest table. Mitigate: cap pings/delivery (~8–15), `own_delivery` scope, date partitioning, possibly reduced ping density in backfill vs. live. Decide cadence in the spec, not in code.
2. **Determinism / staging-routing seam** — put pings in their own staging table; match `orders.py`'s existing "idempotent IDs (`make_id`), plausible unseeded content" convention rather than inventing a new per-ping seeding scheme.

### Key files

- **New:** `src/generator/domains/drivers.py`, `src/generator/geo.py` (ported from twins), `tests/test_drivers.py`.
- **Edit:** `src/generator/domains/guest.py` (+ stable customer geo), `reference/seeder.py`, `reference/us_locations.py`, `entity_registry.py`, `runner.py`, `domains/orders.py` (`driver_id` on `delivery_order`), `main.py` (`DOMAIN_TABLE_MAP`), `src/pipeline/mvm_pipeline.py`.
- **DAB / setup:** `resources/destroy_job.yml`, `src/setup/setup_notebook.py` (staging DDL), `src/setup/apply_governance.py` (driver PII masking), `databricks.yml` (`driver_count_per_unit` var).
- **Tests to extend:** `test_orders.py`, `test_entity_registry.py`, `test_runner.py`, `test_seeder.py`, `test_guest_loyalty_workforce.py`, `test_us_locations.py`.

---

## Phase 5 — OTel Live-Order Integration (dual-source order model)

> Status: Proposed (data-grounded analysis complete — based on profiling `jmrdemo.zerobus.otel_*`, 2026-06-15)

Blend the **live order telemetry** emitted by the PizzaTel storefront (the OpenTelemetry demo, already wired to the recommender and exporting to `jmrdemo.zerobus.otel_*` via Zerobus) into synthData's order model, so downstream silver/gold tables consume **synth-generated and real OTel orders interchangeably**. Goal: a real-time "live order overlay" (actual on-time %, prep-time vs SOS, live order map) layered on top of the synthetic baseline — not a bulk data source.

### Source data (profiled)

| Table | Rows | Role |
|---|---|---|
| `otel_spans` | ~494K | Order signal lives here (traces) |
| `otel_logs` | ~372K | App logs, correlated by `trace_id` |
| `otel_metrics` | ~6.0M | Service RED metrics |

Rolling ~3-day window (Jun 12–15 at time of analysis). Order-relevant services: `checkout`, `cart`, `payment`, `shipping`, and a **custom `order-tracker`** purpose-built to emit QSR-semantic orders.

### The key fit — `order-tracker` already speaks synthData's order language

The `order-tracker received order` span carries: `order.id`, `order.store_id`, `order.channel`, `order.skus` (e.g. `["1 x3"]`), `order.item_count`, `order.total_quantity`, `order.prep_seconds`, `order.location.{state,city,zip}`, and **`sos.target_seconds=1800`** — identical to synthData's 30-min delivery SOS target. Its lifecycle stages map near 1:1 to `status_event`:

| OTel `order-tracker` stage | synthData `status_event` |
|---|---|
| Prep → Bake | placed → preparing |
| QualityCheck | preparing |
| ReadyForPickup / OutForDelivery | ready |
| Delivered | fulfilled |

### Span → silver mapping

| Silver table | OTel source span | Fields available | Gaps |
|---|---|---|---|
| `guest_order` | `CheckoutService/PlaceOrder` + `order-tracker received order` | order.id, store_id, amount, items.count, channel, shipping.amount, currency, location | no `member_id`, `franchisee_id`/`region_id`, discount/tax split |
| `order_item` | `order.skus` + `cart AddItem` | menu_item_id, quantity (**SKU space already == `menu_item_id`**) | no per-line price/discount |
| `status_event` | `order-tracker stage: *` | prior→current state, dwell (span nanos), sos.target | richer vocab than synth (Bake/QualityCheck) |
| `payment` | `PaymentService/Charge` | app.payment.amount | no tender_type/settlement |
| `delivery_order` | `shipping` + `order.shipping.tracking.id` | tracking id, prep_seconds | est/actual delivery approximate |

### Two hard constraints (from the data)

1. **Live overlay, not a data source.** Only ~**45 distinct well-formed orders** (real `app.order.amount`); the other ~2,100 `PlaceOrder` spans are **load-generator noise** (`amount=0.0`, `fee-test`/`c2-verify` user IDs). `order-tracker` saw ~39 orders in 3 days. Treat OTel as a continuous tail, never a backfill.
2. **ID-space mismatch.** OTel uses **UUID** `order.id`/`store_id`/`user.id`; synthData uses **BIGINT `make_id`** keys. No natural join — solved by namespacing via the existing pattern: `make_id("otel", order.id)`, etc.

### Recommended approach — Option A: envelope adapter (single shared staging table)

Normalize OTel into synthData's **existing `order_events` envelope** + a `source` discriminator, so the silver DLT tables (`readStream.table(staging.order_events).filter(event_type=…)`) pick it up with a ~1-line change each.

```
                    ┌─ synth generator ──→ staging.order_events  (source='synth')
otel_spans ──→ [OTel order adapter] ──→ staging.order_events  (source='otel')
  (DLT streaming view: filter order spans,        │
   reshape to envelope, namespace IDs via make_id)│
                                                   ▼
                          existing silver DLT tables (+ new `source` column)
                                                   ▼
                                      gold / metrics (union for free; WHERE source='otel')
```

- New `src/pipeline/otel_order_adapter.py` DLT streaming view: filter order-tracker/checkout/cart/payment spans, reshape to the 5 `event_type`s, parse `order.skus` → `order_item` rows.
- **ID bridge:** `guest_order_id = make_id("otel", order.id)`, `unit_id = make_id("otel-store", store_id)` (or map `order.location.zip` → nearest `ref.unit`), `profile_id = -1` cold-start (or `make_id("otel-user", user.id)` for a stable external customer). Guarantees no PK collision with synth keys.
- Add `source STRING` (`'synth'`/`'otel'`) to the envelope DDL + each silver table; carry it through the `select`.

**Rejected alternatives:** parallel silver tables + union views (doubles tables, forces union in every gold query); keep OTel fully separate (loses the one-coherent-order-model benefit).

### Decisions for the spec (don't bake in blind)

1. **Store mapping** — synthesize a small fixed set of "OTel stores" (`unit_id`s seeded in `ref.unit` so franchisee/region joins survive) vs. map OTel zips onto existing units.
2. **State vocabulary** — collapse Bake/QualityCheck into synth's 4 states vs. enrich `status_event`.
3. **Load-gen handling** — drop `amount=0.0`/`fee-test*` at the adapter vs. tag `is_synthetic_load=true`.

### Suggested phasing & LOE

- **Phase 5.1 — Envelope adapter (MVP, ~2–3 days):** `otel_order_adapter.py` (spans→envelope, ID bridge, SKU parse), `source` column on envelope DDL (`setup_notebook.py`) + 5 silver tables (`mvm_pipeline.py`), load-gen filter, tests. Shippable: live OTel orders flow through existing silver/gold with a `source` flag.
- **Phase 5.2 — Coherence & gold (~2 days):** store-mapping + `ref.unit` seeding, state-vocab reconciliation, a `live_order_ops` gold table / metric view (real-time on-time %, prep vs SOS) split by `source`.
- **Phase 5.3 (optional, ~1–2 days):** join `otel_logs`/`otel_metrics` by `trace_id` for failure/latency context; surface in Genie.
- **Deferred:** OTel as a bulk historical source (volume too low); real customer-identity resolution (OTel users are anonymous UUIDs).

**Total for a demoable dual-source order model: ~4–5 days (5.1 + 5.2).**

### Key files

- **New:** `src/pipeline/otel_order_adapter.py`, `tests/test_otel_order_adapter.py`.
- **Edit:** `src/setup/setup_notebook.py` (`source` column on `order_events` DDL; OTel-store seeding in `ref.unit`), `src/pipeline/mvm_pipeline.py` (carry `source` through 5 silver order tables), `databricks.yml` (`otel_catalog`/`otel_schema` vars, e.g. `jmrdemo.zerobus`), `resources/setup_job.yml` (wire adapter into pipeline/refresh graph).
- **Tests to extend:** `test_runner.py` (no regression on synth path), plus the new adapter test for span-reshape + ID bridge + SKU parse.

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
