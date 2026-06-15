# Brainstorm: Integrating driver profiles + driver location into synthData

_Source: Opus background brainstorm, 2026-06-15. Topic: blending the caspers kitchen / twins driver-profile + driver-location capability into the qsr-synth-data-generator (synthData) data model._

## Framing

The ask is to add a **driver entity** (profiles) plus **driver location during delivery** (GPS-ish tracking) to `qsr-synth-data-generator`, blending the conceptual capability from the "caspers kitchen"/"twins" project into synthData's existing tick-based, deterministic, domain-based generator and its Lakeflow DLT medallion pipeline.

The dominant constraint is that synthData has a very specific, well-established architecture that any addition must respect, or it will fight the grain of the codebase:

- **Tick-based, stateless generation.** `runner.backfill_ticks()` iterates units × ticks. Each `build_tick_rows()` call is independent — it builds a `CausalContext`, emits order/inventory/loyalty rows for that tick, and forgets everything. There is no cross-tick state object. Daily entities (shifts, guest profiles, churn, receiving) are emitted on the 10:00 tick only. A delivery in flight inherently *spans multiple ticks* (a 31-minute delivery crosses 31 one-minute live sub-ticks, or sits inside a single 3600-second backfill tick). This is the central architectural tension.
- **Determinism via `make_id()` + seeded `random`.** IDs are SHA-256 hashes of stable string parts (`make_id("o", unit_id, ts, i)`), making re-runs idempotent. But row *content* uses module-level `random.*` (e.g. `entropy.prep_time_seconds`, `orders.py` `random.randint`), which is **not** seeded per-tick — so content already varies run-to-run. GPS interpolation must decide which side of this line it sits on.
- **Wide/sparse staging schema, event_type routing.** Generators return `list[dict]` rows tagged with `event_type`; `main.write_batch()` routes each `event_type` to one of five staging tables via `DOMAIN_TABLE_MAP`, dropping all-null columns and relying on `mergeSchema`. The DLT pipeline (`mvm_pipeline.py`) filters each staging table by `event_type` into a typed silver table. Adding a new event type is an additive, low-friction operation by design.
- **Geo data already exists.** `us_locations.py` gives every unit a `lat`/`lon` (metro centroid + jitter). This is the natural origin point for a route. There is currently **no customer delivery address with coordinates** — `guest_profile` has only `zip_code`. A destination must be synthesized.
- **Delivery is currently a single summary row.** `orders._build_order()` emits one `delivery_order` event per delivery order (`estimated/actual_delivery_seconds`, `delivery_status="delivered"`), with no driver, no route, no intermediate state. This is the anchor the new entity links to.
- **~160 hermetic tests, DAB-managed everything.** No Spark in tests. Per the project's automation standard, any new ref table → seeder + setup; any DAB-manageable resource → `resources/`.

## Assumptions

1. **The caspers/twins capability is conceptual, not importable.** No shared library or table; we replicate the *idea* in synthData's idiom. Assumed to produce:
   - **Driver profiles:** `driver_id`, name, `vehicle_type` (car/scooter/bike/ebike), `home_unit_id` (dispatch base), employment type (W2 vs. 1099/gig), shift window, `rating`, `status` (active/inactive/on_break).
   - **Location stream during a delivery:** ordered GPS pings — `(driver_id, delivery_order_id, ping_ts, lat, lon, heading, speed_mph, distance_remaining_m, eta_seconds, leg_status)` — store pickup → en route → arrival → return, plus status transitions and a live ETA.
2. **Drivers belong to a unit (store-based dispatch), not 3PD.** Only `own_delivery` channel orders get a synthData driver. `3pd_delivery` keeps its opaque `platform_order_reference` and gets *no* internal driver/GPS (realistic — the brand doesn't see 3PD driver GPS). `own_delivery` is ~16% of the channel mix vs. 40% for 3PD, which scopes volume.
3. **Straight-line (great-circle) interpolation is acceptable for the demo.** No real road-network routing. Destination = synthesized point offset from the store within ~1–5 km, linear lat/lon interpolation over delivery duration with mild jitter.
4. **Backfill is the dominant data volume; live mode is the showcase.** Backfill at `tick_seconds=3600`; live mode subdivides the previous hour into 60 one-minute sub-ticks. Ping cadence defined relative to both.
5. **Determinism target = idempotent IDs, plausible content.** Ping/driver IDs via `make_id()`; coordinates via the same `random` convention `orders.py` already uses. Not bit-identical GPS tracks across runs.
6. **Drivers are a distinct persona**, not the kitchen `employee_id`s from `workforce.py`.
7. **No map-rendering UI in scope** — output is tables (+ optional Genie space / metric view).

## Perspectives

- **Generator engineer:** Wants minimal disruption to the tick loop. Cleanest fit is a new `event_type` emitted from the existing per-tick order path. A multi-tick "live tracking" simulation is where complexity explodes.
- **Data modeler / DLT owner:** Wants new silver tables mirroring the existing pattern (staging filtered by `event_type` → typed silver with PK/FK + `franchisee_id`/`region_id` via `_unit_franchisee()`). `driver_locations` is high-row-count append-only (streaming); `driver_profile` is dimension-like (SCD-1 via CDC, like `guest_profile`).
- **Demo storyteller (the actual value):** The point is a compelling last-mile/delivery-ops narrative — live driver map, ETA accuracy, on-time %, utilization, geofence arrival. Argues for at least interpolated GPS pings; profiles-only doesn't tell a location story.
- **Test author:** Each new generator fn needs a hermetic test. Interpolation math, ping count, FK consistency, "no driver for 3pd_delivery" all testable in isolation. Straightforward, additive.
- **Ops/automation (CLAUDE.md standard):** New ref table (`driver`) → seeded + created in setup, torn down in destroy job. New staging + silver flow through DLT once the pipeline notebook + `DOMAIN_TABLE_MAP` know about them. Governance may want to PII-mask driver name/phone like guests.

## Options

### Option A — New `drivers.py` domain, profiles + interpolated GPS pings (RECOMMENDED, "medium" tier)

**Description.** New `src/generator/domains/drivers.py` as a first-class domain. Exposes:
- `generate_driver_profiles(unit_id, registry, tick_ts)` — emitted on the daily 10:00 tick (same hook as `generate_shift_events`), stable per-unit roster via `make_id("driver", unit_id, i)`. ~3–6 drivers/unit scaled by volume. Event type `driver_profile`.
- `generate_driver_pings_for_delivery(ctx, delivery_order_row, driver_id)` — synthesize a destination (store lat/lon + bearing/distance offset), interpolate N pings over `actual_delivery_seconds`. Each ping: `make_id("ping", delivery_order_id, k)`, interpolated `lat`/`lon` with jitter, decreasing `eta_seconds`, `leg_status` ∈ {dispatched, picked_up, en_route, arrived, returning}. Event type `driver_location`.

Wiring: in `runner.build_tick_rows()` (or inside `orders.generate_orders_for_tick`), after order rows are built, for each `own_delivery` `delivery_order` row, pick a driver from the registry's per-unit pool, append ping rows, augment the `delivery_order` row with `driver_id`. New ref table `driver` seeded (or derived deterministically at registry-load, like the guest pool). `EntityRegistry` gains `driver_pool[unit_id]` + `random_driver_id(unit_id)`, analogous to `random_guest_profile_id`.

**Key design choice — eager full-track generation:** the delivery's full GPS track is generated *at order-creation time*, timestamped into the future along the delivery, rather than simulated tick-by-tick. This keeps the stateless tick model intact — same move the code already makes pre-computing an order's future `status_event` timestamps inside one tick. In backfill (hourly tick) emit a compressed track within the hour; in live mode the same function runs per minute-subtick, pings keyed to the delivery's own clock — no cross-tick state.

Pipeline: add `driver_location` + `driver_profile` to `DOMAIN_TABLE_MAP` (new staging table `staging.driver_events` recommended). In `mvm_pipeline.py` add `driver_profile` (CDC SCD-1, like `guest_profile`, enriched) + `driver_location` (streaming append, filtered by `event_type`). Optional gold `delivery_tracking_summary` (ping count, straight-line distance, ETA error, on-time flag) + `driver_utilization_daily`.

**Pros.** Tells the full last-mile story (map + ETA + utilization); eager full-track generation sidesteps the multi-tick-state problem; mirrors existing patterns exactly; reuses existing geo as route origin.
**Cons.** Highest row volume of realistic options (pings are N× delivery rows — needs cadence cap + partitioning); requires synthesizing a customer destination coordinate; backfill "compressed track within an hour tick" is a modeling compromise (correct timestamps, not per-second historical GPS — fine for demo).
**Fit.** Excellent — most aligned with how the codebase already grew (weather/events Phase 3 added a capability the same way).
**LOE — Medium (~2–3 days).** New: `drivers.py` (~150–220 lines incl. geo math, possibly a small `geo.py`). Edits: `entity_registry.py`, `runner.build_tick_rows`, `orders.py` (`driver_id` on `delivery_order`, own_delivery only), `reference/seeder.py` (`build_drivers_data` + `ref.driver`), `main.py` `DOMAIN_TABLE_MAP`, `mvm_pipeline.py` (+2 silver, optional +1–2 gold). DAB: `destroy_job.yml`, maybe `apply_governance.py` (driver PII), `databricks.yml` (`driver_count_per_unit`). Tests: new `test_drivers.py` + minor additions to `test_orders.py`, `test_entity_registry.py`, `test_runner.py`, `test_seeder.py` (~6 test files).

### Option B — Extend `workforce.py`, status-transitions only, no GPS ("minimal" tier)

**Description.** Drivers as a workforce sub-type (role=`driver`); emit coarse delivery status-transition events (dispatched → picked_up → en_route → delivered) reusing the existing `status_event` pattern. `delivery_order` gains `driver_id`. ETA as a scalar only; no coordinates beyond store lat/lon + synthesized destination zip.
**Pros.** Smallest change; lowest volume; fastest to ship; tiny extension of existing fields.
**Cons.** **No location stream** — the explicit point of the request. Overloads `workforce.py` single responsibility. No map/geofence story.
**Fit.** Poor against the literal ask; fine only as a phase-1 stepping stone.
**LOE — Minimal (~0.5–1 day).** ~5 files.

### Option C — Full live-ish tracking: stateful in-flight deliveries, geofencing, batching ("full" tier)

**Description.** Persistent active-deliveries state across ticks (`ActiveDeliveryStore` or `staging.driver_state` read at tick start). Each live tick advances every in-flight delivery one step, emits next ping, fires geofence-arrival, recomputes ETA, supports driver batching (2–3 orders, route-ordered). Optional real road routing via external API in the refresh layer.
**Pros.** Most realistic; true tick-by-tick motion + batching → richer ops metrics.
**Cons.** **Breaks the stateless tick contract** — biggest architectural risk; state store + ordering + re-run idempotency headaches against the `make_id` idempotency the codebase prizes; routing API adds network dep + secret scope; heaviest test surface.
**Fit.** Over-engineered for a synthetic demo; YAGNI day one. Reserve for a future phase if a live map specifically needs true motion.
**LOE — High (~5–8 days).** ~12+ files.

## Recommendation

**Go with Option A** (new `drivers.py` domain; profiles + eagerly-generated interpolated GPS tracks), structured so Option B is effectively its phase-1 milestone and Option C remains a bounded future phase.

Rationale:
- Directly satisfies the literal ask (profiles **and** location during delivery) and tells the actual demo story.
- **Eager full-track generation at order-creation time** is the crucial choice: delivers a GPS stream *without* violating synthData's stateless/idempotent per-tick model — every ping is a pure function of its delivery, keyed by `make_id`. Same move the code already makes for future `status_event` timestamps.
- Slots into every existing seam: new domain file, `event_type` routing, `_unit_franchisee()` enrichment, CDC dimension for the profile (like `guest_profile`), daily-hook roster (like `generate_shift_events`), seeder + setup + destroy + governance per the automation standard, hermetic tests.
- Scoping driver/GPS to **`own_delivery` only** controls volume and is more realistic than instrumenting 3PD.

Suggested phasing: **Phase 1** = profiles + `driver_id` on `delivery_order` + status legs + the `driver`/`driver_profile`/`driver_location` plumbing with a coarse ping cadence (shippable, demoable). **Phase 2** = richer interpolation, ETA-error gold table, geofence-arrival events, Genie space / metric view for the delivery-ops narrative. Defer Option C unless a live map demands it.

**Top risks:**
1. **Row-volume blow-up from the ping table.** `driver_location` will be the largest table. Mitigation: cap pings/delivery (e.g. 8–15), scope to `own_delivery`, partition silver by date, consider reduced ping density in backfill vs. live. Decide cadence in the spec, not in code.
2. **Determinism / staging-routing seam.** (a) Put pings in their own `staging.driver_events` table rather than widening the sparse `order_events` — cleaner DLT filter. (b) Confirm ping coordinates follow the existing "idempotent IDs, plausible (unseeded) content" convention so re-runs don't trip `make_id` idempotency expectations — match `orders.py` behavior rather than inventing a new per-ping seeding scheme.

### Key files this work touches
- **New:** `src/generator/domains/drivers.py`; likely `src/generator/geo.py`; `tests/test_drivers.py`.
- **Edit:** `src/generator/entity_registry.py`, `src/generator/runner.py`, `src/generator/domains/orders.py`, `src/generator/reference/seeder.py`, `src/generator/reference/us_locations.py` (destination-offset helper), `src/generator/main.py` (`DOMAIN_TABLE_MAP`), `src/pipeline/mvm_pipeline.py` (new silver/gold).
- **DAB / setup:** `resources/destroy_job.yml`, `src/setup/setup_notebook.py` (new staging DDL), `src/setup/apply_governance.py` (driver PII masking), `databricks.yml` (driver-count var). `resources/setup_job.yml` + `resources/pipeline.yml` need no structural change (seeder + pipeline auto-pick-up).
- **Tests to extend:** `tests/test_orders.py`, `tests/test_entity_registry.py`, `tests/test_runner.py`, `tests/test_seeder.py`, `tests/test_us_locations.py`.
