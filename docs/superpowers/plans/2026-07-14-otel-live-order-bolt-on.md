# Plan: OTel Live-Order Bolt-On for synthData

## Context

You run a live PizzaTel storefront whose real orders land — via OpenTelemetry → Zerobus —
in `jmrdemo.zerobus.otel_logs` and `jmrdemo.zerobus.otel_spans`. Today the synthData demo is
100% synthetic. The goal is an **end-to-end live demo**: submit a real order on the website and
watch it flow up into the same silver tables, gold metrics, Genie rooms, and dashboards the
synthetic data already powers — **seamlessly**, so a real order is indistinguishable from a
synthetic one.

Hard constraint (your words): it must be a **bolt-on** — if the otel tables are missing/empty or
access is denied, the core synth pipeline and the existing test suite are completely unaffected.
This is exactly the graceful-degradation shape of the existing **weather-events** bolt-on, which
we replicate.

### Live data profile (verified against the workspace on 2026-07-14 — supersedes the June estimate in `docs/roadmap.md` Phase 5)

- **`otel_logs`** carries the money: `app.order.id` (UUID), `app.order.amount` ($, double),
  `app.order.items.count`, `app.shipping.amount`, `app.shipping.tracking.id`. **2,744 real distinct
  orders** (amount > 0), 2026-06-13 → today, **no load-gen noise** in order-bearing rows. Has
  `trace_id`, `time_unix_nano`.
- **`otel_spans`** (`name = 'order-tracker received order'`) carries semantics: `order.store_id`
  (58 distinct), `order.channel` (`delivery|carryout` — matches synth), `order.skus`
  (`["41 x5","7 x10"]` — leading int **== synth `menu_item_id`, range 1–75 matches exactly**),
  `order.item_count`, `order.prep_seconds`, `order.location.{state,city,zip}` (CA/WA),
  `sos.target_seconds` = 1800 (== synth delivery SOS target). Lifecycle stage spans exist
  (`stage: Prep/Bake/QualityCheck/ReadyForPickup/OutForDelivery/Delivered`).
- **Correlation is by `trace_id`** (~91%: 2,744 logs → 2,492 matched span traces). The log's
  `app.order.id` (UUID) and the span's `order.id` (int) are **different** — do not equate them.

### Decisions locked with you
- **Store mapping:** deterministically hash each otel `store_id` into the existing synth `unit_id`
  pool (state-biased) — real orders blend invisibly into existing franchisee/region rollups. No new units.
- **Cadence:** every 2 minutes (hot-demo setting).
- **Seamless:** no source dimension surfaced anywhere downstream. Real orders look identical to synth
  orders in silver/gold/Genie/dashboards.

---

## Architecture — Pattern B (best-effort adapter → append to staging)

The generator emits event dicts (`event_type` + `event_ts` + columns); `write_batch()`
(`src/generator/main.py:111`) routes them into the single wide Delta table
`{catalog}.{prefix}staging.order_events`. The DLT silver tables in `src/pipeline/mvm_pipeline.py`
do `spark.readStream.table(staging.order_events).filter(event_type == 'guest_order')` etc.
**`staging.order_events` is the bolt-on seam:** a second best-effort writer appends otel-derived
rows in the same envelope, and they flow through the *existing, unchanged* silver → gold → Genie
path automatically.

```
 synth generator (main.py) ──┐
                             ▼
 otel_logs  ─┐   try/except  ┌─►  {catalog}.{prefix}staging.order_events   (append-only)
 otel_spans ─┴─► otel_refresh_notebook.py ─┘        │
                   │  (spark read + high-water-mark + append)
                   └─ calls ─► otel_order_adapter.py  (PURE: rows[dict] → envelope dicts;
                                ID bridge, SKU parse, store→unit map, log⋈span by trace_id)
                             ▼
     existing DLT silver (mvm_pipeline.py, UNCHANGED) → gold / metrics / Genie / dashboards
```

If either otel table is missing/empty/ungranted → adapter returns `[]` → notebook appends nothing
→ pipeline + tests unaffected. Identical failure mode to weather-events.

**Why not Pattern A** (a `readStream.table("jmrdemo.zerobus.otel_spans")` union inside
`mvm_pipeline.py`): DLT resolves every flow's source at pipeline *startup*; a streaming read of a
missing/ungranted external table raises `TABLE_OR_VIEW_NOT_FOUND` during graph analysis and fails
the **entire** pipeline — violating the hard constraint. Rejected.

**Append-only is mandatory:** `staging.order_events` is a DLT streaming source, so the adapter must
only `append` (never MERGE/UPDATE/DELETE — that breaks the stream). Idempotency comes from a
high-water-mark, exactly as the generator already does in `_latest_staging_ts()` (`main.py:133`).

### Seamless ⇒ the `source` column stays internal
Because you want real and synthetic orders to be **indistinguishable downstream**, `source` is used
only for the adapter's high-water-mark idempotency and lives **only in `staging.order_events`**.
We do **not** thread it through the 5 silver tables, add it to gold, or mention it in Genie grounding.
Bonus: this avoids a DLT full-refresh (which changing a streaming-table schema would force) — the
silver/gold/Genie layer is untouched.

---

## Changes

### CREATE

**`src/refresh/otel_order_adapter.py`** — pure, hermetic reshape core (no spark/dbutils/network):
- `parse_skus(skus_raw) -> list[(menu_item_id, qty)]` — `'["41 x5","7 x10"]'` → `[(41,5),(7,10)]`;
  regex `(\d+)\s*x\s*(\d+)`; clamp `1 <= menu_item_id <= 75` and `qty > 0`; `[]` on garbage.
- `map_store_to_unit(store_id, state, unit_ids_by_state, all_unit_ids) -> int` — deterministic:
  `pool = unit_ids_by_state.get(state) or all_unit_ids; return pool[make_id("otel-store", store_id) % len(pool)]`.
  Pure (pools passed in). Reuses `make_id` from `src/generator/id_utils.py`.
- `reshape_otel_orders(log_rows, span_rows, unit_ids_by_state, all_unit_ids, since_ts=None) -> list[dict]`:
  1. Index `order-tracker received order` spans by `trace_id`; collect `stage:` spans per trace_id.
  2. For each log row with `app.order.amount > 0` (load-gen filter) and `event_ts > since_ts`:
     - **ID bridge** (namespaced on `trace_id`, collision-proof vs synth `make_id("o",…)`):
       `guest_order_id = make_id("otel", trace_id)`, `order_item_id = make_id("otel-item", trace_id, i)`,
       `payment_id = make_id("otel-pay", trace_id)`, `status_event_id = make_id("otel-status", trace_id, stage)`,
       `delivery_order_id = make_id("otel-deliv", trace_id)`; `profile_id = member_id = None` (anonymous, like synth anon orders).
     - **guest_order**: `total_amount = app.order.amount`; `subtotal = round(amount/1.085, 2)`,
       `tax_amount = amount - subtotal` (reuse synth's 8.5% `_TAX_RATE`), `discount_amount = 0.0`;
       `channel` = `own_delivery` if span channel `delivery` else `carryout`; `order_type` accordingly;
       `order_status = 'fulfilled'`; `unit_id` from `map_store_to_unit`; `placed_at = event_ts`;
       `sos_breach = prep_seconds > sos_target_seconds`; `source = 'otel'`.
     - **order_item** from `parse_skus`: distribute subtotal by quantity
       (`line_gross = line_net = max(0.01, round(subtotal*qty/total_qty, 2))`, `line_discount = 0`).
       Floor `unit_price > 0` — `order_item()` has `@dp.expect_or_drop("positive_price","unit_price > 0")`
       (`mvm_pipeline.py:109`), so a zero price would silently drop the line.
     - **payment**: `amount = total_amount`, `tender_type = 'card'`, `paid_at = event_ts`.
     - **status_event**: from lifecycle stages (Prep/Bake → preparing, ReadyForPickup/OutForDelivery →
       ready, Delivered → fulfilled), `sos_target_seconds` from span, `is_sos_breach` on the ready transition.
       If stage spans absent, emit the synth-style placed→preparing→ready→fulfilled triple.
     - **delivery_order** only when delivery: `platform_order_reference = app.shipping.tracking.id`,
       est/actual delivery seconds from `prep_seconds`, `delivery_status = 'delivered'`.
     - Every emitted dict carries `event_type`, `event_id`, `event_ts`, `source='otel'`, and the same
       envelope keys the synth rows use (so `write_batch`/DLT selects line up).
  3. Return the flat list. `reshape_otel_orders([], [], …) == []`.
- Load-gen filter: drop `amount is None or <= 0`; drop `user.id` matching `fee-test*`/`c2-verify*` if present.

**`src/refresh/otel_refresh_notebook.py`** — IO wrapper mirroring `src/refresh/refresh_notebook.py`
(sys.path bootstrap, `_widget` helper, `# COMMAND ----------` cells). All spark IO wrapped best-effort:
- Widgets: `catalog_name`, `schema_prefix`, `otel_catalog` (default `jmrdemo`), `otel_schema`
  (default `zerobus`), `mode` (`incremental`|`backfill`).
- **HWM** (guarded, otel-scoped analogue of `_latest_staging_ts`):
  `SELECT MAX(event_ts) FROM {catalog}.{prefix}staging.order_events WHERE source = 'otel'` → `since_ts`
  (None on any error; skipped entirely in `mode=backfill`).
- Load unit map (guarded): `SELECT unit_id, state FROM {catalog}.{prefix}ref.unit` → `unit_ids_by_state`, `all_unit_ids`.
- Read otel (try/except → `[]` on missing table / no grant / empty), hoisting `attributes[...]` map keys
  to columns and converting `time_unix_nano` → tz-naive datetime:
  - `otel_logs` filtered to order-bearing rows (+ `event_ts > since_ts` in incremental) → `.collect()` → list[dict].
  - `otel_spans` where `name = 'order-tracker received order'` (+ `stage:` spans) → `.collect()` → list[dict].
- `reshape_otel_orders(...)`; if empty → `[INFO] no new otel orders`, exit (no write).
- **Append** reusing the exact `write_batch` cleaning idiom (drop all-None cols → `createDataFrame` →
  `.write.format("delta").mode("append").option("mergeSchema","true").saveAsTable(order_events)`).
- Whole body wrapped so any unexpected error prints `[WARN] otel adapter skipped: {e}` and exits cleanly.

**`resources/refresh_otel_orders.yml`** — scheduled job cloned from `resources/refresh_weather_events.yml`:
`otel_orders_refresh_job`, `refresh` environment (`client: "1"`, no extra deps — pure stdlib),
`quartz_cron_expression: ${var.otel_refresh_cron}`, `pause_status: UNPAUSED`, single
`refresh_otel_orders` task → `../src/refresh/otel_refresh_notebook.py` with `mode: incremental`.

**`tests/test_otel_adapter.py`** + **`tests/fixtures/otel_logs_sample.json`**, **`tests/fixtures/otel_spans_sample.json`**
— hermetic, mirroring `tests/test_refresh.py` (inject dicts directly, no spark/network). Cases:
`parse_skus` basic / garbage→[] / clamps to 1–75; ID bridge stable + namespaced (no synth collision);
store→unit deterministic + in-pool (state known vs unknown); reshape emits full envelope with
`total_amount == app.order.amount`, `subtotal+tax ≈ total`, synth channel vocab; every `order_item`
`unit_price > 0` (guards `positive_price`); load-gen (`amount<=0`, `fee-test`) filtered; empty inputs → [];
log-without-span still emits guest_order+payment; `since_ts` filter excludes old rows.

### EDIT

- **`src/setup/setup_notebook.py`** — add `source STRING` to the `staging.order_events` DDL (Step 3,
  the `CREATE TABLE IF NOT EXISTS` block at lines 68–123). Safe on existing deploys: `columnMapping.mode='name'`
  is already set (line 119) and `write_batch` uses `mergeSchema` (main.py:129), so the column is metadata-only.
- **`src/generator/main.py`** — in `write_batch()` (lines 111–129), default `source='synth'` for order-domain
  rows only: `row.setdefault("source", "synth")` for rows whose `event_type` is in the 5 order types
  (scope to order events so the other 4 staging tables don't churn). Domain generators in `orders.py` stay
  untouched → `test_orders.py` unaffected.

**Not edited (seamless ⇒ unnecessary):** `src/pipeline/mvm_pipeline.py` (no source threading, no full
refresh), gold views, `genie_domains/01_grounding.sql`.

### DAB wiring

- **`databricks.yml`** — add vars after the existing block (~line 54):
  `otel_catalog` (`jmrdemo`), `otel_schema` (`zerobus`), `otel_refresh_cron` (`0 0/2 * * * ?` — every 2 min).
- **`resources/setup_job.yml`** — add a one-time historical backfill task `initial_otel_backfill`
  (same notebook, `mode: backfill`, `depends_on: [setup]`, `environment_key: refresh`), and add
  `initial_otel_backfill` to the `backfill` task's `depends_on` (currently `setup` + `initial_weather_refresh`,
  lines 163–166) so ordering is deterministic. Best-effort ⇒ succeeds (WARN) even if otel is unreachable,
  never blocking setup.
- **Teardown:** no `destroy_notebook.py` change. The adapter creates no UC objects (only appends rows into the
  preserved `staging.order_events`); the job is DAB-managed so `databricks bundle destroy` removes it, like
  `weather_events_refresh_job`. We deliberately do **not** delete otel rows (would break append-only on the live stream).

---

## Operational precondition
The job principal needs `SELECT` on `jmrdemo.zerobus.otel_logs` and `otel_spans`. Missing grant degrades
gracefully to a no-op — but then the demo shows no real data, so grant it before demoing.

---

## Verification

1. **Hermetic tests (local, no workspace):** `python3 -m pytest -q` (interpreter is `python3`).
   Expect the current ~118 collected items unchanged + ~12 new = all green. No existing tested file is edited
   except `write_batch` (not under test — `test_runner.py` exercises `backfill_ticks`).
2. **Bundle validates:** `databricks bundle validate -p DEFAULT` → Validation OK (confirms the new job YAML,
   setup-job DAG edit, and new vars).
3. **Adapter dry-run against live otel:** run `otel_refresh_notebook.py` with `mode=backfill`; confirm it
   appends ~2,744 orders' worth of envelope rows to `staging.order_events` with `source='otel'`, and that a
   second run (incremental) appends ~0 (HWM idempotency).
4. **End-to-end seam:** after the DLT pipeline processes the appended rows, verify real orders appear
   indistinguishably in silver:
   `SELECT count(*) FROM {catalog}.{prefix}silver.guest_order WHERE guest_order_id IN (SELECT make_id('otel', trace_id) …)`
   — and that they roll into `synth_metrics.order_performance` and answer correctly in the **Orders & SOS**
   Genie space (e.g. "revenue by channel last 7 days" now includes real orders). Because there is no source
   split, they simply blend in.
5. **Graceful-degradation proof:** point `otel_schema` at a non-existent schema and rerun the adapter → it
   prints `[WARN]`/`[INFO]` and writes nothing; pipeline and tests stay green.
6. **Live demo loop:** with the 2-min job UNPAUSED, place an order on the storefront and confirm it surfaces
   in the Genie room / dashboard within a couple of minutes.

---

## Phasing
- **Phase 5.1 (this plan, MVP):** adapter + notebook + job + backfill + `source` in staging + tests. Ship it.
- **Phase 5.2 (deferred, optional):** seed dedicated "live" units in `ref.unit` for real-location attribution;
  richer status_event vocab. Not needed for the seamless demo.
- **Phase 5.3 (deferred):** join `otel_logs`/`otel_metrics` by `trace_id` for failure/latency context in Genie.
