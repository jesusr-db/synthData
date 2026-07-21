# otel-order-data-engineer

## Role
Implement all Python for the OTel Live-Order bolt-on: a pure/hermetic reshape adapter
module, the best-effort Databricks refresh notebook that reads `jmrdemo.zerobus.otel_*`
and appends to `staging.order_events`, hermetic pytest coverage, and the one-line
`source='synth'` default in the generator's `write_batch`.

## Plan
Read `docs/superpowers/plans/2026-07-14-otel-live-order-bolt-on.md` in full before starting.
This mirrors the existing weather-events bolt-on — study `src/refresh/refresh_notebook.py`,
`src/refresh/openmeteo_client.py`, and `tests/test_refresh.py` as your pattern templates.

## Files You Own (create or modify)

**Create:**
- `src/refresh/otel_order_adapter.py` — PURE reshape core (no spark/dbutils/network):
  `parse_skus`, `map_store_to_unit`, `reshape_otel_orders`. Reuse `make_id` from
  `src/generator/id_utils.py` for the namespaced ID bridge. Reuse the 8.5% `_TAX_RATE`
  convention from `src/generator/domains/orders.py`.
- `src/refresh/otel_refresh_notebook.py` — IO wrapper mirroring `refresh_notebook.py`
  (sys.path bootstrap, `_widget` helper, `# COMMAND ----------` cells). Guarded spark reads
  (try/except → `[]`), high-water-mark on `WHERE source='otel'` (analogue of
  `main.py::_latest_staging_ts`), append-only write reusing the `write_batch` cleaning idiom.
  Whole body wrapped so any error prints `[WARN] otel adapter skipped: {e}` and exits cleanly.
- `tests/fixtures/otel_logs_sample.json` — order-bearing log rows (flattened dicts) + 1
  load-gen row (`amount=0.0`) + 1 `fee-test` user row.
- `tests/fixtures/otel_spans_sample.json` — matching `order-tracker received order` spans +
  a couple `stage:` spans.
- `tests/test_otel_adapter.py` — hermetic tests (inject dicts directly, no spark/network).

**Modify:**
- `src/generator/main.py` — in `write_batch()` only: `row.setdefault("source", "synth")`
  scoped to the 5 order-domain `event_type`s. Do NOT touch domain generators in `orders.py`.

## Files You Must NOT Touch
- `resources/refresh_otel_orders.yml` — owned by otel-order-deploy-engineer
- `resources/setup_job.yml`, `resources/destroy_job.yml` — owned by otel-order-deploy-engineer
- `databricks.yml` — owned by otel-order-deploy-engineer
- `src/setup/setup_notebook.py` — owned by otel-order-deploy-engineer (staging DDL `source` col)
- `src/pipeline/mvm_pipeline.py` — intentionally UNCHANGED (seamless: no source threading)
- Any other file not in your list above

## Key Constraints
- **Append-only.** `staging.order_events` is a DLT streaming source — the notebook must
  `.write.mode("append")` only. NEVER MERGE/UPDATE/DELETE (that breaks the stream). Do NOT
  copy the MERGE idiom from `refresh_notebook.py` (that targets a batch ref table, not a stream).
- **Graceful degradation is mandatory.** If otel tables are missing/empty/ungranted, the
  adapter returns `[]` and the notebook writes nothing — pipeline and tests must be unaffected.
- **Correlation key is `trace_id`.** The log `app.order.id` (UUID) and span `order.id` (int)
  differ — never equate them. All `make_id` seeds and the log⋈span join use `trace_id`.
- **`unit_price > 0`.** `order_item()` has `@dp.expect_or_drop("positive_price","unit_price > 0")`
  (`mvm_pipeline.py:109`) — floor distributed line prices at 0.01 or otel items silently vanish.
- **Clamp SKUs** to synth menu range `1..75`; drop garbage tokens.
- Tests hermetic — no spark, no network. Follow TDD: failing test → implement → confirm.
- Run `python3 -m pytest -q` after each change (interpreter is `python3`, not `python`).

## Test Expectations
- `python3 -m pytest -q` shows the full current suite (~118 collected) unchanged + ~12 new
  in `test_otel_adapter.py`, all passing. No FAIL, no ERROR.
- Explicit cases: parse_skus basic/garbage/clamp; ID bridge stable+namespaced (no synth
  collision); store→unit deterministic + in-pool; full-envelope reshape (`total_amount ==
  app.order.amount`, `subtotal+tax ≈ total`, synth channel vocab, `source=='otel'`); every
  order_item `unit_price>0`; load-gen filtered; empty inputs → []; log-without-span; since_ts filter.

## Commit Cadence
One commit per logical unit (adapter+tests, then notebook, then write_batch default). Clear
`feat(otel-orders): ...` messages.
