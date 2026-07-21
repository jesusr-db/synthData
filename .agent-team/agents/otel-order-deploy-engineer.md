# otel-order-deploy-engineer

## Role
Wire the OTel Live-Order bolt-on into the project's established deployment pattern:
a new DAB-managed refresh job, a setup_job backfill task, the staging DDL `source` column,
new databricks.yml variables, and confirmed/ documented destroy_job teardown coverage.

## Plan
Read `docs/superpowers/plans/2026-07-14-otel-live-order-bolt-on.md` (DAB wiring + teardown
sections). Study `resources/refresh_weather_events.yml`, the `initial_weather_refresh` task
in `resources/setup_job.yml`, `resources/destroy_job.yml`, and `src/setup/destroy_notebook.py`
as your pattern templates.

## Files You Own (create or modify)

**Create:**
- `resources/refresh_otel_orders.yml` — DAB job `otel_orders_refresh_job`, cloned from
  `refresh_weather_events.yml`. `refresh` environment (`client: "1"`, no extra deps — pure
  stdlib). `quartz_cron_expression: ${var.otel_refresh_cron}`, `pause_status: UNPAUSED`.
  Single task `refresh_otel_orders` → `../src/refresh/otel_refresh_notebook.py` with
  `base_parameters` catalog_name, schema_prefix, otel_catalog, otel_schema, `mode: incremental`.

**Modify:**
- `src/setup/setup_notebook.py` — add `source STRING` to the end of the
  `staging.order_events` `CREATE TABLE IF NOT EXISTS` column list (Step 3, ~lines 68–123).
  Safe: `columnMapping.mode='name'` already set; do not alter other staging tables.
- `resources/setup_job.yml` — add `initial_otel_backfill` task (same notebook,
  `mode: backfill`, `depends_on: [setup]`, `environment_key: refresh`), and add
  `initial_otel_backfill` to the `backfill` task's `depends_on` (currently `setup` +
  `initial_weather_refresh`). Mirror the `initial_weather_refresh` shape exactly.
- `databricks.yml` — add three variables after the existing block: `otel_catalog`
  (default `jmrdemo`), `otel_schema` (default `zerobus`), `otel_refresh_cron`
  (default `"0 0/2 * * * ?"` — every 2 min, Quartz).
- `src/setup/destroy_notebook.py` — **only if needed.** The adapter creates NO new UC objects
  (it only appends rows into the already-preserved `staging.order_events`), and the refresh job
  is DAB-managed (removed by `databricks bundle destroy`, like `weather_events_refresh_job`). So
  no functional teardown code is required. Add a short comment near the existing "staging schema
  intentionally preserved" note (~line 350) documenting that otel-sourced rows live in the
  preserved staging table and are NOT deleted (deleting from a live streaming source is unsafe).
  Do not add row-deletion logic.

## Files You Must NOT Touch
- Anything under `src/refresh/` — owned by otel-order-data-engineer
- `src/generator/` — owned by otel-order-data-engineer
- `src/pipeline/mvm_pipeline.py` — intentionally UNCHANGED (seamless)
- `tests/` — owned by otel-order-data-engineer

## Key Constraints (match the project deployment standard — DAB + setup_job + destroy_job)
- **DAB-managed:** the refresh job lives in `resources/` and is torn down by
  `databricks bundle destroy`. No manual steps.
- **setup_job:** `initial_otel_backfill` runs after `setup` (staging table exists) and gates
  `backfill`, exactly like `initial_weather_refresh`. Best-effort ⇒ it must SUCCEED (print WARN)
  even if otel is unreachable, never blocking setup.
- **destroy_job:** confirm the DAB job + preserved-staging policy fully cover teardown; document,
  don't add unsafe deletes.
- `refresh_otel_orders.yml` notebook path: `../src/refresh/otel_refresh_notebook.py`.
- `otel_refresh_cron` default `"0 0/2 * * * ?"` (every 2 minutes — hot-demo cadence).

## Validation Gate
Must pass before declaring done:
```bash
databricks bundle validate -p DEFAULT 2>&1 | tail -5
```
Expected: no validation errors.

## Commit
```bash
git add resources/refresh_otel_orders.yml resources/setup_job.yml databricks.yml \
        src/setup/setup_notebook.py src/setup/destroy_notebook.py
git commit -m "feat(dab): otel live-order refresh job + initial_otel_backfill setup task + staging source col"
```
