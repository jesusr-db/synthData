# Phase 3 Deployment Test Plan

> **For agentic workers:** This is a deployment validation handoff, not a feature implementation plan. Do NOT modify source code. Your job is to deploy, observe, diagnose, and document. Use `databricks-cli` skill for all Databricks operations.

**Goal:** Verify that the Phase 3 weather/events integration works end-to-end in the live `jmrdemo` workspace — real API data flows into `ref.weather_conditions` and `ref.local_events`, the backfill generator picks it up, the `demand_risk_forecast` view is queryable, and the daily refresh job runs clean.

**Branch:** `feat/weather-events-phase3` (not yet merged to `main` — test on this branch)

**Workspace:** `jmrdemo` catalog, `synth_` schema prefix → `jmrdemo.synth_ref`, `jmrdemo.synth_metrics`

**Profile:** `DEFAULT` (Databricks CLI profile already configured)

---

## Context: What Was Built

Phase 3 added real weather + events data to the QSR synthetic data generator. Here is what exists on the branch:

### New Files
| File | Purpose |
|------|---------|
| `src/refresh/openmeteo_client.py` | Fetches 30-day historical + 14-day forecast from Open-Meteo (no API key) |
| `src/refresh/noaa_client.py` | Fetches active NWS alerts per US state (no API key), classifies to advisory/watch/warning |
| `src/refresh/nager_client.py` | Fetches US federal/state holidays from Nager.Date (no API key) |
| `src/refresh/events_client.py` | Fetches major sports + concerts from Ticketmaster and SeatGeek (both optional, key-gated) |
| `src/refresh/multiplier_engine.py` | Reads `conf/weather_event_multipliers.yml`, computes demand multipliers |
| `src/refresh/refresh_notebook.py` | Databricks notebook: orchestrates all fetchers, MERGEs into ref tables |
| `conf/weather_event_multipliers.yml` | Auditable multiplier config — weather conditions, alert levels, event categories |
| `resources/refresh_weather_events.yml` | DAB job: daily 05:00 UTC cron, runs refresh_notebook.py |

### Modified Files
| File | Change |
|------|--------|
| `src/generator/reference/seeder.py` | Real schemas for `ref.weather_conditions` and `ref.local_events` (no more stub columns) |
| `src/generator/causal_context.py` | `build_context()` accepts `weather_event_data: dict | None` — applies demand multiplier + delivery channel shift |
| `src/generator/runner.py` | `backfill_ticks()` accepts `weather_event_lookup: dict | None` |
| `src/generator/main.py` | Loads `(metro_area, date) → dict` lookup from ref tables at startup; passes to `backfill_ticks()` |
| `src/setup/create_metric_views.py` | New `demand_risk_forecast` view: 14-day forward risk signal per unit |
| `resources/setup_job.yml` | New `initial_weather_refresh` task (after `setup`, before `backfill`) |
| `databricks.yml` | New variables: `weather_refresh_cron`, `ticketmaster_secret_scope`, `seatgeek_secret_scope` |

### Setup Job DAG (after Phase 3)
```
setup
  └── initial_weather_refresh          ← NEW (fetches real weather/event data before backfill)
        └── backfill                   ← now depends on weather data being ready
              └── start_pipeline
                    ├── create_metric_views → create_genie_space
                    └── apply_governance → configure_monitoring
  (all) → unpause_generator
```

### Key Behavior to Verify
- `ref.weather_conditions` rows: metro_area + forecast_date as key, demand_multiplier pre-computed
- `ref.local_events` rows: event_id as key, covers federal holidays + optionally Ticketmaster/SeatGeek
- Generator backfill: `_load_weather_event_lookup()` in main.py loads both tables, builds `(metro_area, date)` dict
- Per-tick: `build_tick_rows()` looks up `(unit.metro_area, tick.date())` → applies `demand_multiplier` and `channel_shift_delivery`
- Genie: `metrics.demand_risk_forecast` answers "which units have demand risk this week?"

---

## Pre-Deployment Checklist

Before running any deployment steps:

- [ ] **Confirm you are on branch `feat/weather-events-phase3`**
  ```bash
  git branch --show-current
  # Expected: feat/weather-events-phase3
  ```

- [ ] **Confirm tests pass locally** (sanity check before deploying)
  ```bash
  pytest tests/ -q
  # Expected: 102 passed
  ```

- [ ] **Confirm bundle validates**
  ```bash
  databricks bundle validate -p DEFAULT 2>&1 | tail -3
  # Expected: Validation OK!
  ```

- [ ] **Note: Ticketmaster and SeatGeek are optional**
  The refresh notebook skips them gracefully if secrets are absent. Holidays (Nager.Date) and weather (Open-Meteo + NOAA) work with zero secrets. If you want to test the full events path, set the secrets first (see Task 8).

---

## Task 1: Deploy the Bundle

Deploy the updated bundle to pick up all new DAB resources including `weather_events_refresh_job`.

```bash
databricks bundle deploy -p DEFAULT 2>&1 | tail -10
```

Expected output includes:
- `Uploading bundle files...`
- `Deploying resources...`
- `weather_events_refresh_job` mentioned in resource deployment
- No errors

**Verify the refresh job was created:**
```bash
databricks jobs list -p DEFAULT --output json | python3 -c "
import json, sys
jobs = json.load(sys.stdin).get('jobs', [])
for j in jobs:
    if 'Weather' in j.get('settings', {}).get('name', ''):
        print(j['job_id'], j['settings']['name'])
"
```
Expected: one job with "Weather & Events Refresh" in the name.

**If deploy fails:** Check `databricks bundle validate -p DEFAULT` output for the specific error. Common issues:
- YAML indentation in `resources/setup_job.yml` → read the file around `initial_weather_refresh`
- Missing `pyyaml` in `resources/refresh_weather_events.yml` libraries block → check the file

---

## Task 2: Run the Refresh Notebook Standalone

Before triggering the full setup job, run the refresh notebook in isolation to verify API connectivity and MERGE behavior.

**Find the refresh job ID:**
```bash
databricks jobs list -p DEFAULT --output json | python3 -c "
import json, sys
for j in json.load(sys.stdin).get('jobs', []):
    if 'Weather' in j.get('settings', {}).get('name', ''):
        print(j['job_id'])
"
```

**Trigger a one-off run:**
```bash
databricks jobs run-now <refresh_job_id> -p DEFAULT --output json
```

Note the `run_id` from the output.

**Poll until complete (check every 30s):**
```bash
databricks jobs get-run <run_id> -p DEFAULT --output json | python3 -c "
import json, sys
r = json.load(sys.stdin)
print(r.get('state', {}).get('life_cycle_state'), r.get('state', {}).get('result_state', ''))
"
```
Expected terminal states: `TERMINATED SUCCESS` or `TERMINATED FAILED`

**If FAILED — get the error:**
```bash
databricks jobs get-run-output <run_id> -p DEFAULT --output json | python3 -c "
import json, sys
r = json.load(sys.stdin)
print(r.get('error', ''))
print(r.get('error_trace', '')[:2000])
"
```

**Common failure modes and fixes:**

| Symptom | Root Cause | Fix |
|---------|-----------|-----|
| `ModuleNotFoundError: src.refresh` | Bundle root path not in sys.path | Check `_bundle_root` logic at top of `refresh_notebook.py` — the notebook must be run via the job (not interactively) for the path to resolve correctly |
| `ModuleNotFoundError: yaml` | pyyaml not installed | Verify `libraries: - pypi: package: pyyaml` is in `resources/refresh_weather_events.yml` |
| `ConnectionError` on Open-Meteo | Network connectivity from cluster | Try a different cluster type; serverless should work |
| `AnalysisException: table not found: ref.weather_conditions` | Tables have old stub schema | Run `src/setup/setup_notebook.py` first to recreate tables with real schemas, OR drop and recreate manually (see Task 3 alt path) |
| `cannot resolve 'metro_area'` | Old stub schema still active (`stub_id, placeholder`) | Drop table and re-run setup (see Task 3 alt path) |
| Open-Meteo returns empty `daily` | Wrong date range params | Check `past_days=30&forecast_days=14` in URL — Open-Meteo caps at `past_days=92` |

---

## Task 3: Verify ref Table Contents

After the refresh job succeeds, confirm data landed in both ref tables.

**Weather conditions:**
```python
# Run as a Databricks SQL query or notebook cell:
SELECT metro_area, COUNT(*) as days,
       MIN(forecast_date) as earliest, MAX(forecast_date) as latest,
       SUM(CASE WHEN demand_multiplier IS NULL THEN 1 ELSE 0 END) as null_multipliers,
       COUNT(DISTINCT weather_condition) as condition_types,
       COUNT(DISTINCT alert_level) as alert_levels
FROM jmrdemo.synth_ref.weather_conditions
GROUP BY metro_area
ORDER BY metro_area
```

Expected:
- ~20 rows (one per metro)
- each metro: 44 days (30 historical + 14 forecast) — may be slightly fewer at boundary
- `null_multipliers` = 0 (multipliers always computed)
- `condition_types` ≥ 2 (at least clear + something else across 44 days)
- `alert_levels`: 0 or more (NWS alerts are real — may be 0 if no active alerts)

**Local events (holidays):**
```python
SELECT source, event_category, COUNT(*) as events,
       MIN(event_date) as earliest, MAX(event_date) as latest
FROM jmrdemo.synth_ref.local_events
GROUP BY source, event_category
ORDER BY source
```

Expected:
- `nager` / `national_holiday` rows covering 2026 + 2027 US federal holidays
- If Ticketmaster/SeatGeek secrets configured: additional `ticketmaster`/`seatgeek` rows

**Alt path — if tables have stale stub schema (`stub_id, placeholder`):**
The old tables were created before Phase 3 with stub schemas. Drop them manually:
```sql
DROP TABLE IF EXISTS jmrdemo.synth_ref.weather_conditions;
DROP TABLE IF EXISTS jmrdemo.synth_ref.local_events;
```
Then re-run the refresh job. The notebook's MERGE will CREATE the tables on first run... 

**WAIT** — actually the tables are created by `setup_notebook.py` (seeder.py), not by the refresh notebook. If the existing tables have the old stub schema, you need to re-run setup first. The safest path is Task 5 (full setup job re-run). If you want to avoid a full re-run, drop the two tables and run only the `initial_weather_refresh` task standalone — the MERGE INTO will fail because the tables don't exist yet. You must either:
- Option A: Drop tables → run setup task only → then run refresh → verify
- Option B: Full setup job re-run (Task 5) — cleanest, covers everything

---

## Task 4: Verify demand_risk_forecast View

Run `create_metric_views` task standalone or query the view directly if the full setup job already created it.

**Check if view exists:**
```python
SHOW TABLES IN jmrdemo.synth_metrics LIKE 'demand_risk_forecast'
```

**If view doesn't exist yet** (first deploy), trigger the `create_metric_views` notebook:
```bash
# Find the setup job ID
databricks jobs list -p DEFAULT --output json | python3 -c "
import json, sys
for j in json.load(sys.stdin).get('jobs', []):
    if 'QSR Setup' in j.get('settings', {}).get('name', ''):
        print(j['job_id'])
"
```

**Query the view:**
```python
SELECT risk_level, COUNT(*) as units_dates,
       AVG(combined_demand_multiplier) as avg_multiplier,
       MIN(forecast_date) as min_date, MAX(forecast_date) as max_date
FROM jmrdemo.synth_metrics.demand_risk_forecast
GROUP BY risk_level
ORDER BY risk_level
```

Expected:
- Rows covering next 14 days × 250 units = up to 3,500 rows
- `risk_level` values: `demand_risk`, `capacity_risk`, `normal`
- `avg_multiplier` near 1.0 for `normal`, <0.8 for `demand_risk`, >1.4 for `capacity_risk`
- If no national holidays in the next 14 days and no active severe weather: mostly `normal`

**If view returns 0 rows:**
- `ref.weather_conditions` is empty → the refresh job hasn't run yet or failed
- Check Task 2/3 first

**Genie Space test prompt:**
Once the view exists, open the QSR Genie Space and ask:
> *"Which units have the highest demand risk in the next 7 days?"*
> *"Show me capacity risk units this weekend"*
> *"What is the weather outlook for Phoenix units?"*

---

## Task 5: Full Setup Job Re-Run (End-to-End Test)

This is the definitive test: a full `destroy → deploy → setup job run` cycle verifies the entire Phase 3 DAG works from scratch, including `initial_weather_refresh` firing before `backfill`.

**5a. Run destroy job** to tear down existing jmrdemo objects:
```bash
DESTROY_JOB_ID=$(databricks jobs list -p DEFAULT --output json | python3 -c "
import json, sys
for j in json.load(sys.stdin).get('jobs', []):
    if 'Destroy' in j.get('settings', {}).get('name', ''):
        print(j['job_id'])
")
databricks jobs run-now $DESTROY_JOB_ID -p DEFAULT
```
Wait for completion (~3 min). Expected: SUCCESS.

**5b. Redeploy bundle:**
```bash
databricks bundle deploy -p DEFAULT
```

**5c. Run setup job:**
```bash
SETUP_JOB_ID=$(databricks jobs list -p DEFAULT --output json | python3 -c "
import json, sys
for j in json.load(sys.stdin).get('jobs', []):
    if 'QSR Setup' in j.get('settings', {}).get('name', ''):
        print(j['job_id'])
")
databricks jobs run-now $SETUP_JOB_ID -p DEFAULT
```

**5d. Monitor the DAG:**
```bash
# Poll for completion (run every 60s)
RUN_ID=<run_id from above>
databricks jobs get-run $RUN_ID -p DEFAULT --output json | python3 -c "
import json, sys
r = json.load(sys.stdin)
state = r.get('state', {})
print('State:', state.get('life_cycle_state'), state.get('result_state', ''))
tasks = r.get('tasks', [])
for t in tasks:
    print(f'  {t[\"task_key\"]}: {t.get(\"state\",{}).get(\"life_cycle_state\")} {t.get(\"state\",{}).get(\"result_state\",\"\")}')
"
```

**Expected task completion order:**
1. `setup` → SUCCESS
2. `initial_weather_refresh` → SUCCESS (Open-Meteo + NOAA + Nager.Date)
3. `backfill` → SUCCESS (uses weather/event data loaded from ref tables)
4. `start_pipeline` → SUCCESS
5. `create_metric_views` (includes `demand_risk_forecast`) → SUCCESS
6. `apply_governance`, `configure_monitoring` → SUCCESS
7. `create_genie_space` → SUCCESS
8. `unpause_generator` → SUCCESS

**If `initial_weather_refresh` fails:**
- Check the task error via `databricks jobs get-run-output <task_run_id> -p DEFAULT`
- The most common issue: pyyaml not in the task's `libraries` block — verify `resources/setup_job.yml` has both `requests` and `pyyaml` under `initial_weather_refresh.libraries`
- Fix in the YAML, redeploy, re-run (repair mode: set `initial_weather_refresh` and downstream as `pending`)

**If `backfill` succeeds but multipliers aren't applied:**
- Query `ref.weather_conditions` — if empty, the refresh task succeeded but wrote 0 rows (API error was swallowed)
- Check `[WARN]` lines in `initial_weather_refresh` task output
- Backfill still runs (graceful fallback to multiplier=1.0) so this is a data quality issue, not a crash

---

## Task 6: Verify Generator Applies Weather Multipliers

After the setup job completes, spot-check that the backfilled order data reflects weather effects.

**Check if multipliers are non-uniform:**
```python
-- Look for days where weather suppressed demand (e.g. storm days)
SELECT DATE(event_ts) as date, metro_area,
       COUNT(*) as order_count,
       AVG(order_total) as avg_total
FROM jmrdemo.synth_staging.order_events oe
JOIN jmrdemo.synth_ref.unit u ON oe.unit_id = u.unit_id
JOIN jmrdemo.synth_ref.weather_conditions w
  ON u.metro_area = w.metro_area
  AND DATE(oe.event_ts) = w.forecast_date
WHERE w.demand_multiplier < 0.9
GROUP BY 1, 2
ORDER BY avg_total ASC
LIMIT 20
```

Expected: days with low demand_multiplier (storm, heavy snow) show lower order counts relative to nearby days.

**Check delivery channel shift on rainy days:**
```python
SELECT w.weather_condition,
       channel,
       COUNT(*) as orders
FROM jmrdemo.synth_staging.order_events oe
JOIN jmrdemo.synth_ref.unit u ON oe.unit_id = u.unit_id
JOIN jmrdemo.synth_ref.weather_conditions w
  ON u.metro_area = w.metro_area
  AND DATE(oe.event_ts) = w.forecast_date
WHERE w.weather_condition IN ('rain', 'heavy_rain', 'clear')
GROUP BY 1, 2
ORDER BY 1, 3 DESC
```

Expected: `rain`/`heavy_rain` rows have higher `3pd_delivery` share than `clear` rows.

**Note:** If multipliers look flat (no variation):
- `_load_weather_event_lookup()` may have silently returned `{}` — check `[WARN]` in backfill task output
- The tables may have been empty when the backfill started (race condition if refresh was slow)
- This is non-fatal by design; data is still valid, just without weather effects

---

## Task 7: Verify Daily Refresh Job Schedule

The `weather_events_refresh_job` is deployed with `pause_status: UNPAUSED` and runs at 05:00 UTC daily.

**Verify schedule is active:**
```bash
databricks jobs list -p DEFAULT --output json | python3 -c "
import json, sys
for j in json.load(sys.stdin).get('jobs', []):
    settings = j.get('settings', {})
    if 'Weather' in settings.get('name', ''):
        sched = settings.get('schedule', {})
        print('Job:', j['job_id'])
        print('Schedule:', sched.get('quartz_cron_expression'))
        print('Pause status:', sched.get('pause_status'))
"
```

Expected:
- `quartz_cron_expression: 0 0 5 * * ?`
- `pause_status: UNPAUSED`

**Trigger a manual run to verify idempotence (MERGE must not duplicate rows):**
```bash
# Get job ID from above
databricks jobs run-now <weather_refresh_job_id> -p DEFAULT
```

After it completes, rerun the row counts from Task 3 and confirm they are identical (MERGE is idempotent — no new rows for the same metro+date keys).

---

## Task 8: Optional — Test with Ticketmaster / SeatGeek Keys

If you have API keys available, test the optional events path.

**Set secrets:**
```bash
# Create scope if it doesn't exist
databricks secrets create-scope qsr-synth -p DEFAULT 2>/dev/null || true

# Ticketmaster
databricks secrets put-secret qsr-synth ticketmaster_consumer_key -p DEFAULT
# (enter key at prompt)

# SeatGeek
databricks secrets put-secret qsr-synth seatgeek_client_id -p DEFAULT
# (enter key at prompt)
```

**Trigger refresh job again** and check that `local_events` now contains `ticketmaster` and `seatgeek` source rows:
```python
SELECT source, COUNT(*) FROM jmrdemo.synth_ref.local_events GROUP BY source
```

Expected: `nager` rows + `ticketmaster` rows + `seatgeek` rows.

---

## Task 9: Document Results

After completing the above tasks, write findings to `docs/deployment-test-results-phase3.md`:

```markdown
# Phase 3 Deployment Test Results — <date>

## Environment
- Workspace: jmrdemo
- Branch: feat/weather-events-phase3
- Run date: <date>

## Task Results
| Task | Result | Notes |
|------|--------|-------|
| Bundle deploy | PASS/FAIL | |
| Refresh job standalone | PASS/FAIL | # rows in weather_conditions, local_events |
| demand_risk_forecast view | PASS/FAIL | # rows, risk_level distribution |
| Full setup job re-run | PASS/FAIL | All 8 tasks |
| Generator multipliers visible | PASS/FAIL | |
| Daily schedule active | PASS/FAIL | |
| Ticketmaster/SeatGeek (if tested) | PASS/FAIL/SKIPPED | |

## Row Counts
- ref.weather_conditions: <N> rows, <N> metros
- ref.local_events: <N> rows, sources: [nager, ...]
- metrics.demand_risk_forecast: <N> rows

## Issues Found
<list any bugs, unexpected behavior, or regressions>

## Recommendation
READY TO MERGE / NEEDS FIXES: <brief rationale>
```

Commit the results doc:
```bash
git add docs/deployment-test-results-phase3.md
git commit -m "docs: Phase 3 deployment test results"
```

---

## What PASS Looks Like (Merge Criteria)

The branch is ready to merge to `main` when ALL of these are true:

- [ ] `databricks bundle deploy -p DEFAULT` exits 0
- [ ] `weather_events_refresh_job` is created in workspace with daily schedule UNPAUSED
- [ ] `ref.weather_conditions` has ≥20 metro rows, ≥30 days each, all `demand_multiplier` non-null
- [ ] `ref.local_events` has ≥10 holiday rows from Nager.Date
- [ ] `metrics.demand_risk_forecast` is queryable and returns rows for next 14 days
- [ ] Full setup job run: all 8 tasks SUCCESS (including `initial_weather_refresh` before `backfill`)
- [ ] Second refresh run (idempotence check): row counts unchanged
- [ ] `pytest tests/ -q`: 102 passed (no regressions)

## Rollback

If any task is a hard blocker and cannot be fixed quickly:

```bash
git reset --hard feature-start/weather-events-phase3
databricks bundle deploy -p DEFAULT
databricks jobs run-now <setup_job_id> -p DEFAULT
```

This restores the pre-Phase-3 state (governance-pack branch tip) and redeploys cleanly.
