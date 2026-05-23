# Phase 3 Deployment Test Results — 2026-05-22

## Environment
- Workspace: jmrdemo
- Branch: feat/weather-events-phase3
- Run date: 2026-05-22
- Tester: automated execution via executing-plans skill
- Path taken: Option A (targeted re-seed — existing staging data preserved)

## Task Results

| Task | Result | Notes |
|------|--------|-------|
| Bundle deploy | PASS (after fix) | Required serverless libraries fix — see Bug #1 |
| Refresh job standalone | PASS (after fix) | Required lat/lon column name fix — see Bug #2 |
| `ref.weather_conditions` contents | PASS | 20 metros × 44 days, 0 null multipliers |
| `ref.local_events` contents | PASS | 28 US federal holidays (nager, 2026–2027) |
| `demand_risk_forecast` view | PASS | 3,250 rows, 14-day forecast window, risk levels populated |
| Generator multipliers visible | PARTIAL | JOINs work; existing backfill predates Phase 3 (no weather effects in order data) |
| Daily schedule active | PASS | `0 0 5 * * ?`, UNPAUSED |
| Idempotence check (2nd refresh run) | PASS | 880 weather rows + 28 event rows unchanged |
| Ticketmaster/SeatGeek | SKIPPED | No secrets configured |

## Row Counts

- `ref.weather_conditions`: 880 rows, 20 metros × 44 days (2026-04-22 → 2026-06-04)
- `ref.local_events`: 28 rows, source: nager (US federal holidays 2026-2027)
- `metrics.demand_risk_forecast`: 3,250 rows (250 units × 13 forecast days)
  - `demand_risk`: 59 unit-date rows, avg multiplier 0.61
  - `normal`: 3,191 unit-date rows, avg multiplier 0.97

## Bugs Found and Fixed

### Bug #1 — Serverless tasks using `libraries:` instead of `environments:`

**Files:** `resources/setup_job.yml`, `resources/refresh_weather_events.yml`

**Symptom:** `databricks bundle deploy` failed with:
```
Error: cannot update job: Libraries field is not supported for serverless task,
please specify libraries in environment.
```

**Root cause:** Phase 3 added `libraries: [requests, pyyaml]` at the task level for two new serverless tasks. Databricks serverless tasks require `environments:` + `environment_key:` instead. The local `databricks bundle validate` passed (schema-only check) but the Terraform provider rejected it at deploy time.

**Fix applied:** Added `refresh` environment to both jobs' `environments:` blocks; replaced `libraries:` with `environment_key: refresh` on the affected tasks.

---

### Bug #2 — `refresh_notebook.py` queries `latitude`/`longitude` but `ref.unit` has `lat`/`lon`

**File:** `src/refresh/refresh_notebook.py` line 45

**Symptom:** First refresh run FAILED with:
```
[UNRESOLVED_COLUMN] 'latitude' cannot be resolved. Did you mean 'lat'?
```

**Root cause:** The SQL query that loads metro centroids used `AVG(latitude) AS lat, AVG(longitude) AS lon` but the actual column names in `ref.unit` are `lat` and `lon`.

**Fix applied:** Changed `AVG(latitude)` → `AVG(lat)` and `AVG(longitude)` → `AVG(lon)`.

---

### Bug #3 — Cosmetic: weather refresh job name has duplicate `[dev ...]` prefix

**File:** `resources/refresh_weather_events.yml`

Deployed name: `[dev jesus_rodriguez] [dev jesus.rodriguez@databricks.com] Weather & Events Refresh [dev]`

DAB auto-prepends `[dev <short_user>]` to all job names AND the yml includes an explicit `[${bundle.target} ${workspace.current_user.userName}]` prefix. Result: double prefix.

**Fix needed (pre-merge):** Remove the explicit name prefix from the yml — rely on DAB's auto-prefix, or override in `databricks.yml` with `presets.name_prefix: ""`.

---

### Known Gap — Task 6: Multiplier effects not verifiable on pre-Phase-3 backfill

The `channel` distribution was flat (~5% `3pd_delivery`) across clear/rain/heavy_rain days in the existing order data. This is expected: the backfill that produced the ~6.7M order rows ran before Phase 3 was deployed, so `_load_weather_event_lookup()` returned `{}` and multiplier=1.0 was applied uniformly.

**Not a bug.** A fresh full setup re-run (Option B) would produce order data with real weather effects. Plan's note on this is accurate: "data is still valid, just without weather effects."

## Schema corrections for plan SQL examples

The plan's Task 6 SQL uses `order_total` (does not exist) — actual column is `total_amount`. Also `oe.metro_area` doesn't exist on `order_events` — must use `u.metro_area` after the unit JOIN. These are doc-only issues in the plan; not blockers.

## Recommendation

**NEEDS FIXES BEFORE MERGE** — 2 code bugs fixed in this session (bugs #1, #2), 1 cosmetic issue remaining (bug #3). Branch is not yet fully validated end-to-end (full DAG including `initial_weather_refresh → backfill` was not exercised due to Option A path). Recommend:

1. Fix bug #3 (duplicate name prefix) — 1-line yml change
2. Run Option B (full destroy + setup re-run) to validate the complete DAG ordering and verify multipliers land in freshly backfilled order data
3. Once both pass: merge to main

All API integrations (Open-Meteo, NOAA, Nager.Date) work correctly and produce real data. The `demand_risk_forecast` view is live and queryable. Daily refresh is scheduled and idempotent.
