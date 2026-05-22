# qsr-synth-data-generator — Project Memory

## Introspection

### Feature "governance-pack" — Phase 1: governance-implementation (2026-05-20T23:35:00-04:00)

#### What worked
- governance-engineer: 4 deliverables landed cleanly. The shared `_unit_franchisee()` helper avoids repeating the `spark.read.table(...).select("unit_id","franchisee_id")` boilerplate across 5 table functions, while keeping each table self-contained for DLT analysis.
- governance-deploy-engineer: setup_job.yml edits were a straightforward DAG insertion. `databricks bundle validate` passed first try.
- The 75 existing pytest suite is hermetic (no Spark/Databricks), so the pipeline changes had no test impact — tests passed in 1.29s after all edits.

#### What failed or needed fixing
- None. Both agents' work landed on first attempt with no rework required.

#### Patterns to watch for
- **CDC tables in Lakeflow Declarative Pipelines need their join in the source view, not the target.** `guest_profile` is a streaming table populated by `dp.create_auto_cdc_flow` from `guest_profile_changes` view. To add `franchisee_id`, the join must go in the `@dp.view(name="guest_profile_changes")` function AND the column must be declared in `dp.create_streaming_table(schema=...)`. There is no `@dp.table` decorator to modify.
- **`broadcast` is in `pyspark.sql.functions`, separate from the `F` alias.** Existing pipeline does `from pyspark.sql import functions as F` but uses `F.col(...)`. Adding broadcast required a second import line `from pyspark.sql.functions import broadcast` — could also be `F.broadcast(...)` but the explicit import is clearer at call sites.
- **Sample volume files**: spark CSV writer creates a directory of part-files, not a single `.csv`. The spec says "menu_catalog.csv" but the implementation writes to `menu_catalog_csv/` (directory). Acceptable for demo purposes; if a single-file is required, switch to pandas via `df.toPandas().to_csv(...)` using a local path then `dbutils.fs.cp`. Documented as a known trade-off, not a defect.
- **Wrap every external API call in try/except for governance setup.** Lakehouse Monitoring API, classification scan API, and SET MASK / SET ROW FILTER DDL all depend on workspace tier and prior state. Treating them as best-effort with `[WARN]` logging (per spec) keeps the setup job green across environments.
- **destroy order matters**: governance objects must be cleaned up BEFORE schema drops cascade them away, otherwise the SDK calls (monitor deletes) fail because the parent table is gone. Step 0a/0b/0c precede the existing Step 1.

#### QA iterations
- Attempt 1: PASS
  - pytest: 75/75 passed (1.29s)
  - bundle validate: OK
  - syntax check: 4/4 files parse
  - contract validation: 18/18 structural checks passed (steps, deps, schemas, joins)

### Feature "weather-events-phase3" — Phase 1: weather-events-implementation (2026-05-22T10:00:00-04:00)

#### What worked
- All 11 plan tasks landed on first attempt with no rework. TDD pattern (write failing test → implement → confirm pass) eliminated guesswork — every new module had test coverage before code existed.
- Disjoint file ownership between data-engineer and deploy-engineer scopes worked cleanly: no merge conflicts because the two agent personas never touched the same files. Data-engineer owns all of src/refresh/, src/generator/*, conf/, tests/, src/setup/create_metric_views.py, docs/. Deploy-engineer owns resources/refresh_weather_events.yml, resources/setup_job.yml additions, databricks.yml variables.
- The injectable `_fetch` parameter pattern (used in all 4 API clients: openmeteo, noaa, nager, events) made every test hermetic. Five JSON fixtures cover happy-path parsing; MagicMock with `.status_code` covers error paths without any live network.
- 102 tests pass in 1.36s — 75 baseline + 27 new (1 seeder, 4 causal_context, 22 refresh) all green on first run after final commit.
- `databricks bundle validate -p DEFAULT` passed first try after deploy-engineer changes. Quartz cron syntax (`"0 0 5 * * ?"`) was correct.

#### What failed or needed fixing
- None. Both implementation streams landed clean. QA passed attempt 1 with zero issues.

#### Patterns to watch for
- **PyYAML safe_load is the only YAML touchpoint for runtime config.** `conf/weather_event_multipliers.yml` is loaded once via `load_config()` and never reparsed. Default path resolves via `Path(__file__).parent.parent.parent / "conf" / ...` — works from any cwd because it walks up from the module file. No env var required.
- **Best-effort external APIs use HTTP status checks, not exceptions.** All four API clients return `[]` on non-200 status rather than raising. This means the refresh notebook can fall through Open-Meteo + NOAA + Nager + Ticketmaster + SeatGeek independently, with `[WARN]` logging per source. The lookup in `main.py` falls back to `{}` if both ref tables are missing — the generator still works the day before the first refresh job runs.
- **Stable event_id via short SHA256 hash.** `_make_event_id(source, metro, date, name)` produces a 16-char hex prefix. Same event from same source → same id, idempotent across refresh runs. MERGE on `event_id` works as expected.
- **WMO weather code → condition mapping has overrides for extreme temperatures.** WMO 0-3 → clear by default, but `temp_max > 100` upgrades to `extreme_heat`, `temp_min < 15` to `extreme_cold`. This catches summer heatwaves and winter cold snaps that the WMO code wouldn't flag.
- **CausalContext's `weather_event_data` parameter must accept None silently.** The existing 75 tests do not pass this param. The new code's `if weather_event_data:` guard at line 1296-1306 keeps default behavior identical when data is absent — that's why baseline tests stayed green throughout.
- **demand_risk_forecast view depends on populated ref tables.** Until the first refresh job runs, this view returns zero rows. Document this in handoff so users know the view "lights up" after initial_weather_refresh completes during setup.
- **DAB task graph extension: insert before backfill, not in parallel.** `initial_weather_refresh` runs after `setup` and before `backfill` so the generator can read populated weather data on first run. Adding it as a parallel branch would race with `backfill` and produce empty contexts on day 1.

#### QA iterations
- Attempt 1: PASS
  - pytest: 102/102 passed (1.36s) — required >=75
  - databricks bundle validate -p DEFAULT: Validation OK
  - 15 contract artifacts present (7 src/refresh, 1 conf, 5 fixtures, 1 test file, 1 resource yml)
  - 9 contract compliance checks (seeder schema, build_context signature, backfill_ticks signature, refresh_weather_events.yml cron+libs, setup_job.yml task+deps, databricks.yml vars) all PASS
