# weather-events-deploy-engineer

## Role
Wire the DAB YAML for the Phase 3 weather/events feature: new daily refresh job resource,
initial_weather_refresh task in setup_job.yml, and three new variables in databricks.yml.

## Plan
Read `docs/superpowers/plans/2026-05-22-weather-events-phase3.md`, Task 9 (Steps 2–5).

## Files You Own (create or modify)

**Create:**
- `resources/refresh_weather_events.yml` — new DAB job resource (Task 9 Step 2)

**Modify:**
- `databricks.yml` — add weather_refresh_cron, ticketmaster_secret_scope,
  seatgeek_secret_scope variables (Task 9 Step 3)
- `resources/setup_job.yml` — add initial_weather_refresh task after setup,
  update backfill depends_on (Task 9 Step 4)

## Files You Must NOT Touch
- Anything under `src/` — owned by weather-events-data-engineer
- `tests/` — owned by weather-events-data-engineer
- `conf/` — owned by weather-events-data-engineer
- `docs/` — owned by weather-events-data-engineer

## Key Constraints
- `refresh_weather_events.yml` notebook path: `../src/refresh/refresh_notebook.py`
- `initial_weather_refresh` task in setup_job.yml must depend on `setup` and be depended on by `backfill`
- `weather_refresh_cron` default: `"0 0 5 * * ?"` (05:00 UTC daily, Quartz syntax)
- Secret scope defaults: `"qsr-synth"` for both Ticketmaster and SeatGeek
- After edits, run: `databricks bundle validate -p DEFAULT 2>&1 | tail -5`
  Expected: no validation errors

## Validation Gate
Must pass before declaring done:
```bash
databricks bundle validate -p DEFAULT 2>&1 | tail -5
```

## Commit
```bash
git add resources/refresh_weather_events.yml resources/setup_job.yml databricks.yml
git commit -m "feat(dab): daily weather/events refresh job + initial_weather_refresh setup task"
```
