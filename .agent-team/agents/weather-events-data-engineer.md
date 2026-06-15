# weather-events-data-engineer

## Role
Implement all Python code for the Phase 3 weather/events integration: API clients,
multiplier engine, CausalContext + runner integration, metric view, and docs.

## Plan
Read `docs/superpowers/plans/2026-05-22-weather-events-phase3.md` in full before starting.
Execute Tasks 1–8 + the refresh_notebook.py portion of Task 9 + Task 10 + Task 11.

## Files You Own (create or modify)

**Create:**
- `src/refresh/__init__.py`
- `src/refresh/openmeteo_client.py`
- `src/refresh/noaa_client.py`
- `src/refresh/nager_client.py`
- `src/refresh/events_client.py`
- `src/refresh/multiplier_engine.py`
- `src/refresh/refresh_notebook.py`
- `conf/weather_event_multipliers.yml`
- `tests/fixtures/openmeteo_forecast.json`
- `tests/fixtures/noaa_alerts_ny.json`
- `tests/fixtures/nager_holidays.json`
- `tests/fixtures/ticketmaster_events.json`
- `tests/fixtures/seatgeek_events.json`
- `tests/test_refresh.py`

**Modify:**
- `src/generator/reference/seeder.py` — replace stub weather/events schemas (Task 1)
- `src/generator/causal_context.py` — add weather_event_data param (Task 7)
- `src/generator/runner.py` — add weather_event_lookup param (Task 7)
- `src/generator/main.py` — load lookup at startup (Task 8)
- `src/setup/create_metric_views.py` — add demand_risk_forecast view (Task 10)
- `docs/roadmap.md` — mark Phase 3 in progress (Task 11)
- `docs/handoff.md` — add Weather & Events section (Task 11)

## Files You Must NOT Touch
- `resources/refresh_weather_events.yml` — owned by weather-events-deploy-engineer
- `resources/setup_job.yml` — owned by weather-events-deploy-engineer
- `databricks.yml` — owned by weather-events-deploy-engineer
- Any other file not in your list above

## Key Constraints
- All tests must be hermetic — no live API calls. Use injectable `_fetch` param pattern.
- `build_context()` must fall back silently to multiplier=1.0 when weather_event_data is None.
- PyYAML is the only new dependency. Verify it's available: `python3 -c "import yaml; print('ok')"`.
- Follow TDD: write the failing test first, then implement, then confirm pass.
- Run `pytest tests/ -q` after each task to confirm no regressions.
- The plan has exact code for every step — use it verbatim unless a real conflict requires adaptation.

## Test Expectations
After all tasks:
- `pytest tests/ -q` must show ≥ 75 tests passing (75 existing + new tests in test_seeder,
  test_causal_context, test_refresh)
- No FAIL, no ERROR

## Commit Cadence
One commit per task as specified in the plan. Use the exact commit messages from the plan.
