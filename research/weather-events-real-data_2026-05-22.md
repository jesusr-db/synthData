# Brainstorming: Real Weather & Local Events Data for QSR Demand Model

## Framing

The QSR synthetic data generator currently has two empty reference tables (`ref.weather_conditions`, `ref.local_events`) and a `CausalContext` dataclass with stub fields (`weather_condition`, `precipitation_inches`, `temperature_f`, `local_event_type`, `local_event_attendance`) that are never populated. The demand model uses hardcoded multipliers and a small dictionary of fixed-date holidays (Super Bowl, Halloween, NYE, Black Friday, NFL Sundays) for "event" effects, but has no weather signal at all and no real local event data.

The opportunity: replace these stubs with **real, automated, forward-looking data** that lets the demand model produce realistic spikes/drops AND surface operational early warnings (e.g., "Phoenix unit 47 will see -40% orders Thursday due to hurricane remnants" or "Atlanta units 12-19 see +180% Sunday from SEC Championship"). The 250 units span 20 metros, so the cardinality is tractable (≈20 metro forecasts × 30 days = 600 weather rows/week; events are sparser).

**Constraints / forces:**
- Must be **fully automatable** per project standard (DAB-managed job, setup creates objects, destroy cleans up)
- **Weekly cadence is acceptable** — this is a synth data demo, not production weather forecasting
- **Forward-looking ≥ 14 days** so the demand model can "warn" about upcoming impacts (NOAA NWS gives 7-day point forecasts; longer ranges need climate normals or paid APIs)
- **Free / no-credit-card APIs strongly preferred** — this is an internal demo, and customer reproducibility matters
- **Per-unit OR per-metro granularity** — 250 units share only 20 weather forecast points, so metro-level weather is the natural pivot; events are city/venue-level which roughly maps to metro
- **No PII / external customer data** — public datasets only
- **Must integrate cleanly with `CausalContext`** — the join key is (unit_id, date) and the multiplier injection point is `build_context()` in `causal_context.py`
- **Backfill 30 days history + forecast 14 days forward** is the target window

**What "good" looks like:** Analysts in the Genie Space can ask "Why did Phoenix unit 47 spike last Sunday?" and the answer joins to a real Cardinals home game; "What's our risk Thursday?" and the answer points to a heat advisory + 3 outdoor concerts.

---

## Assumptions

1. **Free public APIs only.** NOAA NWS API (`api.weather.gov`, no key required) for weather. For events, **Ticketmaster Discovery API** (free tier, requires API key stored as Databricks secret) supplemented by **derived holidays/observances** computed in code.
2. **Granularity = metro-day for weather** (20 metros × forecast horizon), then broadcast-joined to units via `unit.metro_area`.
3. **Granularity = metro-day for events**, with a list-of-events column so a single date can carry multiple events.
4. **Refresh cadence: daily** (not weekly). ~20 API calls/day, essentially free. Cron is a configurable variable.
5. **Forward horizon = 14 days.** NOAA gives 7 reliable days; days 8-14 fall back to climatological normals.
6. **Historical backfill = 30 days.** Match the generator's 1-month backfill window.
7. **A new "weather/events refresh" job** lives in `resources/refresh_weather_events.yml`, scheduled daily, writes via `MERGE` (idempotent).
8. **Schemas:**
   - `ref.weather_conditions`: `metro_area STRING, forecast_date DATE, observation_type STRING, high_temp_f DOUBLE, low_temp_f DOUBLE, precipitation_inches DOUBLE, snowfall_inches DOUBLE, weather_condition STRING, alert_level STRING, demand_multiplier DOUBLE, channel_shift_delivery DOUBLE, refreshed_at TIMESTAMP`
   - `ref.local_events`: `metro_area STRING, event_date DATE, event_id STRING, event_name STRING, event_category STRING, venue STRING, est_attendance INT, est_demand_multiplier DOUBLE, source STRING, refreshed_at TIMESTAMP`
9. A **metric view** `vw_demand_risk_forecast` surfaces (unit, date, risk_factor, risk_level, drivers) for next 14 days as a Genie Space question target.
10. **API failures must not break the pipeline.** Best-effort refresh: stale-data tolerance 3 days weather, 7 days events.
11. **Ticketmaster key** stored as `secrets/qsr-synth/ticketmaster_consumer_key`. Setup notebook skips events portion gracefully if absent.
12. **Tests:** fixtures replay recorded NOAA/Ticketmaster JSON responses — no live API calls in CI.

---

## Perspectives

### 1. The Data Generator Engineer
Clean integration with `CausalContext`, idempotent writes, no breaking changes. Wants multiplier injection as a one-line change in `build_context()` using a pre-built `(unit_id, date)` lookup dict.

### 2. The Demo Storyteller (Field Engineering)
"Watch this: tomorrow Phoenix has a 110°F heat advisory AND a Suns playoff game — demand goes UP for delivery but DOWN for carryout." Real venue + team names, not abstract numbers.

### 3. The Ops Forecaster (target persona)
"Show me the 5 units with highest demand risk next 14 days." Cares about the forward view and NOAA alert levels being real fields, not derived thresholds.

### 4. The Cost / Reliability Reviewer
Pushes back on per-unit API calls (use metro-level). Wants retries with backoff, alert on consecutive failures.

### 5. The Demand-Model Statistician
Publishes multipliers as `conf/weather_event_multipliers.yml` so they're auditable and tweakable. Caps composed effects so Christmas + winter storm don't double-count.

### 6. The Governance / Security Reviewer
Confirms `synth_ref` tables fall under existing ABAC governance policy. Ticketmaster key stored in Databricks secret scope.

### 7. The Customer Adopting the Pattern
The synth project IS the reference architecture for "how to do this in production."

---

## Options

### Option A: NOAA NWS + Ticketmaster + Derived Holidays ⭐ Recommended
**Description:** Daily job calls three sources: NOAA NWS (no key, 7-day forecast + 30-day historical observations + active alerts), Ticketmaster Discovery API (free tier, next 14 days major sports/concerts per metro), Python `holidays` library (federal + state observances, hermetic).

**Pros:** 100% free; NOAA is US government-sourced (no rate limits); Ticketmaster covers major sports + concerts across all 20 metros; sources fail independently; mirrors real QSR data team architecture.

**Cons:** Ticketmaster requires one-time API key registration; misses non-ticketed civic events; NOAA days 8-14 need a fallback strategy.

**Fit:** Highest.

---

### Option B: Open-Meteo + PredictHQ + Holidays Library
**Description:** Open-Meteo (no key, 14-day forecast natively, 80-year historical archive) for weather. PredictHQ free tier (1k calls/month) for events.

**Pros:** Open-Meteo gives 14-day forecast in one call; PredictHQ is purpose-built for demand forecasting with event rank field.

**Cons:** Not US government-sourced; PredictHQ free tier is tight (1k/mo ÷ 20 metros); no native NOAA alert levels.

**Fit:** High — strong alternative if NOAA proves operationally complex.

---

### Option C: All-Synthetic (Reject)
**Description:** Generate weather from climatological normals + seasonal random storms; events from curated recurring-events list.

**Cons:** Violates the primary requirement ("real data"). No forewarning value.

**Fit:** Rejected.

---

### Option D: NOAA Only, Skip Events Initially
**Description:** Phase 1 ships weather only. Events table stays stub + holidays library only.

**Fit:** Medium-low. Events are where the most compelling demo storytelling lives.

---

## Recommendation

**Go with Option A: NOAA NWS + Ticketmaster + Derived Holidays.**

### Architecture

```
resources/refresh_weather_events.yml  (DAB-managed, daily 05:00 UTC)
  Tasks: fetch_weather → fetch_events → compute_multipliers
          │
          ▼
src/refresh/refresh_notebook.py
  - ref.unit → distinct(metro_area, lat, lon)
  - NOAA: forecast (7d) + observations (30d back) + active alerts
  - Ticketmaster: events next 14d, filtered to major sports/concerts
  - holidays library: federal + state observances
  - Apply conf/weather_event_multipliers.yml
  - MERGE INTO ref.weather_conditions + ref.local_events
          │
          ▼
src/generator/causal_context.py  (modified)
  - load_weather_event_lookup(spark, catalog) → dict[(unit_id, date)]
  - build_context() applies lookup, composes multipliers (cap 2.5, floor 0.3)
          │
          ▼
metrics views (new)
  - vw_demand_risk_forecast: (unit, date, risk_level, drivers) next 14 days
  - Surfaced in Genie Space
```

### New Files
- `resources/refresh_weather_events.yml` — DAB job, daily cron
- `src/refresh/__init__.py`
- `src/refresh/noaa_client.py` — NOAA NWS wrapper, retry/backoff, fixture-injectable
- `src/refresh/ticketmaster_client.py` — Ticketmaster wrapper, reads secret, graceful skip if absent
- `src/refresh/multiplier_engine.py` — applies multiplier YAML, composition rules
- `src/refresh/refresh_notebook.py` — Databricks notebook orchestrating daily job
- `conf/weather_event_multipliers.yml` — auditable multiplier table with cited sources in comments
- `src/setup/create_risk_views.py` — adds `vw_demand_risk_forecast`
- `tests/fixtures/noaa/*.json`, `tests/fixtures/ticketmaster/*.json`
- `tests/test_refresh.py` — hermetic tests

### Modified Files
- `src/generator/reference/seeder.py` — real schemas on ref stubs (still empty post-seed; refresh populates)
- `src/generator/causal_context.py` — `weather_event_lookup` param + multiplier composition
- `src/generator/main.py` — load lookup dict at start of backfill/live run
- `resources/setup_job.yml` — add `initial_weather_refresh` task at end of DAG (so demo has data day 1)
- `databricks.yml` — add `weather_refresh_cron` and `ticketmaster_secret_scope` variables
- `docs/roadmap.md`, `docs/architecture.md`, `docs/handoff.md`

### Error Handling
- **NOAA 5xx:** Retry 3× with backoff; on final failure, skip metro, yesterday's data stays, log to `ref.refresh_status`
- **Ticketmaster rate limit:** Skip events refresh this run, log warn, existing data stays
- **Missing Ticketmaster secret:** Log info, skip Ticketmaster, holidays library still populates
- **Demand model lookup miss:** Fall back to multiplier = 1.0, no crash

### Top Risks

1. **Ticketmaster API key requirement breaks "zero manual steps" automation standard.** Mitigation: make Ticketmaster optional; system works with NOAA + holidays library alone; provide `events_provider=holidays_only` config variable; clear one-time setup instructions in quickstart.

2. **Multiplier calibration is subjective.** Mitigation: publish `conf/weather_event_multipliers.yml` with cited sources in YAML comments; ship reasonable defaults; defer tuning to a follow-up spec. Don't let it block the ship.

3. **NOAA gridpoint two-step call.** Mitigation: cache resolved gridpoints in `conf/noaa_gridpoints.yml` (committed once); daily runs skip the `/points/{lat,lon}` call unless they get a 404.
