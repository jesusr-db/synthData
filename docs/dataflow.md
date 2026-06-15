# Dataflow

## End-to-End Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  SETUP (one-time, setup_job — 12 tasks)                                      │
│                                                                              │
│  1. setup_notebook.py  (task: setup)                                         │
│     ├── Verify catalog exists (must be pre-created)                          │
│     ├── CREATE SCHEMA IF NOT EXISTS  staging, ref, metrics, features         │
│     ├── CREATE TABLE IF NOT EXISTS  5 staging tables (wide sparse schema)    │
│     ├── CREATE empty ref.weather_conditions + ref.local_events              │
│     └── seed_all() → write ref tables (unit, franchisee, financial_period,   │
│                        item_price, menu_item, recipe_ingredient, supplier)    │
│                                                                              │
│  2. initial_weather_refresh  (after setup)                                   │
│     └── refresh_notebook.py                                                  │
│         ├── distinct metros from ref.unit                                    │
│         ├── Open-Meteo → weather_conditions (±30/+14 days)                   │
│         ├── NOAA NWS alerts overlaid                                         │
│         ├── Nager.Date holidays → local_events                              │
│         ├── Ticketmaster / SeatGeek (optional, key-gated)                    │
│         └── MERGE INTO ref.weather_conditions / ref.local_events            │
│                                                                              │
│  3. backfill  (after setup + initial_weather_refresh)                        │
│     └── main.py mode=backfill                                                │
│         ├── Read MAX(event_ts) across staging tables                         │
│         ├── If no data → generate backfill_months of hourly ticks            │
│         │   (reads weather_conditions + local_events for demand context)    │
│         ├── If data exists → resume from next full hour                      │
│         └── write_batch() → append to 5 staging Delta tables                │
│                                                                              │
│  4. start_pipeline_notebook.py  (after backfill)                            │
│     ├── Poll for any active pipeline update (wait)                           │
│     ├── Trigger pipeline update                                              │
│     └── Fall back to full_refresh=True if update fails                      │
│                                                                              │
│  5a. build_feature_tables.py  (after start_pipeline, env: ml)               │
│      └── customer_features + store_features (+ online tables + FE endpoint)  │
│  5b. create_metric_views.py   (after start_pipeline)                        │
│      └── CREATE OR REPLACE  metrics.order_performance, loyalty_performance, │
│           inventory_waste, staff_hours + demand_risk_forecast view          │
│  5c. apply_governance.py      (after start_pipeline)                        │
│      ├── CREATE VOLUME  ref.assets + export menu/franchise CSV + receipt     │
│      ├── COMMENT ON TABLE / ALTER COLUMN COMMENT                            │
│      ├── ALTER COLUMN SET TAGS  (class.*, financial, supply_chain)          │
│      ├── CREATE FUNCTION  mask_email, mask_phone, tier_to_multiplier         │
│      ├── ALTER COLUMN SET MASK  (email, phone on guest_profile+guest_events)│
│      ├── CREATE FUNCTION  filter_by_franchisee                              │
│      ├── ALTER TABLE SET ROW FILTER  on 6 silver/ref tables                 │
│      └── POST data-classification-tasks (best-effort)                       │
│                                                                              │
│  6a. train_recommender.py     (after build_feature_tables, env: ml)         │
│      └── train model → register UC → create serving endpoint (raw REST)     │
│           → optional CAN_QUERY grant to recommender_query_principal         │
│  6b. create_genie_space.py    (after create_metric_views)                   │
│      └── REST: create Genie Space with 10 seed questions                     │
│  6c. configure_monitoring.py  (after apply_governance)                      │
│      └── SDK: snapshot monitors on order_events, inventory_events,          │
│              loyalty_events (best-effort, non-fatal)                         │
│                                                                              │
│  7. apply_ontos.py            (after configure_monitoring, env: refresh)     │
│     └── register schemas + semantic links in ontos (gated on ontos_enabled) │
│                                                                              │
│  8. unpause_generator_notebook.py                                            │
│     (after backfill + create_genie_space + apply_ontos + train_recommender) │
│     └── SDK: set generator_job schedule to UNPAUSED                         │
└──────────────────────────────────────────────────────────────────────────────┘

                          ┌──────────── every hour ─────────────┐
                          ▼                                      │
┌──────────────────────────────────────────────────────────────────────────────┐
│  LIVE GENERATION (generator_job, hourly cron 0 0 * * * ?)                    │
│                                                                              │
│  Task 1: generate  (main.py mode=live)                                       │
│  ├── window = [now - 1h, now)  rounded to hour boundaries                   │
│  ├── backfill_ticks() with live_tick_seconds sub-ticks (default 60s)        │
│  │   └── 60 sub-ticks × 60s = one sub-tick per minute                       │
│  └── write_batch() → append rows to 5 staging Delta tables                  │
│                                                                              │
│  Task 2: trigger_pipeline  (after generate)                                  │
│  └── Trigger mvm_pipeline update                                             │
└──────────────────────────────────────────────────────────────────────────────┘

                          ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  PIPELINE  (mvm_pipeline, Lakeflow Declarative Pipeline, triggered mode)     │
│                                                                              │
│  Per domain, filter staging by event_type → cast typed columns → silver      │
│                                                                              │
│  SILVER (streaming tables, readStream):                                      │
│  order_events      → guest_order*, order_item, payment, status_event,        │
│                       delivery_order                                          │
│  inventory_events  → on_hand_balance, waste_log*, receiving_order,           │
│                       replenishment_order                                     │
│  guest_events  ─CDC→  guest_profile* (SCD1 via auto_cdc_flow)               │
│                    →  digital_account                                         │
│  loyalty_events    → loyalty_transaction*, reward_redemption                 │
│  workforce_events  → shift, time_punch*                                      │
│                                                                              │
│  * = includes franchisee_id via broadcast join on ref.unit                  │
│                                                                              │
│  GOLD (batch read of silver):                                                │
│  guest_order  →  unit_performance_daily                                      │
│  status_event + guest_order  →  sos_compliance_summary                      │
│  loyalty_transaction  →  loyalty_cohort_metrics                              │
│  waste_log  →  inventory_waste_summary                                       │
└──────────────────────────────────────────────────────────────────────────────┘

                          ┌──────────── daily 05:00 UTC ─────────┐
                          ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  WEATHER & EVENTS REFRESH (weather_events_refresh_job, daily cron)           │
│  refresh_notebook.py → MERGE INTO ref.weather_conditions / ref.local_events  │
│  Feeds backfill demand context + demand_risk_forecast view                   │
└──────────────────────────────────────────────────────────────────────────────┘

                          ┌──────────── weekly Sun 06:00 UTC ────┐
                          ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  FEATURE REFRESH (feature_refresh_job, weekly cron)                          │
│  build_feature_tables.py → rebuild customer_features + store_features         │
│  Keeps recommender lookup features current (no model retrain)                │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Pipeline Refresh Cadence

| Layer | Trigger | Notes |
|---|---|---|
| Staging | Continuous — generator appends each hour | Each `write_batch()` call appends; `mergeSchema=true` allows schema evolution |
| Silver | Triggered — once per generator_job run | Pipeline update triggered by `trigger_pipeline` task after `generate` completes |
| Gold | Same triggered update as silver | Gold tables read silver via `dp.read()` (batch, not streaming) |
| Metric views | Static views — re-read on each query | No refresh needed; views query silver directly |
| `ref.weather_conditions` / `ref.local_events` | Daily — `weather_events_refresh_job` 05:00 UTC | MERGE upsert keyed on `forecast_date` / `event_id` (idempotent) |
| `features.customer_features` / `store_features` | Weekly — `feature_refresh_job` Sun 06:00 UTC | Re-run of `build_feature_tables.py`; recommender retrained only at setup |

## Backfill Window Logic

On `mode=backfill`, the generator determines the start of the backfill window as follows:

1. Query `MAX(event_ts)` across all 5 staging tables.
2. If data exists → start from the next full hour after the max timestamp (avoids duplicating the last partial tick). This makes a destroy/redeploy cycle do incremental backfill rather than regenerating from scratch — the `staging` schema is intentionally preserved by the destroy job.
3. If no data and `start_dt_override` is set → use that ISO datetime.
4. If no data and no override → generate `backfill_months` (default 1) of historical ticks.

IDs are generated via deterministic SHA-256 hashes (`make_id(*parts)` in `id_utils.py`) keyed on `(domain_prefix, unit_id, tick_ts, seq)`, making backfill idempotent — re-running the same window produces the same IDs.

## Sync Status

The `mvm_pipeline` is deployed (pipeline id `dbdf84d9-b3fd-4ea4-82ba-02d26659b13b`) in **triggered (non-continuous), serverless** mode on `channel: PREVIEW`. It does not run on its own schedule — it advances only when the `trigger_pipeline` task fires at the end of each hourly `generator_job` run (or the `start_pipeline` task during setup). Therefore the silver/gold layers are at most one hourly tick behind staging while the generator is unpaused. The customer feature store and recommender are live (the `synth_qsr-recommender` endpoint has been validated end-to-end for both personalized and cold-start paths), with feature tables refreshed weekly and the model retrained only at setup time.

<!-- TODO: human narrative needed — last successful mvm_pipeline update timestamp and current silver row counts (live pipeline run state was unavailable at regeneration time) -->
