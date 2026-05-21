# Dataflow

## End-to-End Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  SETUP (one-time, setup_job)                                                 │
│                                                                              │
│  1. setup_notebook.py                                                        │
│     ├── Verify catalog exists (must be pre-created)                          │
│     ├── CREATE SCHEMA IF NOT EXISTS  staging, ref, metrics                   │
│     ├── CREATE TABLE IF NOT EXISTS  5 staging tables (wide sparse schema)    │
│     └── seed_all() → write ref tables (unit, franchisee, financial_period,   │
│                        item_price, menu_item, recipe_ingredient, supplier)    │
│                                                                              │
│  2. backfill  (parallel with start_pipeline)                                 │
│     └── main.py mode=backfill                                                │
│         ├── Read MAX(event_ts) across staging tables                         │
│         ├── If no data → generate backfill_months of hourly ticks            │
│         ├── If data exists → resume from next full hour                      │
│         └── write_batch() → append to 5 staging Delta tables                │
│                                                                              │
│  3. start_pipeline_notebook.py                                               │
│     ├── Poll for any active pipeline update (wait)                           │
│     ├── Trigger pipeline update                                              │
│     └── Fall back to full_refresh=True if update fails                      │
│                                                                              │
│  4a. create_metric_views.py   (after start_pipeline)                        │
│      └── CREATE OR REPLACE VIEW  metrics.order_performance,                 │
│           loyalty_performance, inventory_waste, staff_hours                  │
│                                                                              │
│  4b. apply_governance.py      (after start_pipeline)                        │
│      ├── CREATE VOLUME  ref.assets                                           │
│      ├── Export menu_catalog_csv + franchise_locations_csv + sample_receipt  │
│      ├── COMMENT ON TABLE  for 28 tables/views                               │
│      ├── ALTER COLUMN COMMENT  on PII, financial, supply_chain columns       │
│      ├── ALTER COLUMN SET TAGS  (pii, financial, supply_chain)               │
│      ├── CREATE FUNCTION  mask_email, mask_phone, tier_to_multiplier         │
│      ├── ALTER COLUMN SET MASK  (email, phone on guest_profile + guest_events│
│      ├── CREATE FUNCTION  filter_by_franchisee                               │
│      ├── ALTER TABLE SET ROW FILTER  on 6 silver/ref tables                  │
│      └── POST /api/2.1/unity-catalog/data-classification-tasks (best-effort) │
│                                                                              │
│  5. create_genie_space.py     (after create_metric_views)                   │
│     └── REST API: create Genie Space with 10 seed questions                  │
│                                                                              │
│  6. configure_monitoring.py   (after apply_governance)                      │
│     └── SDK: create snapshot monitors on  order_events, inventory_events,   │
│              loyalty_events  (staging tables — best-effort, non-fatal)       │
│                                                                              │
│  7. unpause_generator_notebook.py  (after backfill + create_genie_space +   │
│                                      configure_monitoring)                   │
│     └── SDK: set generator_job schedule to UNPAUSED                         │
└──────────────────────────────────────────────────────────────────────────────┘

                          ┌──────────── every hour ─────────────┐
                          ▼                                      │
┌──────────────────────────────────────────────────────────────────────────────┐
│  LIVE GENERATION (generator_job, hourly cron)                                │
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
```

## Pipeline Refresh Cadence

| Layer | Trigger | Notes |
|---|---|---|
| Staging | Continuous — generator appends each hour | Each `write_batch()` call appends; `mergeSchema=true` allows schema evolution |
| Silver | Triggered — once per generator_job run | Pipeline update triggered by `trigger_pipeline` task after `generate` completes |
| Gold | Same triggered update as silver | Gold tables read silver via `dp.read()` (batch, not streaming) |
| Metric views | Static views — re-read on each query | No refresh needed; views query silver directly |

## Backfill Window Logic

On `mode=backfill`, the generator determines the start of the backfill window as follows:

1. Query `MAX(event_ts)` across all 5 staging tables.
2. If data exists → start from the next full hour after the max timestamp (avoids duplicating the last partial tick).
3. If no data and `start_dt_override` is set → use that ISO datetime.
4. If no data and no override → generate `backfill_months` (default 1) of historical ticks.

IDs are generated via deterministic SHA-256 hashes (`make_id(*parts)` in `id_utils.py`) keyed on `(domain_prefix, unit_id, tick_ts, seq)`, making backfill idempotent — re-running the same window produces the same IDs.

## Sync Status

<!-- TODO: human narrative needed — current pipeline run status and last successful update timestamp -->
