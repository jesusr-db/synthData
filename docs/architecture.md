# Architecture

## System Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  QSR Synthetic Data Generator — Databricks Workspace                        │
│                                                                             │
│  ┌─────────────────────────────────────────┐                               │
│  │  generator_job  (every hour, serverless) │                               │
│  │  src/generator/main.py  mode=live        │                               │
│  │  ├── EntityRegistry (from ref tables)    │                               │
│  │  ├── backfill_ticks() (60 sub-ticks)     │                               │
│  │  └── write_batch() → 5 staging tables   │                               │
│  └──────────────┬──────────────────────────┘                               │
│                 │ trigger_pipeline task                                      │
│                 ▼                                                            │
│  ┌─────────────────────────────────────────────────────────┐               │
│  │  mvm_pipeline  (Lakeflow Declarative Pipeline, triggered) │               │
│  │  src/pipeline/mvm_pipeline.py                            │               │
│  │                                                          │               │
│  │  STAGING (5 wide Delta tables)  →  SILVER (14 tables)   │               │
│  │  order_events      ─────────────  guest_order           │               │
│  │                    ─────────────  order_item            │               │
│  │                    ─────────────  payment               │               │
│  │                    ─────────────  status_event          │               │
│  │                    ─────────────  delivery_order        │               │
│  │  inventory_events  ─────────────  on_hand_balance       │               │
│  │                    ─────────────  waste_log             │               │
│  │                    ─────────────  receiving_order       │               │
│  │                    ─────────────  replenishment_order   │               │
│  │  guest_events  ─CDC─────────────  guest_profile        │               │
│  │                    ─────────────  digital_account       │               │
│  │  loyalty_events    ─────────────  loyalty_transaction   │               │
│  │                    ─────────────  reward_redemption     │               │
│  │  workforce_events  ─────────────  shift                 │               │
│  │                    ─────────────  time_punch            │               │
│  │                                                          │               │
│  │  SILVER → GOLD (4 aggregate tables, co-located silver)  │               │
│  │  guest_order   →  unit_performance_daily                 │               │
│  │  status_event  →  sos_compliance_summary                 │               │
│  │  loyalty_txn   →  loyalty_cohort_metrics                 │               │
│  │  waste_log     →  inventory_waste_summary                │               │
│  └─────────────────────────────────────────────────────────┘               │
│                                                                             │
│  ┌─────────────────────────────────────────┐                               │
│  │  weather_events_refresh_job  (daily cron)│                               │
│  │  src/refresh/refresh_notebook.py         │                               │
│  │  ├── Open-Meteo → weather_conditions     │                               │
│  │  ├── NOAA NWS alerts (overlaid on rows)  │                               │
│  │  ├── Nager.Date holidays → local_events  │                               │
│  │  ├── Ticketmaster (optional, key-gated)  │                               │
│  │  └── SeatGeek    (optional, key-gated)  │                               │
│  │  MERGE INTO ref.weather_conditions       │                               │
│  │  MERGE INTO ref.local_events             │                               │
│  │  env: refresh (requests, pyyaml)         │                               │
│  └─────────────────────────────────────────┘                               │
│                                                                             │
│  ┌─────────────────────────────────────────┐                               │
│  │  setup_job  (one-time or on-demand)      │                               │
│  │  9 tasks: setup →                        │                               │
│  │           initial_weather_refresh ────┐  │                               │
│  │           (both) → backfill →         │  │                               │
│  │           start_pipeline →            │  │                               │
│  │             create_metric_views →     │  │                               │
│  │               create_genie_space ─────────┐                             │
│  │             apply_governance →        │   │                             │
│  │               configure_monitoring ───────┤                             │
│  │           backfill + create_genie_space│  │                             │
│  │           + configure_monitoring ─────────┴→ unpause_generator          │
│  │  envs: generator (faker), refresh (requests, pyyaml)                    │
│  └─────────────────────────────────────────┘                               │
│                                                                             │
│  ┌─────────────────────┐   ┌────────────────────────────────────────────┐  │
│  │  metrics schema      │   │  governance (applied by apply_governance)  │  │
│  │  4 UC Metric Views   │   │  ├── UC column tags (class.*/financial/sc) │  │
│  │  (WITH METRICS YAML) │   │  ├── Per-table column masks (SET MASK)     │  │
│  └─────────────────────┘   │  ├── Row filters  (filter_by_franchisee)   │  │
│                             │  ├── UC Volume  (ref.assets)               │  │
│  ┌─────────────────────┐   │  └── Lakehouse Monitors (3 snap + 1 ts)    │  │
│  │  Genie Space         │   └────────────────────────────────────────────┘  │
│  │  10 seed questions   │                                                    │
│  │  all silver+metrics  │                                                    │
│  └─────────────────────┘                                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Deployed Resources

| Name | Type | Purpose | Status |
|---|---|---|---|
| `QSR Setup [dev]` | Job (9 tasks) | Full one-time setup: schemas, staging tables, ref seed, initial weather/events refresh, backfill, pipeline start, metric views, Genie Space, governance, monitoring, unpause | Not deployed (run `bundle deploy` first) |
| `QSR Generator Live [dev]` | Job (hourly cron) | Generates previous hour of events across all 5 domains; triggers pipeline | Not deployed |
| `Weather & Events Refresh [dev]` | Job (daily cron, 05:00 UTC) | Refreshes `ref.weather_conditions` and `ref.local_events` from Open-Meteo, NOAA NWS alerts, Nager.Date holidays, Ticketmaster, and SeatGeek. Note: the yml includes an explicit name prefix that causes a duplicate `[dev ...]` prefix when DAB also auto-prepends one — remove the explicit prefix from the yml before merge. | Not deployed |
| `QSR Destroy [dev]` | Job (on-demand) | Tears down all non-DAB objects: column masks, UC functions, volume, monitors, metric views, ref schema, metrics schema | Not deployed |
| `QSR MVM Pipeline [dev]` | Lakeflow Declarative Pipeline | Streaming promotion of staging → silver → gold; serverless, triggered mode | Not deployed |

All resources are tagged `project: qsr-synth-data-generator`.

## Design Decisions

### Why wide/sparse staging tables instead of narrow per-event-type tables
Each staging table (`order_events`, etc.) holds multiple `event_type` values in a single wide schema. All columns not relevant to a given event type are NULL. This lets the generator write all order-related events in one append per batch, and lets the DLT pipeline filter them with `.filter(F.col("event_type") == "...")` inside each silver table function. The alternative — one staging table per event type — would multiply the number of tables and require the generator to manage more write targets without gaining meaningful query performance.

### Why `CREATE TABLE IF NOT EXISTS` for staging tables (never `CREATE OR REPLACE`)
DLT streaming tables maintain internal checkpoint state keyed on the Delta table ID. If `setup_notebook.py` drops and re-creates a staging table, the table ID changes, and all DLT streaming flows that read it fail with a checkpoint mismatch. Using `IF NOT EXISTS` preserves the table ID across re-runs, so the pipeline can be re-started safely without a full reset.

### Why franchisee_id is joined in the pipeline, not stored in staging
The generator emits `unit_id` on every event. `franchisee_id` is a slowly-changing attribute of `unit_id` that lives in `ref.unit`. Storing it in the pipeline (via a broadcast join in each silver table function) avoids duplicating the ref data in staging and keeps the join logic in one place. The `_unit_franchisee()` helper in `mvm_pipeline.py` centralizes the broadcast join pattern across the five tables that need it (`guest_order`, `waste_log`, `loyalty_transaction`, `time_punch`, `guest_profile_changes`).

### Why the DLT pipeline declares schema inline in `@dp.table`
DLT re-materializes silver table metadata on every update. Comments applied externally via `COMMENT ON TABLE` or `ALTER COLUMN COMMENT` are overwritten each time. Declaring `comment=` and `schema=` (with inline column `COMMENT` and `CONSTRAINT ... NOT ENFORCED`) directly in the decorator is the only way to make metadata durable.

### Why `dp.create_auto_cdc_flow` for `guest_profile` but `@dp.table` for everything else
`guest_profile` can receive update events (churn/deactivation) that share the same `guest_profile_id`. Using CDC (SCD Type 1) ensures later events overwrite earlier ones rather than creating duplicate rows. All other event types are append-only and don't need CDC semantics.

### Why per-table `SET MASK` instead of ABAC catalog-level policies
Unity Catalog ABAC catalog-level `CREATE POLICY` is not supported on tables owned by a DLT pipeline. Silver and staging tables are DLT-managed, so attempting to attach an ABAC policy causes DLT pipeline failures with a `catalog-level ABAC` error. Per-table `ALTER COLUMN SET MASK` DDL works correctly on DLT-managed tables and is the required approach. `apply_governance.py` applies masks directly on the four PII columns (`email`, `phone` on `staging.guest_events` and `silver.guest_profile`) with explicit try/except so re-runs are safe.

### Why `class.*` column tags instead of `pii=true`
`class.*` is the namespace written by Databricks Data Classification. Using the same namespace for both the deterministic tags (set by `apply_governance.py`) and the auto-detected tags (set by Lakehouse Monitor refreshes with `MonitorDataClassificationConfig(enabled=True)`) keeps a consistent tagging standard. The prior `pii=true` approach used a custom namespace incompatible with the Data Classification integration. `class.*` tags also make PII columns discoverable in Catalog Explorer's tag-based search without any additional configuration.

### Why `start_pipeline` depends on `backfill`, not `setup`
The DLT pipeline reads from staging tables. If `start_pipeline` ran immediately after `setup` (before `backfill`), the pipeline would process an empty staging layer and produce zero silver rows. Depending on `backfill` ensures the pipeline has data to process on its first full refresh. `apply_governance` also depends on `start_pipeline` so silver tables exist before column masks and row filters are attached.

### Why `backfill` depends on both `setup` and `initial_weather_refresh`
The backfill generator reads `ref.weather_conditions` and `ref.local_events` to compute demand multipliers for historical ticks. Running backfill before those tables are populated produces ticks with no weather or event context — effectively generating flat demand curves. Gating `backfill` on `initial_weather_refresh` ensures the ref tables are populated with real forward-looking data (±30 days) before any synthetic history is written.

### Why Ticketmaster and SeatGeek are optional in the refresh notebook
Both event APIs require API keys stored in Databricks secrets. The refresh notebook wraps each provider's secret fetch in a bare `except` — if the secret scope or key doesn't exist, the provider is silently skipped and the job continues. Holidays from Nager.Date always run unconditionally. This makes the job functional in environments that haven't configured third-party keys, without requiring conditional bundle config.

### Why serverless tasks declare dependencies in `environments:` rather than task-level `libraries:`
Databricks serverless notebook tasks do not support the `libraries:` field at the task level. The Terraform provider rejects a bundle deploy with `"Libraries field is not supported for serverless task, please specify libraries in environment."` Notably, `databricks bundle validate` (a schema-only check) passes locally without catching this — the error only surfaces at deploy time. The correct pattern: declare an `environments:` block at the job level with a named `spec.dependencies` list, then reference it on each task via `environment_key: <name>`. Both `setup_job.yml` and `refresh_weather_events.yml` use a `refresh` environment spec (`requests`, `pyyaml`) for this reason.
