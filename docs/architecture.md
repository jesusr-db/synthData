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
│  │  setup_job  (one-time or on-demand)      │                               │
│  │  9 tasks: setup → backfill,              │                               │
│  │           start_pipeline →              │                               │
│  │             create_metric_views →        │                               │
│  │               create_genie_space         │                               │
│  │             apply_governance →           │                               │
│  │               configure_monitoring       │                               │
│  │           → unpause_generator            │                               │
│  └─────────────────────────────────────────┘                               │
│                                                                             │
│  ┌─────────────────────┐   ┌────────────────────────────────────────────┐  │
│  │  metrics schema      │   │  governance (applied by apply_governance)  │  │
│  │  4 UC Metric Views   │   │  ├── UC column tags (class.*/financial/sc) │  │
│  │  (WITH METRICS YAML) │   │  ├── ABAC mask policies (catalog-level)    │  │
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
| `QSR Setup [dev]` | Job (9 tasks) | Full one-time setup: schemas, staging tables, ref seed, pipeline start, metric views, Genie Space, governance, monitoring, backfill, unpause | Not deployed (run `bundle deploy` first) |
| `QSR Generator Live [dev]` | Job (hourly cron) | Generates previous hour of events across all 5 domains; triggers pipeline | Not deployed |
| `QSR Destroy [dev]` | Job (on-demand) | Tears down all non-DAB objects: ABAC policies, UC functions, volume, monitors, metric views, ref schema, metrics schema | Not deployed |
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

### Why ABAC catalog-level policies instead of per-table `SET MASK`
A single `CREATE POLICY` statement at the catalog level covers every current and future table automatically. Per-table `ALTER COLUMN SET MASK` requires re-running DDL each time a new table with PII columns is added. The ABAC approach also composes naturally with `MonitorDataClassificationConfig(enabled=True)`: monitors write `class.*` tags on each refresh, and the ABAC policy picks them up without any additional DDL. `apply_governance.py` sets deterministic `class.*` tags as a fallback so the policy is effective before the first monitor refresh.

### Why `class.*` column tags instead of `pii=true`
`class.*` is the namespace written by Databricks Data Classification. Using the same namespace for both the deterministic tags (set by `apply_governance.py`) and the auto-detected tags (set by Lakehouse Monitor refreshes) means a single ABAC policy predicate (`has_tag('class.email_address')`) covers both sources. The prior `pii=true` approach required a separate tag namespace and was incompatible with the Data Classification integration.
