# Quickstart

## Prerequisites

<!-- NARRATIVE -->
- A Databricks workspace with Unity Catalog enabled
- A UC catalog pre-created (default: `jmrdemo`) — the setup job verifies it exists but does not create it
- A Databricks CLI profile configured (`DEFAULT` for dev, `aws` for prod)
- Python 3.11+, `databricks-cli` ≥ 0.18, and `faker>=20.0.0` available in the job environment (declared in `generator` environment spec in bundle YAML)
<!-- /NARRATIVE -->

## Environment Variables / Bundle Parameters

All configuration lives in `databricks.yml` as bundle variables. Override at deploy time with `--var key=value`.

| Variable | Default | Description |
|---|---|---|
| `catalog_name` | `jmrdemo` | Unity Catalog catalog. Must be pre-created. |
| `num_units` | `250` | Number of restaurant units to simulate. |
| `backfill_months` | `1` | Months of history to generate on first setup. |
| `live_tick_seconds` | `60` | Sub-tick granularity in seconds for live generation (60 = per-minute). |
| `base_orders_per_unit_per_hour` | `18` | Base hourly order volume per unit. |
| `start_dt_override` | `""` | ISO datetime override for backfill start. Empty = auto from staging MAX(event_ts). |
| `schema_prefix` | `synth_` | Prefix for all UC schemas. Use `""` for no prefix. |
| `weather_refresh_cron` | `0 0 5 * * ?` | Quartz cron for daily weather/events refresh job (default 05:00 UTC). |
| `ticketmaster_secret_scope` | `qsr-synth` | Databricks secret scope containing `ticketmaster_consumer_key`. Omit if not using Ticketmaster. |
| `seatgeek_secret_scope` | `qsr-synth` | Databricks secret scope containing `seatgeek_client_id`. Omit if not using SeatGeek. |
| `ontos_app_url` | `https://ontos-7405605519549535.15.azure.databricksapps.com` | Base URL of the deployed ontos Databricks App. |
| `ontos_enabled` | `true` | Set to `false` to skip ontos configuration steps in setup/destroy. |
| `features_enabled` | `true` | Set to `false` to skip feature store + recommender setup/destroy steps. |
| `feature_refresh_cron` | `0 0 6 ? * SUN` | Quartz cron for the weekly feature-table refresh job (default Sundays 06:00 UTC). |
| `recommender_query_principal` | `""` | Service principal (or user PAT principal) granted `CAN_QUERY` on the recommender endpoint so PizzaTel can call it. Empty = skip the grant. |

## Deploy Steps

```bash
# 1. Clone the repo
git clone https://github.com/jesusr-db/synthData
cd synthData

# 2. Deploy the bundle (creates all job/pipeline resources)
databricks bundle deploy --target dev

# 3. Get the setup job ID
databricks bundle run setup_job --target dev --dry-run
# or: databricks jobs list | grep "QSR Setup"

# 4. Run the setup job (fully automated, ~20-30 min)
databricks bundle run setup_job --target dev

# Alternatively, run the job by ID:
databricks jobs run-now <setup_job_id>
```

The setup job (12 tasks) handles everything in order: catalog check → schemas → staging tables → ref seed → (parallel) initial weather/events refresh → backfill → pipeline start → (parallel after pipeline) feature tables + metric views + governance → recommender training (after feature tables) + Genie Space + monitoring → ontos ontology layer → unpause generator.

If deploying to a workspace without the ontos app, add `--var ontos_enabled=false` to skip the ontology steps:

```bash
databricks bundle deploy --target dev --var ontos_enabled=false
```

If deploying to a workspace where the feature store + recommender are not needed (or the `ml` environment dependencies cannot be installed), add `--var features_enabled=false` to skip the feature-table build and recommender training:

```bash
databricks bundle deploy --target dev --var features_enabled=false
```

## Common Commands

```bash
# Deploy bundle
databricks bundle deploy --target dev

# Redeploy after code change
databricks bundle deploy --target dev

# Run setup from scratch (safe to re-run — IF NOT EXISTS throughout)
databricks bundle run setup_job --target dev

# Run setup without ontos (e.g. ontos app not deployed in target workspace)
databricks bundle run setup_job --target dev --var ontos_enabled=false

# Run setup without feature store + recommender
databricks bundle run setup_job --target dev --var features_enabled=false

# Run just the generator once (backfill mode, custom date range)
databricks jobs run-now <generator_job_id> \
  --job-parameters '{"mode":"backfill","start_dt_override":"2026-05-01T00:00:00"}'

# Manually trigger a weather/events refresh (e.g. after adding API keys)
databricks bundle run weather_events_refresh_job --target dev

# Manually trigger a feature-table refresh (rebuilds customer + store features)
databricks bundle run feature_refresh_job --target dev

# Repair a failed setup_job run (preferred over restarting)
databricks jobs repair-run --run-id <run_id> --rerun-all-failed-tasks

# Tear down non-DAB objects
databricks bundle run destroy_job --target dev

# Tear down DAB-managed resources (jobs, pipeline definitions)
databricks bundle destroy --target dev

# Validate bundle config locally
databricks bundle validate

# Run tests (hermetic — no Spark/Databricks required)
pytest tests/ -v

# Check silver data after setup
databricks sql statement execute \
  --warehouse-id <warehouse_id> \
  --statement "SELECT COUNT(*) FROM jmrdemo.synth_silver.guest_order"
```

## Verifying After Deployment

```sql
-- Check silver row counts
SELECT COUNT(*) FROM jmrdemo.synth_silver.guest_order;
SELECT COUNT(*) FROM jmrdemo.synth_silver.waste_log;

-- Check metric views
SELECT * FROM jmrdemo.synth_metrics.order_performance LIMIT 5;
SELECT * FROM jmrdemo.synth_metrics.loyalty_performance LIMIT 5;

-- Check waste distribution (expected: over_prep ~50%, spoilage ~25%, theft/expiry ~10% each, damaged ~5%)
SELECT waste_category, COUNT(*) FROM jmrdemo.synth_silver.waste_log GROUP BY 1 ORDER BY 2 DESC;

-- Check item status distribution (expected: fulfilled ~87%, cancelled ~12%, refunded ~1%)
SELECT item_status, COUNT(*) FROM jmrdemo.synth_silver.order_item GROUP BY 1;

-- Verify column comments and constraints survived pipeline refresh
DESCRIBE TABLE EXTENDED jmrdemo.synth_silver.guest_order;

-- Verify class.* tags were applied by apply_governance
SELECT table_name, column_name, tag_name, tag_value
FROM system.information_schema.column_tags
WHERE catalog_name = 'jmrdemo'
  AND tag_name LIKE 'class.%'
ORDER BY table_name, column_name;
-- Expected: ~10 rows covering email, phone, first_name, last_name, zip_code
-- on both synth_staging.guest_events and synth_silver.guest_profile

-- Verify per-table column masks are active
SELECT table_name, column_name, mask_function_name
FROM system.information_schema.column_masks
WHERE catalog_name = 'jmrdemo'
  AND schema_name IN ('synth_staging', 'synth_silver');
-- Expected: rows for email and phone on guest_events and guest_profile

-- Verify PII masking is active (per-table SET MASK on email/phone columns)
SELECT email, phone FROM jmrdemo.synth_silver.guest_profile LIMIT 5;
-- email shows as j***@example.com, phone as *******1234

-- Check weather/events ref tables were populated by initial refresh
SELECT COUNT(*), MIN(forecast_date), MAX(forecast_date)
FROM jmrdemo.synth_ref.weather_conditions;
-- Expected: ~880 rows (20 metros × ~44 days: ~30 days back + ~14 days forward)

SELECT COUNT(*) FROM jmrdemo.synth_ref.local_events;
-- Expected: at minimum 28 rows for US federal holidays (current + next year via Nager.Date)
-- Additional Ticketmaster/SeatGeek rows if those API keys are configured

SELECT event_category, COUNT(*)
FROM jmrdemo.synth_ref.local_events
GROUP BY 1 ORDER BY 2 DESC;

-- Check demand_risk_forecast view (populated after initial_weather_refresh completes)
SELECT risk_level, COUNT(*), AVG(demand_multiplier)
FROM jmrdemo.synth_metrics.demand_risk_forecast
GROUP BY 1 ORDER BY 1;
-- Expected: ~3,250 rows total (num_units × 13 forecast days)
-- normal: ~3,191 rows (avg multiplier ~0.97), demand_risk: ~59 rows (avg multiplier ~0.61)
-- Returns 0 rows if initial_weather_refresh has not yet run
```

## Known Failure Modes

<!-- NARRATIVE -->
- **`configure_monitoring` silently succeeds but no monitors appear**: The notebook catches all exceptions. Check cell output in the job run log for `[INFO] Monitor created` vs `[WARN] Monitor skipped`. If skipped, check table ownership with `DESCRIBE EXTENDED {table}` and see [gotchas.md](gotchas.md).
- **`start_pipeline` task fails with "FAILED unexpectedly"**: Transient DLT coordinator error — all flows may have completed. Use `databricks jobs repair-run` to re-run just the failed task rather than restarting the full job.
- **Silver table row counts are 0 after pipeline completes**: The pipeline may have reset streaming checkpoints. Trigger a `full_refresh` via `start_pipeline_notebook.py` or manually in the pipeline UI.
- **`backfill` produces 0 new rows**: The auto-detect logic found an existing `MAX(event_ts)` that is already at the current hour. This is expected — it means staging is current. Use `start_dt_override` to force a specific window.
<!-- /NARRATIVE -->
