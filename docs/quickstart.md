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

# 4. Run the setup job (fully automated, ~15-25 min)
databricks bundle run setup_job --target dev

# Alternatively, run the job by ID:
databricks jobs run-now <setup_job_id>
```

The setup job handles everything in order: catalog check → schemas → staging tables → ref seed → (parallel) backfill + pipeline start → metric views → Genie Space → governance → monitoring → unpause generator.

## Common Commands

```bash
# Deploy bundle
databricks bundle deploy --target dev

# Redeploy after code change
databricks bundle deploy --target dev

# Run setup from scratch (safe to re-run — IF NOT EXISTS throughout)
databricks bundle run setup_job --target dev

# Run just the generator once (backfill mode, custom date range)
databricks jobs run-now <generator_job_id> \
  --job-parameters '{"mode":"backfill","start_dt_override":"2026-05-01T00:00:00"}'

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

-- Verify ABAC catalog-level mask policies exist
SELECT policy_name, catalog_name, mask_function_name
FROM system.information_schema.column_mask_policies
WHERE catalog_name = 'jmrdemo';
-- Expected: rows for mask_email_policy and mask_phone_policy

-- Verify PII masking is active (ABAC policy applies automatically via class.* tags)
SELECT email, phone FROM jmrdemo.synth_silver.guest_profile LIMIT 5;
-- email shows as j***@example.com, phone as *******1234
```

## Known Failure Modes

<!-- NARRATIVE -->
- **`configure_monitoring` silently succeeds but no monitors appear**: The notebook catches all exceptions. Check cell output in the job run log for `[INFO] Monitor created` vs `[WARN] Monitor skipped`. If skipped, check table ownership with `DESCRIBE EXTENDED {table}` and see [gotchas.md](gotchas.md).
- **`start_pipeline` task fails with "FAILED unexpectedly"**: Transient DLT coordinator error — all flows may have completed. Use `databricks jobs repair-run` to re-run just the failed task rather than restarting the full job.
- **Silver table row counts are 0 after pipeline completes**: The pipeline may have reset streaming checkpoints. Trigger a `full_refresh` via `start_pipeline_notebook.py` or manually in the pipeline UI.
- **`backfill` produces 0 new rows**: The auto-detect logic found an existing `MAX(event_ts)` that is already at the current hour. This is expected — it means staging is current. Use `start_dt_override` to force a specific window.
<!-- /NARRATIVE -->
