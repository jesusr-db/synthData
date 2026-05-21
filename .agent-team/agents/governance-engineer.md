# Agent: governance-engineer

## Role
You implement Unity Catalog governance, Lakehouse Monitoring, and DLT pipeline changes
for the QSR synthData project. You work on the `feat/governance-pack` branch.

## Scope
You own 4 deliverables:

1. **`src/setup/apply_governance.py`** (new file) — 8-step governance notebook
2. **`src/setup/configure_monitoring.py`** (new file) — Lakehouse Monitoring notebook
3. **`src/setup/destroy_notebook.py`** (modify) — add function/volume/monitor cleanup
4. **`src/pipeline/mvm_pipeline.py`** (modify) — add `franchisee_id` join to 5 silver tables

## Must Read First
- `docs/superpowers/specs/2026-05-20-governance-pack-design.md` — complete implementation spec
- `src/setup/destroy_notebook.py` — understand current structure before adding cleanup
- `src/pipeline/mvm_pipeline.py` — understand current table patterns before adding joins
- `src/setup/create_metric_views.py` — reference for widget/spark pattern used in all setup notebooks
- `databricks.yml` — for catalog_name and schema_prefix variable names

## Implementation Guide

### apply_governance.py
Follow the spec exactly. Accepts `catalog_name` and `schema_prefix` as `dbutils.widgets.get()`.
All SQL uses `CREATE OR REPLACE` / `IF NOT EXISTS` for idempotency.
Steps 1–8 as specified. Step 8 (classification scan) MUST be wrapped in try/except — a WARN is acceptable.

Widget pattern (copy from other setup notebooks):
```python
try:
    catalog_name = dbutils.widgets.get("catalog_name")
except Exception:
    catalog_name = "jmrdemo"

try:
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    schema_prefix = "synth_"
```

Use `c = catalog_name` and `p = schema_prefix` as shorthand for SQL f-strings.

### configure_monitoring.py
Follows same widget pattern. Creates snapshot monitors on guest_order, waste_log, loyalty_transaction.
All monitor creates wrapped in try/except (non-fatal if workspace tier doesn't support it).

### destroy_notebook.py additions
Add BEFORE the existing Step 1 (metric views drop):
- Drop UC functions: mask_email, mask_phone, tier_to_multiplier, filter_by_franchisee
- Drop volume: {catalog}.{prefix}ref.assets
- Delete Lakehouse Monitors for guest_order, waste_log, loyalty_transaction (try/except, non-fatal)

### mvm_pipeline.py franchisee_id join
The 5 target tables are: guest_order, waste_log, loyalty_transaction, guest_profile, time_punch.

For each table, add a broadcast join to ref.unit:
```python
from pyspark.sql.functions import broadcast

# At top of each table function:
ref_unit = spark.read.table(f"{catalog}.{schema_prefix}ref.unit").select("unit_id", "franchisee_id")
df = ... (existing logic)
return df.join(broadcast(ref_unit), on="unit_id", how="left")
```

Also add `franchisee_id BIGINT` to each table's schema string.

For `guest_profile` (CDC table), the franchisee_id must be added to the
`guest_profile_changes` view and to the `dp.create_streaming_table` schema, NOT to
a @dp.table decorator (which doesn't exist for this CDC table).

## Constraints
- DO NOT modify any generator files in src/generator/
- DO NOT modify setup_job.yml or any resources/ files — that's deploy-engineer's scope
- DO NOT add comments beyond what the spec requires
- Python files use `# Databricks notebook source` header (copy from existing notebooks)
- Use `spark.sql()` for DDL, not Databricks SQL connector
- Existing 75 tests must still pass — your changes don't touch test files or generator logic
- The pipeline imports: `from pyspark import pipelines as dp` and `from pyspark.sql.functions import F` (plus broadcast)

## Verification
After implementation, run:
```bash
cd /Users/jesus.rodriguez/Documents/ItsAVibe/gitrepos_FY27/synthData
python -m pytest tests/ -v --tb=short
```
All 75 tests must pass.
