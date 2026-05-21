# Governance Pack — Design Spec

**Date:** 2026-05-20
**Branch:** feat/hourly-live-generator
**Scope:** Add Unity Catalog governance, data classification, Lakehouse Monitoring, volumes, and row-level security capabilities as new setup_job tasks for the DPZ POC.

---

## Problem

The synthData generator produces realistic QSR data in Unity Catalog, but the tables have no governance metadata: no column tags, no masks, no row filters, no descriptions, no monitors. For the DPZ POC, every workstream (ABAC, Purview, Power BI RLS, AI Governance, Delta Sharing) needs the data to be *governed*, not just present.

---

## Scope

### In scope
- Column and table descriptions (COMMENT ON)
- Column tags: `pii`, `financial`, `supply_chain`
- UC scalar functions: `mask_email`, `mask_phone`, `tier_to_multiplier`
- Column masks on PII email/phone columns
- Per-franchisee row filter on silver transactional tables
- UC Volume with sample unstructured files
- Databricks automated data classification scan
- Lakehouse Monitoring on 3 silver tables
- Destroy cleanup for all new objects
- Pipeline change: `franchisee_id` added to 5 silver tables

### Out of scope
- MLflow model registration (Sprint 2)
- AI Gateway / inference tables (Sprint 2)
- Delta Share (Sprint 2)
- External system JDBC mirrors (SA team)
- Volume file generation via unstructured-pdf-generation skill (plain CSV/JSON files only)

---

## Architecture

### Task DAG Changes

```
setup ──→ backfill ──────────────────────────────────────────────────────────────┐
      └─→ start_pipeline ──→ create_metric_views ──→ create_genie_space ─────────┤
                         └──→ apply_governance ──→ configure_monitoring ─────────┤
                                                                                  ↓
                                                                      unpause_generator
```

`unpause_generator` gains a new dependency on `configure_monitoring` (in addition to existing `backfill` and `create_genie_space`).

`apply_governance` and `create_metric_views` run in parallel after `start_pipeline` (silver tables exist because `start_pipeline_notebook.py` blocks until the DLT update completes).

### New Files

| File | Task key | Description |
|---|---|---|
| `src/setup/apply_governance.py` | `apply_governance` | Volume, descriptions, tags, masks, row filter, classification |
| `src/setup/configure_monitoring.py` | `configure_monitoring` | Lakehouse Monitoring on 3 silver tables |

### Changed Files

| File | Change |
|---|---|
| `resources/setup_job.yml` | Add 2 new tasks; update `unpause_generator` deps |
| `resources/destroy_job.yml` | Pass `catalog_name` + `schema_prefix` (already done) |
| `src/setup/destroy_notebook.py` | Cleanup: functions, volume, monitors |
| `src/pipeline/mvm_pipeline.py` | Add `franchisee_id` join to 5 silver tables |

---

## Detailed Design

### `apply_governance.py`

Accepts `catalog_name` and `schema_prefix` widgets (same pattern as all other setup notebooks). All steps are idempotent — safe to re-run.

**Step 1 — Create volume + sample files**

```python
spark.sql(f"CREATE VOLUME IF NOT EXISTS {c}.{p}ref.assets")
```

Write 3 files to `/Volumes/{catalog}/{schema_prefix}ref/assets/`:
- `menu_catalog.csv` — exported from `ref.menu_item` via spark read → write
- `franchise_locations.csv` — exported from `ref.unit` (unit_id, unit_name, city, state, franchisee_id)
- `sample_receipt.json` — single synthetic JSON receipt constructed inline (no faker, just a hardcoded representative dict)

**Step 2 — Table + column descriptions**

`COMMENT ON TABLE {c}.{p}silver.guest_order IS '...'` for every silver, staging, and ref table (one SQL statement per table).

`ALTER TABLE ... ALTER COLUMN ... COMMENT '...'` for:
- PII columns (first_name, last_name, email, phone, zip_code)
- Financial columns (subtotal, discount_amount, tax_amount, total_amount, waste_cost)
- Supply-chain columns (stock_sku, supplier_id, waste_quantity)
- Key dimension columns (unit_id, channel, order_type, tier, transaction_type)

**Step 3 — Column tags**

```sql
ALTER TABLE {c}.{p}staging.guest_events
  ALTER COLUMN email SET TAGS ('pii' = 'true');
```

Tag targets:
- `pii = 'true'`: `email`, `phone`, `first_name`, `last_name`, `zip_code` — on `staging.guest_events` and `silver.guest_profile`
- `financial = 'true'`: `subtotal`, `discount_amount`, `tax_amount`, `total_amount` on `silver.guest_order`; `waste_cost` on `silver.waste_log`
- `supply_chain = 'true'`: `stock_sku` on `silver.waste_log` and `silver.on_hand_balance`; `supplier_id` on `ref.supplier`

**Step 4 — UC scalar functions**

```sql
CREATE OR REPLACE FUNCTION {c}.{p}ref.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN email IS NULL THEN NULL
  WHEN INSTR(email, '@') > 1 THEN CONCAT(LEFT(email, 1), REPEAT('*', INSTR(email,'@')-2), SUBSTR(email, INSTR(email,'@')))
  ELSE '***'
END;

CREATE OR REPLACE FUNCTION {c}.{p}ref.mask_phone(phone STRING)
RETURNS STRING
RETURN CASE
  WHEN phone IS NULL THEN NULL
  ELSE CONCAT(REPEAT('*', GREATEST(0, LENGTH(REGEXP_REPLACE(phone,'[^0-9]','')) - 4)), RIGHT(REGEXP_REPLACE(phone,'[^0-9]',''), 4))
END;

CREATE OR REPLACE FUNCTION {c}.{p}ref.tier_to_multiplier(tier STRING)
RETURNS DOUBLE
RETURN CASE tier
  WHEN 'bronze' THEN 1.0
  WHEN 'silver' THEN 1.5
  WHEN 'gold'   THEN 2.0
  WHEN 'elite'  THEN 3.0
  ELSE 1.0
END;
```

All functions live in `{schema_prefix}ref` schema (co-located with ref tables, not a separate functions schema).

**Step 5 — Column masks**

```sql
ALTER TABLE {c}.{p}staging.guest_events
  ALTER COLUMN email SET MASK {c}.{p}ref.mask_email;

ALTER TABLE {c}.{p}staging.guest_events
  ALTER COLUMN phone SET MASK {c}.{p}ref.mask_phone;

ALTER TABLE {c}.{p}silver.guest_profile
  ALTER COLUMN email SET MASK {c}.{p}ref.mask_email;

ALTER TABLE {c}.{p}silver.guest_profile
  ALTER COLUMN phone SET MASK {c}.{p}ref.mask_phone;
```

**Step 6 — Row filter function + attach**

```sql
CREATE OR REPLACE FUNCTION {c}.{p}ref.filter_by_franchisee(franchisee_id BIGINT)
RETURNS BOOLEAN
RETURN IS_ACCOUNT_GROUP_MEMBER(CONCAT('franchisee_', CAST(franchisee_id AS STRING)))
    OR IS_ACCOUNT_GROUP_MEMBER('qsr_admin');
```

Apply to all silver tables that carry `franchisee_id` after the pipeline change:
- `silver.guest_order`, `silver.waste_log`, `silver.loyalty_transaction`, `silver.guest_profile`, `silver.time_punch`
- Also `ref.unit` (already has `franchisee_id`)

```sql
ALTER TABLE {c}.{p}silver.guest_order
  SET ROW FILTER {c}.{p}ref.filter_by_franchisee ON (franchisee_id);
```

**Step 7 — Databricks system classification tags**

Apply `databricks:auto_classification:pii` tag on PII columns so the UC classification panel recognises them as pre-classified:

```sql
ALTER TABLE {c}.{p}silver.guest_profile
  ALTER COLUMN email SET TAGS ('databricks:auto_classification:pii' = 'true');
```

Same set of columns as Step 3.

**Step 8 — Trigger UC automated data classification scan**

Use `WorkspaceClient` to submit classification tasks for 4 silver tables:

```python
from databricks.sdk import WorkspaceClient
import requests, json

w = WorkspaceClient()
host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

for table_name in ["guest_profile", "guest_order", "waste_log", "loyalty_transaction"]:
    full_name = f"{catalog_name}.{schema_prefix}silver.{table_name}"
    try:
        resp = requests.post(
            f"https://{host}/api/2.1/unity-catalog/data-classification-tasks",
            headers={"Authorization": f"Bearer {token}"},
            json={"table_name": full_name},
            timeout=30,
        )
        resp.raise_for_status()
        print(f"[INFO] Classification task submitted: {full_name}")
    except Exception as e:
        print(f"[WARN] Classification task skipped for {full_name}: {e}")
```

Uses the REST API directly — SDK method name for this endpoint is not yet stable. Wrapped in try/except because the classification API may not be available in all workspace tiers — a warning is acceptable; this must not fail the task.

---

### `configure_monitoring.py`

Accepts `catalog_name` and `schema_prefix` widgets.

Creates a snapshot monitor on each of 3 tables using the Databricks SDK:

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorSnapshot

w = WorkspaceClient()
TABLES = ["guest_order", "waste_log", "loyalty_transaction"]

for table in TABLES:
    full_name = f"{catalog_name}.{schema_prefix}silver.{table}"
    output_schema = f"{catalog_name}.{schema_prefix}metrics"
    try:
        w.quality_monitors.create(
            table_name=full_name,
            assets_dir=f"/Shared/qsr_monitors/{schema_prefix}{table}",
            output_schema_name=output_schema,
            snapshot=MonitorSnapshot(),
        )
        print(f"[INFO] Monitor created: {full_name}")
    except Exception as e:
        # Already exists or workspace tier doesn't support it — non-fatal
        print(f"[WARN] Monitor skipped for {full_name}: {e}")
```

Monitor profile and drift tables land in `{schema_prefix}metrics` schema alongside the existing metric views.

---

### Pipeline change: `mvm_pipeline.py`

For each of the 5 target silver tables, add a broadcast join to pull `franchisee_id` from `ref.unit`:

```python
# Pattern for each table (example: guest_order)
ref_unit = spark.read.table(f"{catalog_name}.{schema_prefix}ref.unit").select("unit_id", "franchisee_id")

@dp.table(name="guest_order", ...)
def guest_order():
    df = ...  # existing logic
    return df.join(broadcast(ref_unit), on="unit_id", how="left")
```

The join is `left` so rows with a null `unit_id` (if any) are preserved. `franchisee_id` will be null for those rows — acceptable.

Tables receiving `franchisee_id`: `guest_order`, `waste_log`, `loyalty_transaction`, `guest_profile`, `time_punch`.

---

### `setup_job.yml` additions

```yaml
- task_key: apply_governance
  depends_on:
    - task_key: start_pipeline
  notebook_task:
    notebook_path: ../src/setup/apply_governance.py
    base_parameters:
      catalog_name: ${var.catalog_name}
      schema_prefix: ${var.schema_prefix}

- task_key: configure_monitoring
  depends_on:
    - task_key: apply_governance
  notebook_task:
    notebook_path: ../src/setup/configure_monitoring.py
    base_parameters:
      catalog_name: ${var.catalog_name}
      schema_prefix: ${var.schema_prefix}
```

`unpause_generator` gains:
```yaml
depends_on:
  - task_key: backfill
  - task_key: create_genie_space
  - task_key: configure_monitoring   # new
```

---

### `destroy_notebook.py` additions

New cleanup section (runs before dropping schemas):

```python
# Drop UC functions
FUNCTIONS = ["mask_email", "mask_phone", "tier_to_multiplier", "filter_by_franchisee"]
for fn in FUNCTIONS:
    spark.sql(f"DROP FUNCTION IF EXISTS {catalog_name}.{schema_prefix}ref.{fn}")

# Drop volume
spark.sql(f"DROP VOLUME IF EXISTS {catalog_name}.{schema_prefix}ref.assets")

# Delete Lakehouse Monitors
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
for table in ["guest_order", "waste_log", "loyalty_transaction"]:
    full_name = f"{catalog_name}.{schema_prefix}silver.{table}"
    try:
        w.quality_monitors.delete(table_name=full_name)
        print(f"[INFO] Monitor deleted: {full_name}")
    except Exception as e:
        print(f"[WARN] Monitor delete skipped for {full_name}: {e}")
```

Column masks and tags are implicitly removed when the silver/staging schemas are dropped by `databricks bundle destroy`.

---

## Error Handling

- All `apply_governance.py` SQL steps use `CREATE OR REPLACE` / `IF NOT EXISTS` — re-runnable without side effects.
- Data classification scan (Step 8) and Lakehouse Monitor creation are wrapped in `try/except` with `[WARN]` output — workspace tier limitations must not fail the setup job.
- Pipeline change is a `left` join — null franchisee_id rows pass through rather than being dropped.

## Testing

- Existing 75 tests remain unaffected (generator tests, pipeline unit tests).
- Manual verification: re-run setup job on dev, check UC Data Explorer for tags/masks/descriptions, verify row filter on `silver.guest_order`, confirm 3 monitors appear in `{schema_prefix}metrics`.
- No new automated tests required for governance SQL (it's declarative DDL).
