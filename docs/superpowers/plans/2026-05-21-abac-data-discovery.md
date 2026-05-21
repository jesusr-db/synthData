# ABAC + Data Discovery + Scheduled Monitoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace manual per-column `SET MASK` governance with tag-driven ABAC policies, wire Data Classification into Lakehouse Monitors so it runs on schedule, and make the full governance story automated end-to-end.

**Architecture:** Lakehouse Monitors (12h cron) run data classification per refresh, writing `class.*` tags to detected PII columns. A single catalog-level ABAC policy binds `class.*` tags to masking UDFs automatically — no per-table DDL. `apply_governance.py` applies deterministic `class.*` tags as a fallback so ABAC works from day one even before the first monitor refresh.

**Tech Stack:** Databricks Unity Catalog, Databricks SDK (`databricks-sdk`), PySpark, Lakeflow Declarative Pipelines, Databricks Asset Bundles

---

## Key Discoveries (read before coding)

- `MonitorDataClassificationConfig(enabled=True)` is a first-class param on `w.quality_monitors.create` — classification runs as part of each monitor refresh, no separate API call needed.
- ABAC policies use `CREATE POLICY ... MATCH COLUMNS (has_tag(...))` SQL DDL — one policy covers the entire catalog.
- Current `apply_governance.py` Step 3 sets `pii=true` which **fails** on this workspace (policy only allows `pii=salary`). These must be replaced with `class.*` namespace tags.
- Current Step 5 per-table `ALTER COLUMN SET MASK` is replaced by the ABAC policy.
- Current Step 8 (classification scan via REST POST) will be removed — monitors handle this now.
- `destroy_notebook.py` must drop ABAC policies **before** dropping functions (same ordering lesson as column masks).
- Branch: `feat/abac-data-discovery`

---

## File Map

| File | Change |
|---|---|
| `src/setup/apply_governance.py` | Step 3: replace `pii=true` tags with `class.*` tags; Step 5: replace per-table SET MASK with ABAC policy DDL; Step 8: remove REST classification call |
| `src/setup/configure_monitoring.py` | Add `MONITOR_SCHEDULE` constant; add `MonitorDataClassificationConfig(enabled=True)` to all monitor creates; add timeseries monitor on `silver.guest_order`; add update path for existing monitors |
| `src/setup/destroy_notebook.py` | Add Step 0e (ABAC policy drop before functions); add `silver.guest_order` to monitor delete loop |

`setup_job.yml` — no changes. `apply_governance → configure_monitoring` chain is already correct.

---

## Task 1: Validate `class.*` Tag Writability and ABAC Policy Syntax

> Gate task — run these checks manually before any code changes. If either fails, see fallback notes.

**Files:**
- No file changes — validation only

- [ ] **Step 1: Test writing a `class.*` tag to a staging column**

In a Databricks notebook or SQL editor connected to `jmrdemo`:
```sql
ALTER TABLE jmrdemo.synth_staging.guest_events
  ALTER COLUMN email SET TAGS ('class.email_address' = 'true');
```
Expected: `OK` or equivalent success response.
If error `Tag key class.email_address is reserved`: switch to Option B — use tag key `governance_class` with value `email_address` throughout the plan instead of `class.email_address`.

- [ ] **Step 2: Verify the tag was written**
```sql
SELECT tag_name, tag_value
FROM system.information_schema.column_tags
WHERE catalog_name = 'jmrdemo'
  AND schema_name = 'synth_staging'
  AND table_name = 'guest_events'
  AND column_name = 'email';
```
Expected: row with `tag_name = 'class.email_address'`, `tag_value = 'true'`.

- [ ] **Step 3: Clean up the test tag**
```sql
ALTER TABLE jmrdemo.synth_staging.guest_events
  ALTER COLUMN email UNSET TAGS ('class.email_address');
```

- [ ] **Step 4: Validate ABAC policy DDL syntax**

In a notebook connected to `jmrdemo`, create a test policy, then immediately drop it:
```sql
-- Create a no-op test policy to verify syntax is accepted
CREATE POLICY IF NOT EXISTS test_abac_syntax_check
  ON CATALOG jmrdemo
  COLUMN MASK jmrdemo.synth_ref.mask_email
  TO `account users`
  FOR TABLES
    MATCH COLUMNS (has_tag('class.email_address')) AS m
  ON COLUMN m;
```
Expected: success.
If syntax error: check [Databricks ABAC docs](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policies) for updated DDL form.

- [ ] **Step 5: Drop the test policy**
```sql
DROP POLICY IF EXISTS test_abac_syntax_check ON CATALOG jmrdemo;
```

- [ ] **Step 6: Note results before continuing**

Document in this checklist which tag key and ABAC predicate syntax worked:
- Tag key: `class.email_address` (or `governance_class` if reserved)
- ABAC predicate: `has_tag('class.email_address')` (or `has_tag_value('class', 'email_address')`)

---

## Task 2: Update `apply_governance.py` — Replace `pii=true` Tags with `class.*` Tags

**Files:**
- Modify: `src/setup/apply_governance.py:172-204` (Step 3)

- [ ] **Step 1: Replace the COLUMN_TAGS list in Step 3**

In `src/setup/apply_governance.py`, replace the entire Step 3 block (lines 171–204):

```python
# COMMAND ----------
# Step 3: Column tags — class.* for PII (feeds ABAC policy); financial and supply_chain unchanged
# class.* tags are the input for ABAC-driven masking; monitors also write these on each refresh.
COLUMN_TAGS = [
    # PII — class.* namespace (data classification standard tags)
    (f"{c}.{p}staging.guest_events", "email",      "class.email_address", "true"),
    (f"{c}.{p}staging.guest_events", "phone",       "class.phone_number",  "true"),
    (f"{c}.{p}staging.guest_events", "first_name",  "class.name",          "true"),
    (f"{c}.{p}staging.guest_events", "last_name",   "class.name",          "true"),
    (f"{c}.{p}staging.guest_events", "zip_code",    "class.zip_code",      "true"),
    (f"{c}.{p}silver.guest_profile", "email",       "class.email_address", "true"),
    (f"{c}.{p}silver.guest_profile", "phone",       "class.phone_number",  "true"),
    (f"{c}.{p}silver.guest_profile", "first_name",  "class.name",          "true"),
    (f"{c}.{p}silver.guest_profile", "last_name",   "class.name",          "true"),
    (f"{c}.{p}silver.guest_profile", "zip_code",    "class.zip_code",      "true"),
    # Financial
    (f"{c}.{p}silver.guest_order",   "subtotal",         "financial", "true"),
    (f"{c}.{p}silver.guest_order",   "discount_amount",  "financial", "true"),
    (f"{c}.{p}silver.guest_order",   "tax_amount",       "financial", "true"),
    (f"{c}.{p}silver.guest_order",   "total_amount",     "financial", "true"),
    (f"{c}.{p}silver.waste_log",     "waste_cost",       "financial", "true"),
    # Supply chain
    (f"{c}.{p}silver.waste_log",       "stock_sku",   "supply_chain", "true"),
    (f"{c}.{p}silver.on_hand_balance", "stock_sku",   "supply_chain", "true"),
    (f"{c}.{p}ref.supplier",           "supplier_id", "supply_chain", "true"),
]

for table, column, tag, value in COLUMN_TAGS:
    try:
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {column} SET TAGS ('{tag}' = '{value}')")
        print(f"[OK] tag {table}.{column} {tag}={value}")
    except Exception as e:
        print(f"[WARN] tag {table}.{column} skipped: {e}")
```

- [ ] **Step 2: Commit**
```bash
git add src/setup/apply_governance.py
git commit -m "feat(governance): replace pii=true tags with class.* namespace for ABAC"
```

---

## Task 3: Update `apply_governance.py` — Replace Per-Table SET MASK with ABAC Policy

**Files:**
- Modify: `src/setup/apply_governance.py:242-256` (Step 5)
- Modify: `src/setup/apply_governance.py:284-330` (Steps 7 and 8)

- [ ] **Step 1: Replace Step 5 (per-table column masks) with ABAC policy DDL**

Replace the Step 5 block (lines 242–256) with:

```python
# COMMAND ----------
# Step 5: ABAC column mask policies — catalog-level, tag-driven
# One policy per masking function; any column tagged class.email_address is masked automatically.
# No per-table ALTER COLUMN SET MASK needed.
ABAC_POLICIES = [
    (
        "mask_email_policy",
        f"{c}.{p}ref.mask_email",
        "has_tag('class.email_address')",
    ),
    (
        "mask_phone_policy",
        f"{c}.{p}ref.mask_phone",
        "has_tag('class.phone_number')",
    ),
]

for policy_name, mask_fn, tag_predicate in ABAC_POLICIES:
    try:
        spark.sql(f"""
            CREATE POLICY IF NOT EXISTS {policy_name}
              ON CATALOG {c}
              COLUMN MASK {mask_fn}
              TO `account users`
              FOR TABLES
                MATCH COLUMNS ({tag_predicate}) AS m
              ON COLUMN m
        """)
        print(f"[OK] ABAC policy {policy_name} -> {mask_fn} for columns where {tag_predicate}")
    except Exception as e:
        print(f"[WARN] ABAC policy {policy_name} skipped: {e}")
```

- [ ] **Step 2: Remove Steps 7 and 8 (old auto_classification and REST scan)**

Delete the entire Step 7 block (lines 284–307, `AUTO_CLASS_PII` loop) and Step 8 block (lines 309–330, `requests.post` classification scan). Replace both with a single comment cell:

```python
# COMMAND ----------
# Data classification is now handled by Lakehouse Monitors (configure_monitoring.py).
# Each monitor refresh runs MonitorDataClassificationConfig(enabled=True), which writes
# class.* tags automatically. The class.* tags applied in Step 3 above serve as the
# deterministic fallback so ABAC policies work before the first monitor refresh.
print("[INFO] Data classification driven by monitors — see configure_monitoring task")
```

- [ ] **Step 3: Update the final print statement**
```python
# COMMAND ----------
print("[INFO] apply_governance complete — volume, comments, class.* tags, functions, ABAC policies, row filters applied")
```

- [ ] **Step 4: Commit**
```bash
git add src/setup/apply_governance.py
git commit -m "feat(governance): replace per-table SET MASK with catalog-level ABAC policies"
```

---

## Task 4: Update `configure_monitoring.py` — Add Classification, Timeseries Monitor, Update Path

**Files:**
- Modify: `src/setup/configure_monitoring.py`

- [ ] **Step 1: Rewrite `configure_monitoring.py` with all changes**

Replace the entire file content after the boilerplate header with:

```python
# Databricks notebook source
# COMMAND ----------
import sys

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_notebook_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

try:
    catalog_name = dbutils.widgets.get("catalog_name")
except Exception:
    catalog_name = "jmrdemo"

try:
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    schema_prefix = "synth_"

print(f"[INFO] configure_monitoring: catalog={catalog_name}, schema_prefix={schema_prefix}")
c = catalog_name
p = schema_prefix

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {c}.{p}metrics")
print(f"[OK] schema {c}.{p}metrics ready")

# COMMAND ----------
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    MonitorSnapshot,
    MonitorTimeSeries,
    MonitorCronSchedule,
    MonitorDataClassificationConfig,
)

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
token = ctx.apiToken().get()
host = spark.conf.get("spark.databricks.workspaceUrl")
w = WorkspaceClient(host=f"https://{host}", token=token)

MONITOR_SCHEDULE = MonitorCronSchedule(
    quartz_cron_expression="0 0 0/12 * * ?",
    timezone_id="UTC",
)

# (table_full_name, assets_dir_suffix, monitor_type_kwargs)
MONITORS = [
    (
        f"{c}.{p}staging.order_events",
        f"{p}order_events",
        {"snapshot": MonitorSnapshot()},
    ),
    (
        f"{c}.{p}staging.inventory_events",
        f"{p}inventory_events",
        {"snapshot": MonitorSnapshot()},
    ),
    (
        f"{c}.{p}staging.loyalty_events",
        f"{p}loyalty_events",
        {"snapshot": MonitorSnapshot()},
    ),
    (
        f"{c}.{p}silver.guest_order",
        f"{p}guest_order",
        {"time_series": MonitorTimeSeries(timestamp_col="placed_at", granularities=["1 day"])},
    ),
]

output_schema = f"{c}.{p}metrics"

for full_name, assets_suffix, monitor_kwargs in MONITORS:
    try:
        spark.sql(f"GRANT SELECT ON TABLE {full_name} TO `account users`")
        print(f"[INFO] SELECT granted on {full_name} to account users")
    except Exception as e:
        print(f"[WARN] SELECT grant skipped for {full_name}: {e}")

    try:
        existing = w.quality_monitors.get(table_name=full_name)
        # Update schedule and classification config if monitor already exists
        try:
            w.quality_monitors.update(
                table_name=full_name,
                schedule=MONITOR_SCHEDULE,
                data_classification_config=MonitorDataClassificationConfig(enabled=True),
            )
            print(f"[INFO] Monitor updated: {full_name} (schedule=0 0 0/12 * * ?, classification=enabled)")
        except Exception as ue:
            print(f"[WARN] Monitor update skipped for {full_name}: {ue}")
    except Exception:
        try:
            w.quality_monitors.create(
                table_name=full_name,
                assets_dir=f"/Shared/qsr_monitors/{assets_suffix}",
                output_schema_name=output_schema,
                schedule=MONITOR_SCHEDULE,
                data_classification_config=MonitorDataClassificationConfig(enabled=True),
                **monitor_kwargs,
            )
            print(f"[INFO] Monitor created: {full_name} (schedule=0 0 0/12 * * ?, classification=enabled)")
        except Exception as e:
            print(f"[WARN] Monitor skipped for {full_name}: {e}")

# COMMAND ----------
print("[INFO] configure_monitoring complete — 4 monitors (3 snapshot + 1 timeseries), 12h schedule, classification enabled")
```

- [ ] **Step 2: Commit**
```bash
git add src/setup/configure_monitoring.py
git commit -m "feat(monitoring): add data classification, timeseries monitor on guest_order, update path"
```

---

## Task 5: Update `destroy_notebook.py` — ABAC Policy Teardown + Timeseries Monitor

**Files:**
- Modify: `src/setup/destroy_notebook.py`

- [ ] **Step 1: Add ABAC policy drop as Step 0e (after monitor delete, before function drops)**

In `destroy_notebook.py`, insert after the existing Step 0d (monitor delete block) and before Step 0b (function drops):

```python
# COMMAND ----------
# Step 0e: Drop ABAC policies BEFORE dropping functions they reference
ABAC_POLICIES = ["mask_email_policy", "mask_phone_policy"]
for policy_name in ABAC_POLICIES:
    try:
        spark.sql(f"DROP POLICY IF EXISTS {policy_name} ON CATALOG {catalog_name}")
        print(f"[INFO] Dropped ABAC policy: {policy_name}")
    except Exception as e:
        print(f"[WARN] Drop ABAC policy {policy_name} skipped: {e}")
```

Place this block **between** Step 0d and Step 0b so the full order is:
```
Step 0a: DROP MASK on staging.guest_events and silver.guest_profile
Step 0e: DROP POLICY (ABAC policies)  ← new
Step 0b: DROP FUNCTION
Step 0c: DROP VOLUME
Step 0d: DELETE monitors              ← move this before 0e
```

Wait — correct ordering:
- Monitors reference tables (not functions), so monitor delete can go first
- ABAC policies reference mask functions → must drop policies before functions
- Column masks (Step 0a) reference functions → must drop masks before functions

Final destroy order:
```
Step 0a: DROP MASK (column masks on staging + silver)
Step 0d: DELETE monitors (Lakehouse Monitors — no function dependency)
Step 0e: DROP POLICY (ABAC policies reference mask functions)
Step 0b: DROP FUNCTION
Step 0c: DROP VOLUME
Step 1+: DROP schemas
```

- [ ] **Step 2: Add `silver.guest_order` to the monitor delete loop in Step 0d**

Find the existing Step 0d monitor delete loop and update the table list:

```python
# Step 0d: Delete Lakehouse Monitors — non-fatal
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    for table in ["order_events", "inventory_events", "loyalty_events"]:
        full_name = f"{catalog_name}.{schema_prefix}staging.{table}"
        try:
            w.quality_monitors.delete(table_name=full_name)
            print(f"[INFO] Monitor deleted: {full_name}")
        except Exception as e:
            print(f"[WARN] Monitor delete skipped for {full_name}: {e}")
    # Timeseries monitor on silver.guest_order
    guest_order_monitor = f"{catalog_name}.{schema_prefix}silver.guest_order"
    try:
        w.quality_monitors.delete(table_name=guest_order_monitor)
        print(f"[INFO] Monitor deleted: {guest_order_monitor}")
    except Exception as e:
        print(f"[WARN] Monitor delete skipped for {guest_order_monitor}: {e}")
except Exception as e:
    print(f"[WARN] Monitor cleanup step skipped entirely: {e}")
```

- [ ] **Step 3: Commit**
```bash
git add src/setup/destroy_notebook.py
git commit -m "feat(governance): add ABAC policy teardown and guest_order monitor delete to destroy"
```

---

## Task 6: Deploy and Verify End-to-End

**Files:** No code changes — deployment and verification only.

- [ ] **Step 1: Deploy the bundle**
```bash
databricks bundle deploy --target dev -p DEFAULT
```
Expected: `Deployment complete!`

- [ ] **Step 2: Run the setup job**
```bash
databricks jobs run-now 408239066702899 -p DEFAULT --output json
```
Note the `run_id` from the output.

- [ ] **Step 3: Monitor until all tasks complete**
```bash
databricks jobs get-run <run_id> --output json -p DEFAULT | python3 -c "
import json,sys
d=json.load(sys.stdin)
print('lifecycle:', d['state']['life_cycle_state'], 'result:', d['state'].get('result_state',''))
for t in d.get('tasks',[]):
    print(' ', t['task_key'], t['state']['life_cycle_state'], t['state'].get('result_state',''))
"
```
Expected: all 8 tasks `TERMINATED SUCCESS`, including `apply_governance` and `configure_monitoring`.

- [ ] **Step 4: Verify ABAC policies exist**
```sql
SELECT policy_name, catalog_name, mask_function_name
FROM system.information_schema.column_mask_policies
WHERE catalog_name = 'jmrdemo';
```
Expected: rows for `mask_email_policy` and `mask_phone_policy`.

- [ ] **Step 5: Verify `class.*` tags were applied**
```sql
SELECT table_name, column_name, tag_name, tag_value
FROM system.information_schema.column_tags
WHERE catalog_name = 'jmrdemo'
  AND tag_name LIKE 'class.%'
ORDER BY table_name, column_name;
```
Expected: 10 rows covering `email`, `phone`, `first_name`, `last_name`, `zip_code` on both `staging.guest_events` and `silver.guest_profile`.

- [ ] **Step 6: Verify all 4 monitors exist with classification enabled**
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(profile='DEFAULT')
tables = [
    'jmrdemo.synth_staging.order_events',
    'jmrdemo.synth_staging.inventory_events',
    'jmrdemo.synth_staging.loyalty_events',
    'jmrdemo.synth_silver.guest_order',
]
for t in tables:
    try:
        m = w.quality_monitors.get(table_name=t)
        classif = m.data_classification_config
        sched = m.schedule.quartz_cron_expression if m.schedule else 'none'
        print(f'[OK] {t} status={m.status} schedule={sched} classification={classif}')
    except Exception as e:
        print(f'[MISS] {t} — {e}')
```
Expected: all 4 monitors `MONITOR_STATUS_ACTIVE`, schedule `0 0 0/12 * * ?`, `classification.enabled=True`.

- [ ] **Step 7: Verify masking works via ABAC**

In a SQL editor (as a non-admin user or using `EXECUTE AS`):
```sql
SELECT email, phone FROM jmrdemo.synth_silver.guest_profile LIMIT 5;
```
Expected: `email` shows `j***@example.com` pattern, `phone` shows `*****1234` pattern — confirming ABAC policy is applying the mask.

- [ ] **Step 8: Commit final state and push branch**
```bash
git add -A
git commit -m "chore: verify ABAC + data discovery implementation complete"
git push -u origin feat/abac-data-discovery
```

---

## Self-Review

**Spec coverage check:**
- ✅ Data Classification auto-tags via `MonitorDataClassificationConfig(enabled=True)` on all monitors
- ✅ `class.*` tags applied deterministically as fallback in `apply_governance.py` Step 3
- ✅ ABAC policy at catalog level replaces per-table SET MASK (Task 3)
- ✅ 12h monitor schedule — single `MONITOR_SCHEDULE` constant, applied and updated idempotently
- ✅ Timeseries monitor on `silver.guest_order` added (Task 4)
- ✅ Destroy ordering: masks → monitors → ABAC policies → functions (Task 5)
- ✅ `pii=true` writes removed (Task 2)
- ✅ REST classification scan removed — monitors handle it (Task 3)
- ✅ Validation gate before coding (Task 1)

**Risks called out inline:**
- Task 1 validates `class.*` namespace writability before any code changes
- Task 1 validates ABAC DDL syntax before Task 3 writes it into a notebook
- If either fails, fallback is documented (Option B: `governance_class` key; per-table SET MASK kept)
