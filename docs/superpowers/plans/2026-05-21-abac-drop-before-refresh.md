# ABAC Drop-Before-Refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable catalog-level ABAC column masking on DLT-managed tables by dropping ABAC policies before `full_refresh` and recreating them after, eliminating the current per-table `SET MASK` workaround.

**Architecture:** `start_pipeline_notebook.py` drops ABAC policies before triggering DLT `full_refresh` (DLT rejects full_refresh when catalog ABAC policies are bound). `apply_governance.py` recreates the policies after the pipeline finishes — this already happens because `apply_governance` runs after `start_pipeline` in the task graph. `destroy_notebook.py` gets a new Step 0e to drop policies before dropping the mask functions they reference.

**Tech Stack:** Databricks Unity Catalog ABAC, PySpark `spark.sql()`, Databricks SDK (`WorkspaceClient`), Databricks Asset Bundles

---

## Background: What's Already Confirmed

All DDL patterns below were validated end-to-end in jmrdemo on 2026-05-21.

- Exact working CREATE POLICY syntax:
  ```sql
  CREATE POLICY {name} ON CATALOG {catalog}
    COLUMN MASK {mask_fn}
    TO `account users`
    FOR TABLES
      MATCH COLUMNS (has_tag('{tag_name}')) AS m
    ON COLUMN m
  ```
- Exact working DROP POLICY syntax: `DROP POLICY {name} ON CATALOG {catalog}` (no `IF EXISTS` — syntax error)
- `SHOW POLICIES ON CATALOG {catalog}` returns columns: `Policy Name`, `Policy Type`, `Catalog`, `Schema`, `Table`, `Comment`
- `class.*` tag values must be `''` (empty string) — already correct in current `apply_governance.py`
- Idempotency pattern: SHOW POLICIES → check `Policy Name` column → conditionally DROP → CREATE

## Why There Are No Unit Tests in This Plan

All three files are Databricks notebooks. Their logic depends on `spark`, `dbutils`, and `WorkspaceClient` — none of which are available in the local pytest suite. The existing 75 tests cover only the Python generator (no Spark). Verification is via the end-to-end setup job run in Task 4.

---

## File Map

| File | Change |
|---|---|
| `src/setup/start_pipeline_notebook.py` | Add `drop_abac_policies_before_refresh()` called before the pipeline state check |
| `src/setup/apply_governance.py` | Step 5: replace per-table `SET MASK` loop with idempotent ABAC `CREATE POLICY` (includes best-effort legacy mask drop) |
| `src/setup/destroy_notebook.py` | Add Step 0e (ABAC policy teardown) between Step 0d and Step 0b |

`resources/setup_job.yml` — **no changes**. The `start_pipeline → apply_governance` ordering is already correct.

---

## Task 1: `start_pipeline_notebook.py` — Drop ABAC Policies Before Full Refresh

**Files:**
- Modify: `src/setup/start_pipeline_notebook.py:8-17` (after catalog_name setup, before pipeline lookup)

- [ ] **Step 1: Add the ABAC drop helper and call it**

Open `src/setup/start_pipeline_notebook.py`. After line 17 (`catalog_name = dbutils.widgets.get("catalog_name")`), insert a new `# COMMAND ----------` block:

```python
# COMMAND ----------
# Drop ABAC catalog policies before full_refresh.
# DLT rejects full_refresh with ABAC_POLICIES_NOT_SUPPORTED when catalog-level policies are bound.
# apply_governance (which runs after this task) will recreate them.
ABAC_POLICY_NAMES = ["mask_email_policy", "mask_phone_policy"]

def drop_abac_policies_before_refresh(catalog: str) -> None:
    try:
        existing = {row["Policy Name"] for row in spark.sql(f"SHOW POLICIES ON CATALOG {catalog}").collect()}
    except Exception as e:
        print(f"[WARN] Could not check ABAC policies, skipping drop: {e}")
        return
    for policy_name in ABAC_POLICY_NAMES:
        if policy_name in existing:
            try:
                spark.sql(f"DROP POLICY {policy_name} ON CATALOG {catalog}")
                print(f"[INFO] Dropped ABAC policy: {policy_name} (apply_governance will recreate)")
            except Exception as e:
                print(f"[WARN] Drop ABAC policy {policy_name} failed: {e}")
        else:
            print(f"[INFO] ABAC policy {policy_name} not present — nothing to drop")

drop_abac_policies_before_refresh(catalog_name)
```

This block goes between the `catalog_name` line and the `w = WorkspaceClient()` line. The final order of top-level statements in the file will be:

```
# COMMAND ----------    ← existing: imports + catalog_name widget
catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------    ← NEW: drop ABAC policies
...drop_abac_policies_before_refresh(catalog_name)

# COMMAND ----------    ← existing: pipeline lookup + helper functions
w = WorkspaceClient()
...
```

- [ ] **Step 2: Verify the file still parses**

```bash
python3 -c "
import ast
with open('src/setup/start_pipeline_notebook.py') as f:
    src = f.read()
# Strip notebook magic comments before parsing
clean = '\n'.join(l for l in src.splitlines() if not l.startswith('# COMMAND') and not l.startswith('# Databricks'))
ast.parse(clean)
print('OK: syntax valid')
"
```
Expected: `OK: syntax valid`

- [ ] **Step 3: Commit**

```bash
git add src/setup/start_pipeline_notebook.py
git commit -m "feat(governance): drop ABAC policies before DLT full_refresh to avoid ABAC_POLICIES_NOT_SUPPORTED"
```

---

## Task 2: `apply_governance.py` — Replace Per-Table SET MASK with ABAC CREATE POLICY

**Files:**
- Modify: `src/setup/apply_governance.py:240-256` (Step 5)

- [ ] **Step 1: Replace the Step 5 block**

In `src/setup/apply_governance.py`, replace the entire Step 5 block (lines 240–256, from `# Step 5: Column masks on PII email/phone` through the end of the `for` loop) with:

```python
# COMMAND ----------
# Step 5: ABAC column mask policies — catalog-level, tag-driven
# Per-table SET MASK is NOT used; one policy per mask function covers all tagged columns catalog-wide.
# start_pipeline_notebook.py drops these before DLT full_refresh to avoid ABAC_POLICIES_NOT_SUPPORTED.

# Best-effort: drop any legacy per-table masks left from prior SET MASK runs to avoid double-masking
for _table, _col in [
    (f"{c}.{p}staging.guest_events", "email"),
    (f"{c}.{p}staging.guest_events", "phone"),
    (f"{c}.{p}silver.guest_profile", "email"),
    (f"{c}.{p}silver.guest_profile", "phone"),
]:
    try:
        spark.sql(f"ALTER TABLE {_table} ALTER COLUMN {_col} DROP MASK")
        print(f"[INFO] Dropped legacy per-table mask: {_table}.{_col}")
    except Exception:
        pass  # expected if mask was not set

# ABAC policies — idempotent: SHOW POLICIES → drop if exists → create
ABAC_POLICIES = [
    ("mask_email_policy", f"{c}.{p}ref.mask_email", "class.email_address"),
    ("mask_phone_policy", f"{c}.{p}ref.mask_phone", "class.phone_number"),
]

try:
    _existing_policies = {
        row["Policy Name"]
        for row in spark.sql(f"SHOW POLICIES ON CATALOG {c}").collect()
    }
except Exception as e:
    print(f"[WARN] SHOW POLICIES failed, assuming empty: {e}")
    _existing_policies = set()

for policy_name, mask_fn, tag_name in ABAC_POLICIES:
    try:
        if policy_name in _existing_policies:
            spark.sql(f"DROP POLICY {policy_name} ON CATALOG {c}")
            print(f"[INFO] Dropped existing ABAC policy: {policy_name}")
        spark.sql(f"""
            CREATE POLICY {policy_name}
              ON CATALOG {c}
              COLUMN MASK {mask_fn}
              TO `account users`
              FOR TABLES
                MATCH COLUMNS (has_tag('{tag_name}')) AS m
              ON COLUMN m
        """)
        print(f"[OK] ABAC policy {policy_name}: {mask_fn} for columns with tag '{tag_name}'")
    except Exception as e:
        print(f"[WARN] ABAC policy {policy_name} skipped: {e}")
```

- [ ] **Step 2: Update the final print statement**

Find and replace the last print in the file:

Old:
```python
print("[INFO] apply_governance complete — volume, comments, class.* tags, functions, column masks, row filters applied")
```

New:
```python
print("[INFO] apply_governance complete — volume, comments, class.* tags, functions, ABAC policies, row filters applied")
```

- [ ] **Step 3: Verify the file still parses**

```bash
python3 -c "
import ast
with open('src/setup/apply_governance.py') as f:
    src = f.read()
clean = '\n'.join(l for l in src.splitlines() if not l.startswith('# COMMAND') and not l.startswith('# Databricks'))
ast.parse(clean)
print('OK: syntax valid')
"
```
Expected: `OK: syntax valid`

- [ ] **Step 4: Commit**

```bash
git add src/setup/apply_governance.py
git commit -m "feat(governance): replace per-table SET MASK with catalog-level ABAC policies"
```

---

## Task 3: `destroy_notebook.py` — Add ABAC Policy Teardown

**Files:**
- Modify: `src/setup/destroy_notebook.py:73-82` (between Step 0d and Step 0b)

The required teardown order is:
```
Step 0a: DROP per-table column masks (staging.guest_events + silver.guest_profile)
Step 0d: DELETE Lakehouse Monitors
Step 0e: DROP ABAC policies             ← new — must precede function drops
Step 0b: DROP FUNCTION
Step 0c: DROP VOLUME
Step 1+: DROP schemas
```

- [ ] **Step 1: Insert Step 0e between Step 0d and Step 0b**

In `src/setup/destroy_notebook.py`, find the line `# Step 0b: Drop UC functions (governance pack)` (currently at line 75). Insert a new `# COMMAND ----------` block immediately before it:

```python
# COMMAND ----------
# Step 0e: Drop ABAC policies BEFORE dropping mask functions they reference.
# DROP POLICY has no IF EXISTS guard — use SHOW POLICIES to check existence first.
_ABAC_POLICIES = ["mask_email_policy", "mask_phone_policy"]
try:
    _existing = {
        row["Policy Name"]
        for row in spark.sql(f"SHOW POLICIES ON CATALOG {catalog_name}").collect()
    }
    for _policy_name in _ABAC_POLICIES:
        if _policy_name in _existing:
            spark.sql(f"DROP POLICY {_policy_name} ON CATALOG {catalog_name}")
            print(f"[INFO] Dropped ABAC policy: {_policy_name}")
        else:
            print(f"[INFO] ABAC policy {_policy_name} not found (ok)")
except Exception as e:
    print(f"[WARN] ABAC policy cleanup skipped: {e}")
```

- [ ] **Step 2: Verify the file still parses**

```bash
python3 -c "
import ast
with open('src/setup/destroy_notebook.py') as f:
    src = f.read()
clean = '\n'.join(l for l in src.splitlines() if not l.startswith('# COMMAND') and not l.startswith('# Databricks'))
ast.parse(clean)
print('OK: syntax valid')
"
```
Expected: `OK: syntax valid`

- [ ] **Step 3: Commit**

```bash
git add src/setup/destroy_notebook.py
git commit -m "feat(governance): add ABAC policy teardown to destroy notebook (Step 0e)"
```

---

## Task 4: Deploy and Verify End-to-End

**Files:** No code changes — deploy and verify only.

- [ ] **Step 1: Deploy the bundle**

```bash
databricks bundle deploy --target dev -p DEFAULT
```
Expected: `Deployment complete!` (or equivalent — no error lines)

- [ ] **Step 2: Run the setup job**

```bash
databricks jobs run-now --job-id $(databricks bundle run --dry-run setup_job 2>/dev/null | grep job_id | awk '{print $2}') -p DEFAULT
```

Or if you know the job ID (currently `460616743434988` on jmrdemo):
```bash
databricks jobs run-now 460616743434988 -p DEFAULT
```

Note the `run_id` from the output.

- [ ] **Step 3: Monitor job progress until all tasks complete**

Poll every 30s until `life_cycle_state` is `TERMINATED`:
```bash
databricks jobs get-run <run_id> -p DEFAULT | python3 -c "
import json, sys
d = json.load(sys.stdin)
state = d['state']
print(f'lifecycle={state[\"life_cycle_state\"]} result={state.get(\"result_state\",\"-\")}')
for t in d.get('tasks', []):
    ts = t['state']
    print(f'  {t[\"task_key\"]}: {ts[\"life_cycle_state\"]} {ts.get(\"result_state\",\"\")}')
"
```
Expected: all 8 tasks `TERMINATED SUCCESS`.

- [ ] **Step 4: Verify ABAC policies exist in Unity Catalog**

```sql
SELECT `Policy Name`, `Policy Type`, `Catalog`
FROM (SHOW POLICIES ON CATALOG jmrdemo)
WHERE `Policy Name` IN ('mask_email_policy', 'mask_phone_policy');
```
Expected: 2 rows, both `Policy Type = COLUMN_MASK`.

- [ ] **Step 5: Verify per-table masks are gone**

```sql
SELECT table_schema, table_name, column_name, mask_name
FROM system.information_schema.column_masks
WHERE table_catalog = 'jmrdemo';
```
Expected: 0 rows (ABAC replaced per-table masks; no `SET MASK` bindings should remain).

- [ ] **Step 6: Confirm masking fires via ABAC**

```sql
SELECT email, phone FROM jmrdemo.synth_staging.guest_events LIMIT 3;
```
Expected: `email` shows `j***@example.com` pattern, `phone` shows `*****1234` pattern. If both columns return plain text, ABAC policies are not firing — check that `class.*` tags are present on those columns (Step 7 below).

- [ ] **Step 7: Confirm class.* tags are still present**

```sql
SELECT table_name, column_name, tag_name, tag_value
FROM system.information_schema.column_tags
WHERE catalog_name = 'jmrdemo' AND tag_name LIKE 'class.%'
ORDER BY table_name, column_name;
```
Expected: 10 rows — `email`, `phone`, `first_name`, `last_name`, `zip_code` on both `synth_staging.guest_events` and `synth_silver.guest_profile`, all with `tag_value = ''`.

- [ ] **Step 8: Confirm DLT full_refresh succeeded (no ABAC error)**

Check that `start_pipeline` task log contains:
```
[INFO] Dropped ABAC policy: mask_email_policy (apply_governance will recreate)
[INFO] Dropped ABAC policy: mask_phone_policy (apply_governance will recreate)
```
(or `not present — nothing to drop` on first run) and does **not** contain `ABAC_POLICIES_NOT_SUPPORTED`.

- [ ] **Step 9: Update docs/handoff.md to reflect ABAC is now active**

In `docs/handoff.md`:

1. In the **What's Built** section, change:
   > column masks (email/phone via per-table `SET MASK`)
   
   to:
   > column masks (email/phone via catalog-level ABAC policies — `CREATE POLICY ... MATCH COLUMNS (has_tag(...))`)

2. In the **Important behaviors** section, change:
   > **`apply_governance`** is fully idempotent — uses `CREATE OR REPLACE` / `IF NOT EXISTS` throughout. Per-table `SET MASK` (not ABAC) — see Known Gotchas.
   
   to:
   > **`apply_governance`** is fully idempotent — Step 5 uses ABAC `CREATE POLICY` (SHOW POLICIES → DROP if exists → CREATE). **`start_pipeline`** drops ABAC policies before `full_refresh` to avoid `ABAC_POLICIES_NOT_SUPPORTED`.

3. In the **Open Issues** section, remove the `ABAC masking not yet wired` section entirely (it's now resolved).

4. In the **Known Gotchas** table, update the ABAC row:
   
   Old:
   > | Catalog-level ABAC policies (`CREATE POLICY ON CATALOG`) cause DLT full_refresh to fail | DLT rejects `full_refresh` on tables that have catalog-level ABAC policies bound: `ABAC_POLICIES_NOT_SUPPORTED`. | Reverted Step 5 of `apply_governance.py` to per-table `SET MASK`. ABAC is the intended end state — drop-before-refresh pattern in `start_pipeline_notebook.py` is the remaining work. See Open Issues. |
   
   New:
   > | Catalog-level ABAC policies cause DLT full_refresh to fail with `ABAC_POLICIES_NOT_SUPPORTED` | DLT pipeline-owned tables cannot have catalog ABAC policies active during `full_refresh`. | `start_pipeline_notebook.py` drops ABAC policies before triggering `full_refresh`; `apply_governance` recreates them after. |

- [ ] **Step 10: Commit docs and push branch**

```bash
git add docs/handoff.md
git commit -m "docs: update handoff — ABAC drop-before-refresh implemented, open issue resolved"
git push origin feat/abac-data-discovery
```

---

## Self-Review

**Spec coverage (from `docs/handoff.md` Open Issues):**
- ✅ "Drop ABAC policies in `start_pipeline_notebook.py` before triggering the full_refresh" → Task 1
- ✅ "`apply_governance` recreates them after" → Task 2 (ABAC CREATE POLICY replaces SET MASK)
- ✅ Destroy cleanup → Task 3

**Placeholder scan:** None found — all steps contain exact code.

**Type/name consistency:**
- Policy names `mask_email_policy` / `mask_phone_policy` — consistent across Task 1, Task 2, Task 3
- `SHOW POLICIES ON CATALOG` column `"Policy Name"` — confirmed from observation 1859 (6-column schema)
- Tag names `class.email_address` / `class.phone_number` — consistent with existing Step 3 `COLUMN_TAGS` list
- Mask functions `{c}.{p}ref.mask_email` / `{c}.{p}ref.mask_phone` — consistent with Step 4 `CREATE OR REPLACE FUNCTION`

**Risk notes:**
- On first-ever run, `SHOW POLICIES` returns 0 rows for our policy names → drop is a no-op → CREATE succeeds. No special-casing needed.
- If `SHOW POLICIES` itself fails (workspace tier issue), both `start_pipeline` and `apply_governance` catch and warn without failing the task.
- `destroy_notebook.py` Step 0a (drop per-table masks) is kept — it's a no-op if ABAC is in use, but protects against legacy runs that still had SET MASK bound.
