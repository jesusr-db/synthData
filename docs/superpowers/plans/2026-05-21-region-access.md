# Regional Access (Option B) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Propagate `region_id` to 5 silver tables and extend the row-filter function to support regional-manager access, then create 3 demo workspace groups (`franchisee_1`, `franchisee_2`, `region_1`).

**Architecture:** `region_id` already lives in `ref.unit` (5 regions across 250 units). The `_unit_franchisee()` broadcast-join helper in `mvm_pipeline.py` is the single point where we add `region_id` to the silver layer. `apply_governance.py` extends `filter_by_franchisee` to a 2-arg signature and re-binds all 6 row-filter tables with `ON (franchisee_id, region_id)`. Three demo groups are created by `setup_notebook.py` and cleaned up by `destroy_notebook.py`.

**Tech Stack:** PySpark Lakeflow Declarative Pipelines, Databricks Unity Catalog DDL (CREATE OR REPLACE FUNCTION, SET ROW FILTER, IS_MEMBER), Databricks SDK (WorkspaceClient, groups API), Python.

---

## File Map

| File | Change |
|---|---|
| `src/pipeline/mvm_pipeline.py` | `_unit_franchisee()` + 5 table schemas + `guest_profile_changes` view |
| `src/setup/apply_governance.py` | `filter_by_franchisee` signature + 6 row-filter bindings + `franchise_locations.csv` export |
| `src/setup/setup_notebook.py` | New Step 6 — create 3 demo groups |
| `src/setup/destroy_notebook.py` | New step — delete 3 demo groups |
| `docs/handoff.md` | Update silver description, add regional access section, add demo group docs |

---

## Pre-flight

Create the branch:

```bash
git checkout -b feat/region-access
```

---

### Task 1: Propagate `region_id` to silver tables in `mvm_pipeline.py`

**Files:**
- Modify: `src/pipeline/mvm_pipeline.py:18-22` (`_unit_franchisee` helper)
- Modify: `src/pipeline/mvm_pipeline.py:29-55` (`guest_order` schema)
- Modify: `src/pipeline/mvm_pipeline.py:272-287` (`waste_log` schema)
- Modify: `src/pipeline/mvm_pipeline.py:398-415` (`dp.create_streaming_table` for `guest_profile`)
- Modify: `src/pipeline/mvm_pipeline.py:457-474` (`loyalty_transaction` schema)
- Modify: `src/pipeline/mvm_pipeline.py:567-581` (`time_punch` schema)

> **Note on test philosophy:** `mvm_pipeline.py` is a Lakeflow Declarative Pipeline file — it cannot run without a live Databricks DLT context. There are no unit tests for it in the test suite, and none should be added for this change. Verification is done via schema inspection in the pipeline after full_refresh.

- [ ] **Step 1: Update `_unit_franchisee()` to include `region_id`**

  In `src/pipeline/mvm_pipeline.py`, change lines 18-22:

  ```python
  def _unit_franchisee():
      """Lookup helper: returns (unit_id, franchisee_id, region_id) from ref.unit, broadcast-friendly."""
      return spark.read.table(f"{catalog}.{schema_prefix}ref.unit").select(
          "unit_id", "franchisee_id", "region_id"
      )
  ```

- [ ] **Step 2: Add `region_id BIGINT` to `guest_order` schema**

  In the `@dp.table(name="guest_order", ...)` decorator, add `region_id BIGINT` immediately after `franchisee_id BIGINT`:

  ```
  franchisee_id       BIGINT    COMMENT 'Franchisee owner of the unit (from ref.unit).',
  region_id           BIGINT    COMMENT 'Geographic region of the unit (from ref.unit).',
  channel             STRING    ...
  ```

- [ ] **Step 3: Add `region_id BIGINT` to `waste_log` schema**

  In the `@dp.table(name="waste_log", ...)` decorator, add `region_id BIGINT` after `franchisee_id BIGINT`:

  ```
  franchisee_id  BIGINT COMMENT 'Franchisee owner of the unit (from ref.unit).',
  region_id      BIGINT COMMENT 'Geographic region of the unit (from ref.unit).',
  stock_sku      STRING ...
  ```

- [ ] **Step 4: Add `region_id BIGINT` to `guest_profile` streaming table schema**

  In `dp.create_streaming_table(name="guest_profile", ...)` starting around line 398, add `region_id BIGINT` after `franchisee_id BIGINT`:

  ```
  franchisee_id    BIGINT  COMMENT 'Franchisee owner of the unit (from ref.unit).',
  region_id        BIGINT  COMMENT 'Geographic region of the unit (from ref.unit).',
  first_name       STRING,
  ```

  The `guest_profile_changes_view` function itself does `df.join(broadcast(ref_unit), on="unit_id", how="left")` — once `_unit_franchisee()` returns `region_id`, the join already passes it through. No change needed to the view body.

- [ ] **Step 5: Add `region_id BIGINT` to `loyalty_transaction` schema**

  In the `@dp.table(name="loyalty_transaction", ...)` decorator, add `region_id BIGINT` after `franchisee_id BIGINT`:

  ```
  franchisee_id          BIGINT    COMMENT 'Franchisee owner of the unit (from ref.unit).',
  region_id              BIGINT    COMMENT 'Geographic region of the unit (from ref.unit).',
  transaction_type       STRING    ...
  ```

- [ ] **Step 6: Add `region_id BIGINT` to `time_punch` schema**

  In the `@dp.table(name="time_punch", ...)` decorator, add `region_id BIGINT` after `franchisee_id BIGINT`:

  ```
  franchisee_id BIGINT COMMENT 'Franchisee owner of the unit (from ref.unit).',
  region_id     BIGINT COMMENT 'Geographic region of the unit (from ref.unit).',
  punch_in      TIMESTAMP,
  ```

- [ ] **Step 7: Syntax-check the file**

  ```bash
  python3 -c "import ast; ast.parse(open('src/pipeline/mvm_pipeline.py').read()); print('OK')"
  ```

  Expected output: `OK`

- [ ] **Step 8: Commit**

  ```bash
  git add src/pipeline/mvm_pipeline.py
  git commit -m "feat(pipeline): propagate region_id to 5 silver tables via _unit_franchisee join"
  ```

---

### Task 2: Extend `filter_by_franchisee` for regional access in `apply_governance.py`

**Files:**
- Modify: `src/setup/apply_governance.py:46-57` (franchise_locations export — add region_id)
- Modify: `src/setup/apply_governance.py:293-315` (row filter function + bindings)

- [ ] **Step 1: Add `region_id` to franchise_locations export**

  In `apply_governance.py`, find the `franchise_locations.csv` export around line 46. Change the `.select(...)` call from:

  ```python
  unit_df = spark.read.table(f"{c}.{p}ref.unit").select(
      "unit_id", "unit_name", "city", "state", "franchisee_id"
  )
  ```

  to:

  ```python
  unit_df = spark.read.table(f"{c}.{p}ref.unit").select(
      "unit_id", "unit_name", "city", "state", "franchisee_id", "region_id"
  )
  ```

- [ ] **Step 2: Replace `filter_by_franchisee` function definition**

  In `apply_governance.py`, find the `CREATE OR REPLACE FUNCTION ... filter_by_franchisee` block (around line 293). Replace the entire `spark.sql(...)` call for the function:

  ```python
  spark.sql(f"""
  CREATE OR REPLACE FUNCTION {c}.{p}ref.filter_by_franchisee(franchisee_id BIGINT, region_id BIGINT)
  RETURNS BOOLEAN
  RETURN IS_MEMBER(CONCAT('franchisee_', CAST(franchisee_id AS STRING)))
      OR IS_MEMBER(CONCAT('region_', CAST(region_id AS STRING)))
      OR IS_MEMBER('qsr_admin')
  """)
  print(f"[OK] function {c}.{p}ref.filter_by_franchisee")
  ```

- [ ] **Step 3: Update all 6 `SET ROW FILTER` bindings**

  In `apply_governance.py`, find the loop over `ROW_FILTER_TABLES` (around line 301). Change the `ALTER TABLE ... SET ROW FILTER` line from:

  ```python
  spark.sql(f"ALTER TABLE {table} SET ROW FILTER {c}.{p}ref.filter_by_franchisee ON (franchisee_id)")
  ```

  to:

  ```python
  spark.sql(f"ALTER TABLE {table} SET ROW FILTER {c}.{p}ref.filter_by_franchisee ON (franchisee_id, region_id)")
  ```

  The 6 tables (`guest_order`, `waste_log`, `loyalty_transaction`, `guest_profile`, `time_punch`, `ref.unit`) all have both `franchisee_id` and `region_id` after the pipeline full_refresh. `ref.unit` already had `region_id` before this change.

- [ ] **Step 4: Syntax-check the file**

  ```bash
  python3 -c "import ast; ast.parse(open('src/setup/apply_governance.py').read()); print('OK')"
  ```

  Expected output: `OK`

- [ ] **Step 5: Commit**

  ```bash
  git add src/setup/apply_governance.py
  git commit -m "feat(governance): extend filter_by_franchisee to region_id for regional manager access"
  ```

---

### Task 3: Create demo workspace groups in `setup_notebook.py`

**Files:**
- Modify: `src/setup/setup_notebook.py` (add Step 6 after existing Step 5)

- [ ] **Step 1: Add Step 6 COMMAND block for demo groups**

  In `src/setup/setup_notebook.py`, append a new COMMAND block at the end of the file (after `print("[INFO] Setup complete")`):

  ```python
  # COMMAND ----------
  # Step 6: Create demo workspace groups for row-filter testing — non-fatal
  # franchisee_<n>: row filter matches units owned by that franchisee
  # region_<n>:     row filter matches all units in that geographic region
  DEMO_GROUPS = ["franchisee_1", "franchisee_2", "region_1"]
  try:
      from databricks.sdk import WorkspaceClient
      _wc = WorkspaceClient()
      for _group_name in DEMO_GROUPS:
          _existing = list(_wc.groups.list(filter=f"displayName eq '{_group_name}'", attributes="id,displayName"))
          if not _existing:
              _wc.groups.create(display_name=_group_name)
              print(f"[OK] Created demo group: {_group_name}")
          else:
              print(f"[INFO] Demo group already exists: {_group_name}")
  except Exception as e:
      print(f"[WARN] Demo group creation skipped (requires workspace admin): {e}")
  ```

  > Use fresh `_wc = WorkspaceClient()` and underscore-prefixed variables to avoid clobbering the `w` and `me` variables from Step 5, which are in a separate try block and may not be defined.

- [ ] **Step 2: Syntax-check the file**

  ```bash
  python3 -c "import ast; ast.parse(open('src/setup/setup_notebook.py').read()); print('OK')"
  ```

  Expected output: `OK`

- [ ] **Step 3: Commit**

  ```bash
  git add src/setup/setup_notebook.py
  git commit -m "feat(setup): create franchisee_1, franchisee_2, region_1 demo workspace groups"
  ```

---

### Task 4: Clean up demo groups in `destroy_notebook.py`

**Files:**
- Modify: `src/setup/destroy_notebook.py` (add new step before Step 0b functions)

- [ ] **Step 1: Add demo group cleanup step**

  In `src/setup/destroy_notebook.py`, add a new COMMAND block between Step 0e (ABAC policies) and Step 0b (UC functions). Insert after the closing `except` of Step 0e:

  ```python
  # COMMAND ----------
  # Step 0f: Delete demo workspace groups — non-fatal
  DEMO_GROUPS = ["franchisee_1", "franchisee_2", "region_1"]
  try:
      from databricks.sdk import WorkspaceClient
      from databricks.sdk.errors import NotFound
      _wc = WorkspaceClient()
      for _group_name in DEMO_GROUPS:
          _existing = list(_wc.groups.list(filter=f"displayName eq '{_group_name}'", attributes="id,displayName"))
          if _existing:
              _wc.groups.delete(id=_existing[0].id)
              print(f"[INFO] Deleted demo group: {_group_name}")
          else:
              print(f"[INFO] Demo group not found (ok): {_group_name}")
  except Exception as e:
      print(f"[WARN] Demo group cleanup skipped: {e}")
  ```

- [ ] **Step 2: Syntax-check the file**

  ```bash
  python3 -c "import ast; ast.parse(open('src/setup/destroy_notebook.py').read()); print('OK')"
  ```

  Expected output: `OK`

- [ ] **Step 3: Commit**

  ```bash
  git add src/setup/destroy_notebook.py
  git commit -m "feat(destroy): clean up franchisee_1, franchisee_2, region_1 demo groups"
  ```

---

### Task 5: Update `docs/handoff.md` for regional access

**Files:**
- Modify: `docs/handoff.md`

- [ ] **Step 1: Update silver layer description**

  Find the line:
  > `All silver tables include `franchisee_id` (left-joined from `ref.unit`).`

  Replace with:
  > `All silver tables include `franchisee_id` and `region_id` (left-joined from `ref.unit` via `_unit_franchisee()` helper).`

- [ ] **Step 2: Update Key Files entry for mvm_pipeline.py**

  Find:
  > `src/pipeline/mvm_pipeline.py               # Lakeflow Declarative Pipeline (14 silver + 4 gold; franchisee_id on 5 tables)`

  Replace with:
  > `src/pipeline/mvm_pipeline.py               # Lakeflow Declarative Pipeline (14 silver + 4 gold; franchisee_id + region_id on 5 tables)`

- [ ] **Step 3: Add regional access section after the "What's Built" section**

  After the "What's Built" section and before "Deployed Resources", add:

  ```markdown
  ---

  ## Access Control Model

  Row isolation is enforced via a UC row filter function bound to 6 tables (5 silver + `ref.unit`):

  ```sql
  FUNCTION filter_by_franchisee(franchisee_id BIGINT, region_id BIGINT)
  RETURNS BOOLEAN
  RETURN IS_MEMBER(CONCAT('franchisee_', CAST(franchisee_id AS STRING)))
      OR IS_MEMBER(CONCAT('region_', CAST(region_id AS STRING)))
      OR IS_MEMBER('qsr_admin')
  ```

  | Group pattern | Who it gives access to | Example |
  |---|---|---|
  | `franchisee_<id>` | All units owned by that franchisee | `franchisee_1` → sees only franchisee 1's stores |
  | `region_<id>` | All units in that geographic region | `region_1` → sees all stores in region 1 |
  | `qsr_admin` | All units (full access) | `jesus.rodriguez@databricks.com` is a member |

  **Demo groups pre-created by setup:** `franchisee_1`, `franchisee_2`, `region_1`
  (no users added — add yourself via Workspace Settings → Groups to test isolation)

  **Tables with row filter:** `silver.guest_order`, `silver.waste_log`, `silver.loyalty_transaction`, `silver.guest_profile`, `silver.time_punch`, `ref.unit`

  ---
  ```

- [ ] **Step 4: Add demo group verification to "Verifying After Deployment" section**

  Add the following SQL block to the Verifying section:

  ```sql
  -- Test franchisee row isolation: add yourself to franchisee_1 group first, remove from qsr_admin
  -- Then verify you only see rows for units owned by franchisee 1:
  SELECT DISTINCT franchisee_id FROM jmrdemo.synth_silver.guest_order;
  -- Expected: only franchisee_id = 1

  -- Test regional access: add yourself to region_1 group first
  -- Then verify you see rows for all units in region 1:
  SELECT DISTINCT region_id FROM jmrdemo.synth_silver.guest_order;
  -- Expected: only region_id = 1

  -- Check ref.unit region distribution (5 regions across 250 units):
  SELECT region_id, COUNT(*) AS unit_count FROM jmrdemo.synth_ref.unit GROUP BY 1 ORDER BY 1;
  -- Expected: 5 rows, ~50 units per region
  ```

- [ ] **Step 5: Commit**

  ```bash
  git add docs/handoff.md
  git commit -m "docs: document regional access model, demo groups, and verification queries"
  ```

---

## Post-Implementation

- [ ] **Run tests to confirm no regressions**

  ```bash
  pytest tests/ -v
  ```

  Expected: all 75 tests pass. These tests cover the generator and seeder only — `mvm_pipeline.py` and `apply_governance.py` are not unit-tested (require live Databricks context).

- [ ] **Use `superpowers:finishing-a-development-branch` to merge or create PR**

---

## Re-deployment Notes (for whoever runs this against jmrdemo)

After merging and deploying via `databricks bundle deploy --target dev`:

1. Run the **QSR Setup job** — this does a pipeline `full_refresh` (adds `region_id` to silver tables), then re-runs `apply_governance` (updates row filter to 2-arg signature and rebinds all 6 tables), and creates the 3 demo groups.
2. That's it. The setup job is fully idempotent — safe to re-run on an existing deployment.

> **ABAC reminder:** `start_pipeline_notebook.py` drops ABAC policies before `full_refresh`. After `full_refresh`, `apply_governance` recreates them. The new row filter with `region_id` is bound after pipeline completes, so all 6 tables have the column before the filter is attached.
