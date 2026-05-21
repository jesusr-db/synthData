# Guest Profile CDC Fix + 1-Day Rebuild Test Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the ~19 duplicate `guest_profile_id` rows in `silver.guest_profile` by migrating to CDC merge, wire a `start_dt_override` variable through the bundle so setup can run a fast 1-day backfill, then exercise the full destroy cycle to verify staging data survives.

**Architecture:** Replace the `@dp.table guest_profile()` append-stream in `mvm_pipeline.py` with a `@dp.view` source + `dp.create_streaming_table` + `dp.create_auto_cdc_flow` (SCD Type 1). Add a `start_dt_override` bundle variable (default `""`) wired to the backfill task so the setup job can be scoped to a narrow window. Deploy → run 1-day setup → run destroy job → `bundle destroy` → verify staging intact.

**Tech Stack:** `pyspark.pipelines` (Lakeflow Declarative Pipelines), Databricks Asset Bundles, Databricks CLI

---

## File Map

| File | Change |
|------|--------|
| `src/pipeline/mvm_pipeline.py:361-394` | Replace `@dp.table guest_profile()` append stream with `@dp.view guest_profile_changes()` + `dp.create_streaming_table` + `dp.create_auto_cdc_flow` |
| `databricks.yml` | Add `start_dt_override` variable (default `""`) |
| `resources/setup_job.yml` | Add `start_dt_override: ${var.start_dt_override}` to backfill task `base_parameters` |

---

## Task 1: Replace `guest_profile` append stream with CDC flow

**Files:**
- Modify: `src/pipeline/mvm_pipeline.py:361-394`

- [ ] **Step 1.1: Replace the `guest_profile` table block**

In `mvm_pipeline.py`, replace lines 361–394 (the `@dp.table guest_profile()` block) with the following three blocks. Insert them in the same location, keeping the surrounding `# GUEST DOMAIN` comment and leaving `digital_account` untouched.

```python
@dp.view(name="guest_profile_changes")
def guest_profile_changes_view():
    return (
        spark.readStream.table(f"{catalog}.staging.guest_events")
        .filter(F.col("event_type") == "guest_profile")
        .select(
            F.col("guest_profile_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("first_name"),
            F.col("last_name"),
            F.col("email"),
            F.col("phone"),
            F.col("zip_code"),
            F.col("created_date"),
            F.col("account_status"),
            F.col("event_ts").alias("created_at"),
        )
    )


dp.create_streaming_table(
    name="guest_profile",
    comment="Customer profile record created at loyalty enrollment or first online order.",
    schema="""
        guest_profile_id BIGINT  COMMENT 'Surrogate primary key for the guest profile.',
        unit_id          BIGINT,
        first_name       STRING,
        last_name        STRING,
        email            STRING,
        phone            STRING,
        zip_code         STRING,
        created_date     STRING,
        account_status   STRING  COMMENT 'Profile state: active, inactive, suspended.',
        created_at       TIMESTAMP,
        CONSTRAINT pk_guest_profile PRIMARY KEY (guest_profile_id) NOT ENFORCED
    """,
)

dp.create_auto_cdc_flow(
    target="guest_profile",
    source="guest_profile_changes",
    keys=["guest_profile_id"],
    sequence_by=F.col("created_at"),
    stored_as_scd_type=1,
)
```

**What changed vs the old code:**
- Old: `@dp.table` with `readStream` → appends every event (same guest can have multiple rows)
- New: `@dp.view` feeds a CDC flow that upserts on `guest_profile_id` — latest `account_status` wins
- `created_at` now holds the original event timestamp (`event_ts`) rather than pipeline processing time

- [ ] **Step 1.2: Run the test suite to verify no regressions**

```bash
cd /Users/jesus.rodriguez/Documents/ItsAVibe/gitrepos_FY27/synthData
pytest tests/ -v
```

Expected: all 71 tests pass. None of the existing tests cover `mvm_pipeline.py` directly, so this is a regression check only.

- [ ] **Step 1.3: Commit**

```bash
git add src/pipeline/mvm_pipeline.py
git commit -m "feat: migrate silver.guest_profile to CDC merge (SCD Type 1) to eliminate guest_profile_id duplicates"
```

---

## Task 2: Add `start_dt_override` variable to bundle config

**Files:**
- Modify: `databricks.yml`
- Modify: `resources/setup_job.yml`

- [ ] **Step 2.1: Add `start_dt_override` to `databricks.yml`**

In `databricks.yml`, add after the `base_orders_per_unit_per_hour` variable block:

```yaml
  start_dt_override:
    default: ""
    description: "Backfill window start (ISO datetime, e.g. 2026-05-19T00:00:00). Empty string = use staging MAX(event_ts) or backfill_months default."
```

Full `variables` section after the edit:

```yaml
variables:
  catalog_name:
    default: jmrdemo
  num_units:
    default: "250"
  backfill_months:
    default: "1"
  live_tick_seconds:
    default: "60"
    description: "Sub-tick granularity (seconds) within each hourly live run. 60 = one sub-tick per minute, matching per-minute historical cadence."
  base_orders_per_unit_per_hour:
    default: "18"
  start_dt_override:
    default: ""
    description: "Backfill window start (ISO datetime, e.g. 2026-05-19T00:00:00). Empty string = use staging MAX(event_ts) or backfill_months default."
```

- [ ] **Step 2.2: Wire `start_dt_override` into the backfill task in `setup_job.yml`**

In `resources/setup_job.yml`, find the `backfill` task's `base_parameters` block and add `start_dt_override`:

```yaml
        - task_key: backfill
          depends_on:
            - task_key: setup
          environment_key: generator
          notebook_task:
            notebook_path: ../src/generator/main.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              num_units: ${var.num_units}
              backfill_months: ${var.backfill_months}
              live_tick_seconds: ${var.live_tick_seconds}
              base_orders_per_unit_per_hour: ${var.base_orders_per_unit_per_hour}
              start_dt_override: ${var.start_dt_override}
              mode: backfill
```

- [ ] **Step 2.3: Commit**

```bash
git add databricks.yml resources/setup_job.yml
git commit -m "feat: expose start_dt_override bundle variable to scope setup backfill window"
```

---

## Task 3: Set 1-day start, deploy, run setup job

- [ ] **Step 3.1: Set `start_dt_override` to yesterday's date for the 1-day test**

In `databricks.yml`, change the `start_dt_override` default to yesterday:

```yaml
  start_dt_override:
    default: "2026-05-19T00:00:00"
```

(This scopes the backfill to ~44 hours instead of 30 days — fast enough to test destroy.)

- [ ] **Step 3.2: Deploy the bundle**

```bash
databricks bundle deploy --target dev -p DEFAULT
```

Expected: `Deployment complete!`

- [ ] **Step 3.3: Run the setup job**

```bash
databricks jobs list -p DEFAULT --output json | python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
for j in jobs:
    name = j['settings']['name']
    if 'Setup' in name:
        print(j['job_id'], name)
"
```

Note the setup job ID, then run it:

```bash
databricks jobs run-now <SETUP_JOB_ID> -p DEFAULT
```

Expected: job starts. The backfill task will generate ~44 hours of data (2026-05-19 00:00 to now), not 30 days — should complete in under 15 minutes.

- [ ] **Step 3.4: Wait for setup job to complete**

```bash
databricks jobs list-runs --job-id <SETUP_JOB_ID> --output json -p DEFAULT | python3 -c "
import sys, json
runs = json.load(sys.stdin).get('runs', [])
for r in runs[:3]:
    lc = r['state']['life_cycle_state']
    rs = r['state'].get('result_state', '')
    print(r['run_id'], lc, rs)
"
```

Re-run until latest run shows `TERMINATED SUCCESS`.

- [ ] **Step 3.5: Verify silver data and CDC dedup**

Open a Databricks SQL session or notebook and run:

```sql
-- 1. Confirm silver.guest_profile is populated
SELECT COUNT(*) AS total_rows FROM jmrdemo.silver.guest_profile;
-- Expected: > 0 (some guests generated in the 1-day window)

-- 2. Verify NO duplicate guest_profile_id rows (the fix)
SELECT guest_profile_id, COUNT(*) AS n
FROM jmrdemo.silver.guest_profile
GROUP BY guest_profile_id
HAVING COUNT(*) > 1;
-- Expected: 0 rows (CDC merge deduped by guest_profile_id)

-- 3. Spot-check account_status has latest value (churn events should show inactive/churned)
SELECT account_status, COUNT(*) FROM jmrdemo.silver.guest_profile GROUP BY 1;
-- Expected: mix of 'active' and 'inactive' (churn events updating status)

-- 4. Confirm other silver tables are also populated
SELECT COUNT(*) FROM jmrdemo.silver.guest_order;
SELECT COUNT(*) FROM jmrdemo.silver.waste_log;
```

---

## Task 4: Run the destroy process and verify staging survives

- [ ] **Step 4.1: Record staging row counts before destroy**

```sql
SELECT 'order_events'     AS tbl, COUNT(*) AS rows FROM jmrdemo.staging.order_events
UNION ALL
SELECT 'inventory_events', COUNT(*) FROM jmrdemo.staging.inventory_events
UNION ALL
SELECT 'guest_events',     COUNT(*) FROM jmrdemo.staging.guest_events
UNION ALL
SELECT 'loyalty_events',   COUNT(*) FROM jmrdemo.staging.loyalty_events
UNION ALL
SELECT 'workforce_events', COUNT(*) FROM jmrdemo.staging.workforce_events;
```

Save these numbers — you'll verify they match after destroy.

- [ ] **Step 4.2: Run the QSR Destroy job (tears down non-DAB objects)**

```bash
databricks jobs list -p DEFAULT --output json | python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
for j in jobs:
    name = j['settings']['name']
    if 'Destroy' in name:
        print(j['job_id'], name)
"
```

```bash
databricks jobs run-now <DESTROY_JOB_ID> -p DEFAULT
```

Wait for `TERMINATED SUCCESS`. This drops metric views, Genie Space, and ref tables/schemas.

- [ ] **Step 4.3: Destroy DAB-managed resources**

```bash
databricks bundle destroy --target dev -p DEFAULT
```

When prompted `This will permanently destroy ...`, type `yes`.

Expected: removes the pipeline definition, job definitions. Does NOT touch UC tables.

- [ ] **Step 4.4: Verify staging tables survived**

```sql
-- Re-run the same query from Step 4.1
SELECT 'order_events'     AS tbl, COUNT(*) AS rows FROM jmrdemo.staging.order_events
UNION ALL
SELECT 'inventory_events', COUNT(*) FROM jmrdemo.staging.inventory_events
UNION ALL
SELECT 'guest_events',     COUNT(*) FROM jmrdemo.staging.guest_events
UNION ALL
SELECT 'loyalty_events',   COUNT(*) FROM jmrdemo.staging.loyalty_events
UNION ALL
SELECT 'workforce_events', COUNT(*) FROM jmrdemo.staging.workforce_events;
```

Expected: row counts match Step 4.1 exactly.

- [ ] **Step 4.5: Verify silver, gold, ref, and metrics are gone**

```sql
SHOW TABLES IN jmrdemo.silver;
-- Expected: empty (DLT pipeline removed these on bundle destroy)

SHOW TABLES IN jmrdemo.ref;
-- Expected: empty (destroy job dropped ref schema)

SHOW SCHEMAS IN jmrdemo;
-- Expected: only 'staging' remains (silver/gold/ref/metrics all dropped)
```

---

## Task 5: Reset `start_dt_override` to empty and commit

- [ ] **Step 5.1: Reset `start_dt_override` back to empty string in `databricks.yml`**

```yaml
  start_dt_override:
    default: ""
    description: "Backfill window start (ISO datetime, e.g. 2026-05-19T00:00:00). Empty string = use staging MAX(event_ts) or backfill_months default."
```

- [ ] **Step 5.2: Commit**

```bash
git add databricks.yml
git commit -m "chore: reset start_dt_override to empty (was set to 2026-05-19 for destroy test)"
```

---

## Self-Review

**Spec coverage:**
- ✅ CDC fix for silver.guest_profile — Task 1 (migrate to `dp.create_auto_cdc_flow`)
- ✅ Dedup verified — Task 3 Step 3.5 (SQL query with HAVING COUNT(*) > 1 expected empty)
- ✅ 1-day backfill — Task 3 Step 3.1 (start_dt_override = 2026-05-19)
- ✅ start_dt_override wired through bundle — Task 2
- ✅ Destroy process tested — Task 4
- ✅ Staging survival verified — Task 4 Steps 4.1 + 4.4 (before/after row count comparison)
- ✅ Default restored — Task 5

**Placeholder scan:** None. All steps include exact code, exact commands, and expected output.

**Type consistency:**
- `guest_profile_changes` used as both the `@dp.view` name and `source=` in `create_auto_cdc_flow` — consistent
- `F.col("created_at")` in `sequence_by` matches the `.alias("created_at")` in the view select — consistent
- `stored_as_scd_type=1` (int) used per the Databricks Python API examples — consistent
