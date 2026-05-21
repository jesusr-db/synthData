# Hourly Live Generator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the live generator once per hour, producing 60 sub-ticks of events with correct per-minute timestamps spread across the previous hour — identical data characteristics to today's per-minute schedule at 60x lower job overhead.

**Architecture:** Add an `end_dt` stop parameter to `backfill_ticks()` so it can be bounded to a single-hour window. Reuse that function directly in the live branch of `main.py` — the live branch becomes "backfill the previous hour." Change the cron from every-minute to top-of-every-hour. `live_tick_seconds` variable stays `60` (it now means sub-tick granularity within the hour, not the schedule interval).

**Tech Stack:** Python, Databricks Asset Bundles, Quartz cron, `dateutil.relativedelta`, existing `backfill_ticks()` iterator in `src/generator/runner.py`.

---

## File Map

| File | Change |
|------|--------|
| `src/generator/runner.py` | Add `end_dt: datetime \| None = None` to `backfill_ticks()`; change loop condition to stop at `end_dt` |
| `src/generator/main.py` | Replace live branch `live_tick()` call with bounded `backfill_ticks()` call over previous 1-hour window |
| `resources/generator_job.yml` | Change `quartz_cron_expression` from `0 * * * * ?` to `0 0 * * * ?` |
| `databricks.yml` | No value change; update inline comment on `live_tick_seconds` |
| `tests/test_runner.py` | Add tests: `end_dt` boundary, hourly window timestamp spread, volume equivalence |

---

## Task 1: Add `end_dt` parameter to `backfill_ticks()`

**Files:**
- Modify: `src/generator/runner.py:39-71`
- Test: `tests/test_runner.py`

- [ ] **Step 1.1: Write the failing test for `end_dt` boundary**

Add to `tests/test_runner.py`:

```python
def test_backfill_ticks_respects_end_dt():
    """backfill_ticks must not yield batches at or after end_dt."""
    from datetime import datetime, timedelta
    from src.generator.runner import backfill_ticks
    from src.generator.entity_registry import EntityRegistry
    from src.generator.reference.us_locations import generate_units
    from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
    from src.generator.reference.seeder import build_financial_periods_data

    reg = EntityRegistry(
        units=generate_units(2),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(1),
    )
    start = datetime(2025, 9, 19, 12, 0)   # noon
    end   = datetime(2025, 9, 19, 13, 0)   # 1pm — exclusive
    batches = list(backfill_ticks(reg, backfill_months=1, tick_seconds=3600,
                                  base_orders_per_hour=18, start_dt=start, end_dt=end))
    # With tick_seconds=3600, only one tick (12:00) should be yielded; 13:00 is end_dt (exclusive)
    assert len(batches) == 1


def test_backfill_ticks_end_dt_60s_ticks_yields_60_batches():
    """60 one-minute ticks from 12:00 to 13:00 (exclusive) yields exactly 60 batches."""
    from datetime import datetime, timedelta
    from src.generator.runner import backfill_ticks
    from src.generator.entity_registry import EntityRegistry
    from src.generator.reference.us_locations import generate_units
    from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
    from src.generator.reference.seeder import build_financial_periods_data

    reg = EntityRegistry(
        units=generate_units(1),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(1),
    )
    start = datetime(2025, 9, 19, 12, 0)
    end   = datetime(2025, 9, 19, 13, 0)
    batches = list(backfill_ticks(reg, backfill_months=1, tick_seconds=60,
                                  base_orders_per_hour=18, start_dt=start, end_dt=end))
    assert len(batches) == 60
```

- [ ] **Step 1.2: Run to verify tests fail**

```bash
cd /Users/jesus.rodriguez/Documents/ItsAVibe/gitrepos_FY27/synthData
pytest tests/test_runner.py::test_backfill_ticks_respects_end_dt tests/test_runner.py::test_backfill_ticks_end_dt_60s_ticks_yields_60_batches -v
```

Expected: `TypeError` — `backfill_ticks()` got unexpected keyword argument `end_dt`.

- [ ] **Step 1.3: Add `end_dt` parameter to `backfill_ticks()`**

In `src/generator/runner.py`, replace the `backfill_ticks` signature and loop condition:

```python
def backfill_ticks(
    registry: EntityRegistry,
    backfill_months: int,
    tick_seconds: int = 3600,
    base_orders_per_hour: int = 18,
    start_dt: datetime | None = None,
    end_dt: datetime | None = None,
) -> Iterator[list[dict]]:
    """Yield batches of rows for all units, one tick at a time, from start_dt up to (but not including) end_dt."""
    from dateutil.relativedelta import relativedelta

    now = datetime.now().replace(minute=0, second=0, microsecond=0)
    start = start_dt if start_dt is not None else now - relativedelta(months=backfill_months)
    end   = end_dt   if end_dt   is not None else now
    current = start
    while current < end:
        batch = []
        for unit in registry.all_units():
            uid = unit["unit_id"]
            batch.extend(build_tick_rows(uid, current, registry, tick_seconds, base_orders_per_hour))
            if current.hour == 10:
                batch.extend(
                    generate_shift_events(
                        uid,
                        current.date().isoformat(),
                        projected_orders=base_orders_per_hour * 12,
                        tick_ts=current,
                    )
                )
                batch.extend(generate_new_guest_profiles(uid, current.date().isoformat(), tick_ts=current))
                batch.extend(generate_guest_churn(uid, registry, current.date().isoformat(), tick_ts=current))
                batch.extend(generate_daily_receiving(uid, registry, current.date().isoformat(), tick_ts=current))
        yield batch
        current += timedelta(seconds=tick_seconds)
```

Key change: `while current <= now:` → `while current < end:`, and `now` replaced by `end`.

- [ ] **Step 1.4: Run tests to verify they pass**

```bash
pytest tests/test_runner.py -v
```

Expected: all tests in `test_runner.py` pass including the two new ones.

- [ ] **Step 1.5: Commit**

```bash
git add src/generator/runner.py tests/test_runner.py
git commit -m "feat: add end_dt parameter to backfill_ticks for bounded windows"
```

---

## Task 2: Replace live branch with bounded `backfill_ticks()` call

**Files:**
- Modify: `src/generator/main.py:130-135`
- Test: `tests/test_runner.py`

- [ ] **Step 2.1: Write a failing test for the hourly-window timestamp spread**

Add to `tests/test_runner.py`:

```python
def test_hourly_window_timestamps_span_full_hour():
    """60 sub-ticks over [start, start+1h) should produce events at 60 distinct minute marks."""
    from datetime import datetime, timedelta
    from src.generator.runner import backfill_ticks
    from src.generator.entity_registry import EntityRegistry
    from src.generator.reference.us_locations import generate_units
    from src.generator.reference.menu_catalog import get_menu_items, get_recipe_ingredients
    from src.generator.reference.seeder import build_financial_periods_data

    reg = EntityRegistry(
        units=generate_units(3),
        menu_items=get_menu_items(),
        bom=get_recipe_ingredients(),
        financial_periods=build_financial_periods_data(1),
    )
    # Use dinner hour for reliable order volume
    start = datetime(2025, 9, 19, 19, 0)
    end   = datetime(2025, 9, 19, 20, 0)
    all_rows = []
    for batch in backfill_ticks(reg, backfill_months=1, tick_seconds=60,
                                 base_orders_per_hour=18, start_dt=start, end_dt=end):
        all_rows.extend(batch)

    # All event timestamps must fall within [start, end)
    order_rows = [r for r in all_rows if r.get("event_type") == "guest_order"]
    assert len(order_rows) > 0, "Expected orders in a busy dinner hour"

    timestamps = {r["placed_at"] for r in order_rows if r.get("placed_at") is not None}
    min_ts = min(timestamps)
    max_ts = max(timestamps)
    assert min_ts >= start, f"Event before window start: {min_ts}"
    assert max_ts < end,    f"Event at or after window end: {max_ts}"

    # Events should span at least 30 distinct minutes (probabilistic — dinner hour, 3 units)
    distinct_minutes = {ts.replace(second=0, microsecond=0) for ts in timestamps}
    assert len(distinct_minutes) >= 30, f"Only {len(distinct_minutes)} distinct minutes — timestamps not spread"
```

- [ ] **Step 2.2: Run test to verify it passes already (it tests `backfill_ticks` which is already fixed)**

```bash
pytest tests/test_runner.py::test_hourly_window_timestamps_span_full_hour -v
```

Expected: PASS — this tests the runner directly, not `main.py`. If it fails, debug `build_tick_rows` timestamp stamping before continuing.

- [ ] **Step 2.3: Update the live branch in `main.py`**

In `src/generator/main.py`, replace lines 130-135 (the `else` / live branch):

```python
else:
    # Live mode: generate the previous hour as 60 sub-ticks with correct per-minute timestamps.
    # Runs once per hour (scheduled via cron). live_tick_seconds controls sub-tick granularity.
    from datetime import timedelta
    end_dt   = datetime.now().replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(hours=1)
    print(f"[INFO] Live tick: window=[{start_dt}, {end_dt}), sub_tick_seconds={live_tick_seconds}, catalog={catalog_name}")
    total_rows = 0
    for batch in backfill_ticks(registry, backfill_months=1, tick_seconds=live_tick_seconds,
                                 base_orders_per_hour=base_orders, start_dt=start_dt, end_dt=end_dt):
        write_batch(batch)
        total_rows += len(batch)
    print(f"[INFO] Live tick complete: {total_rows} rows written for window [{start_dt}, {end_dt})")
```

Also update the import at the top of the file — `backfill_ticks` is already imported (line 31: `from src.generator.runner import backfill_ticks, live_tick, GeneratorConfig`). The `datetime` import is already present via `_latest_staging_ts`. Verify `datetime` is imported at module level by checking line 1-10; if not, add:

```python
from datetime import datetime, timedelta
```

near the other imports (after the sys.path block).

- [ ] **Step 2.4: Run the full test suite to confirm no regressions**

```bash
pytest tests/ -v
```

Expected: all tests pass. The existing `test_live_tick_returns_list_of_dicts` still passes because `live_tick` still exists in `runner.py` — it just isn't called by `main.py` anymore.

- [ ] **Step 2.5: Commit**

```bash
git add src/generator/main.py tests/test_runner.py
git commit -m "feat: live mode uses backfill_ticks over previous 1-hour window"
```

---

## Task 3: Update DAB config — hourly cron, comment on `live_tick_seconds`

**Files:**
- Modify: `resources/generator_job.yml`
- Modify: `databricks.yml`

- [ ] **Step 3.1: Change the cron expression in `generator_job.yml`**

In `resources/generator_job.yml`, change:

```yaml
      schedule:
        quartz_cron_expression: "0 * * * * ?"
```

to:

```yaml
      schedule:
        quartz_cron_expression: "0 0 * * * ?"
```

`0 0 * * * ?` fires at second=0, minute=0 of every hour (top of hour). `0 * * * * ?` fired every minute.

- [ ] **Step 3.2: Update the `live_tick_seconds` comment in `databricks.yml`**

In `databricks.yml`, the `live_tick_seconds` variable currently has no description. Add one to clarify its meaning has changed:

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
```

- [ ] **Step 3.3: Deploy to dev and confirm the job schedule updated**

```bash
databricks bundle deploy --target dev -p DEFAULT
```

Expected output ends with `Deployment complete!`.

Then verify the job schedule in the workspace:

```bash
databricks jobs get $(databricks jobs list --output json -p DEFAULT | python3 -c "import sys,json; jobs=json.load(sys.stdin)['jobs']; print(next(j['job_id'] for j in jobs if 'Generator Live' in j['settings']['name']))" ) --output json -p DEFAULT | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['settings']['schedule']['quartz_cron_expression'])"
```

Expected: `0 0 * * * ?`

- [ ] **Step 3.4: Commit**

```bash
git add resources/generator_job.yml databricks.yml
git commit -m "feat: generator runs hourly at top of hour; live_tick_seconds=60 is sub-tick granularity"
```

---

## Task 4: Smoke-verify data from the next scheduled run

- [ ] **Step 4.1: Wait for the next top-of-hour run to complete**

The job fires at the top of every hour. Monitor until the next run completes:

```bash
databricks jobs list-runs --job-id <GENERATOR_JOB_ID> --output json -p DEFAULT | python3 -c "
import sys, json
runs = json.load(sys.stdin)['runs']
for r in runs[:3]:
    print(r['run_id'], r['state']['life_cycle_state'], r.get('state', {}).get('result_state', ''))
"
```

Replace `<GENERATOR_JOB_ID>` with the ID from `databricks jobs list`. Wait until a run shows `TERMINATED` + `SUCCESS`.

- [ ] **Step 4.2: Query silver order volume for the run's hour**

Use the Databricks SQL query skill or run via SDK. Replace `<YYYY-MM-DD HH>` with the hour that was just generated (the hour *before* the run time, since the job generates the previous hour):

```sql
SELECT
    DATE_TRUNC('MINUTE', placed_at) AS minute_bucket,
    COUNT(*) AS order_count
FROM jmrdemo.silver.guest_order
WHERE placed_at >= '<YYYY-MM-DD HH>:00:00'
  AND placed_at <  '<YYYY-MM-DD HH+1>:00:00'
GROUP BY 1
ORDER BY 1;
```

Expected:
- ~60 distinct `minute_bucket` rows (one per minute sub-tick)
- Each bucket has > 0 orders for at least ~80% of minutes (Poisson at dinner hour with 250 units → virtually guaranteed)
- Total orders for the hour ≈ `250 × 18 × HOURLY_MULTIPLIER[hour] × DOW_MULTIPLIER[weekday]`

Example for noon on a Monday: `250 × 18 × 0.80 × 1.0 = 3,600` expected orders ±15% noise.

- [ ] **Step 4.3: Confirm no duplicate minute buckets from double-runs**

```sql
SELECT
    DATE_TRUNC('MINUTE', placed_at) AS minute_bucket,
    COUNT(*) AS n
FROM jmrdemo.silver.guest_order
WHERE placed_at >= '<YYYY-MM-DD HH>:00:00'
  AND placed_at <  '<YYYY-MM-DD HH+1>:00:00'
GROUP BY 1
HAVING COUNT(*) > 500   -- spike threshold; tune to ~2× expected per-minute count
ORDER BY n DESC
LIMIT 10;
```

Expected: 0 rows (no minute buckets with suspiciously high counts indicating double-writes). If rows appear, the DLT streaming pipeline is deduplicating on the PK — verify via `information_schema.table_constraints`.

---

## Self-Review

**Spec coverage:**
- ✅ Run generator once per hour (Task 3 — cron change)
- ✅ Data backfilled with proper timestamps (Task 2 — `backfill_ticks` with 1h window)
- ✅ Correct per-minute distribution (Task 1 — 60 sub-ticks × `tick_seconds=60`)
- ✅ `HOURLY_MULTIPLIERS` / `DOW_MULTIPLIERS` applied correctly (inherited from `backfill_ticks`)
- ✅ Tests verifying timestamp spread (Task 2, Step 2.1)
- ✅ Deploy and smoke-verify (Task 3 + Task 4)

**Placeholder scan:** None found. All steps include exact code and exact commands.

**Type consistency:** `backfill_ticks` signature in Task 1 matches the call in Task 2. `end_dt` parameter name used consistently. `datetime` imported from standard library throughout.
