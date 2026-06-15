# Brainstorm: Hourly Generator with Proper Timestamp Backfill

## Framing

The user currently runs a synthetic data generator job every minute. Each minute, `live_tick(registry, tick_seconds=60, base_orders)` produces ~1/60th of an hour's worth of events, but stamps every event in that batch with a single `datetime.now()`. The realism comes from running 60 times per hour, so timestamps naturally spread across the hour (one cluster per minute) and `HOURLY_MULTIPLIERS` / `DOW_MULTIPLIERS` apply per-tick.

The goal: reduce the schedule from every minute to every hour while preserving the temporal characteristics of the data — i.e., events spread realistically across the hour, hourly demand multipliers respected, and downstream consumers (DLT pipelines, dashboards) seeing a stream that's indistinguishable from the current minute-level cadence.

The core technical issue is that flipping `live_tick_seconds` from `60` to `3600` would generate the correct *volume* of events for one hour, but `live_tick` collapses all event timestamps to a single `datetime.now()`. That violates the synthetic-data realism requirement — we'd get hourly "spikes" of identical timestamps instead of a smooth stream.

Constraints:
- Must stay fully DAB-automatable (databricks.yml + generator_job.yml).
- Must not break the existing `backfill` mode, which already handles multi-tick iteration correctly.
- Must respect the demand-shaping model (`HOURLY_MULTIPLIERS`, `DOW_MULTIPLIERS`) — these are applied per-tick based on the tick's hour.
- The fix should be minimal-surface and avoid regressions on the recently-fixed widget loading and staging-schema work.
- Downstream DLT/streaming consumers should see timestamps that look continuous, not hourly-bursty.

## Assumptions

1. **"Backfilled with proper timestamps" means intra-hour spreading**, not historical backfill. The user wants the hourly run to retroactively produce events for the last 60 minutes with realistically distributed timestamps, not to backfill prior days.
2. **The hourly run should cover the hour that just elapsed** (e.g., job runs at 14:00 and produces events timestamped between 13:00 and 13:59:59).
3. **Per-minute granularity is sufficient.** We do not need sub-minute jitter; spreading events into 60 one-minute sub-ticks matches the current behavior exactly.
4. **No state needs to persist between runs.** Because we're always generating the previous hour, the job is idempotent on schedule.
5. **The `base_orders` parameter is per-hour-equivalent**, scaled by `tick_fraction = tick_seconds / 3600`.
6. **The user wants a single canonical solution**, not a feature flag for both behaviors. Hourly is the new normal; minute scheduling is being retired.
7. **Branch policy applies**: this touches multiple files, so a feature branch is required.

## Perspectives

**Cost / ops:** Going from 1440 runs/day to 24 runs/day is a 60x reduction in job startup overhead, cluster spin-up cost, and noise in job history / lineage.

**Data-realism:** Downstream consumers have been calibrated against minute-level cadence. Any hourly solution must produce timestamps that, when aggregated to 1-minute or 5-minute buckets, look statistically identical to today's stream. Bursty "all-at-the-top-of-the-hour" timestamps would break dashboards showing "events per minute" trends.

**Code-locality:** The cleanest fix lives entirely inside the generator. No schema changes, no DLT changes, no DAB resource topology changes beyond the cron and one variable.

**Reuse:** `backfill_ticks()` already iterates hour-by-hour with correct timestamps. The hourly-live mode is really just "backfill exactly the previous hour."

**Streaming-semantics:** Today, an event arrives at the bronze table within ~60 seconds of being "generated." After the change, an event for 13:05 will only land at ~14:00 — worst-case latency of ~60 minutes. For a synthetic demo dataset, this is acceptable.

## Options

### Option A — Run `backfill_ticks()` for a single-hour window in live mode ✅ Recommended

**Description.** Replace the live branch in `main.py` with a call to `backfill_ticks(registry, start_dt=floor_to_hour(now) - 1h, tick_seconds=60)`. The function already exists, already iterates per-tick with correct timestamps, and already applies hourly/DOW multipliers correctly. Schedule changes to `0 0 * * * ?` (top of every hour); `live_tick_seconds` is reinterpreted as the sub-tick granularity (default 60).

**Pros:** Maximum code reuse, zero risk of behavior drift between backfill and live, trivial to extend to finer sub-tick granularity later.

**Cons:** Semantically blurs "live" vs "backfill" distinction; naming may need updating.

**Fit:** Strong. Lowest-risk, highest-reuse path.

### Option B — Add a "spread" parameter to `live_tick`

**Description.** Extend `live_tick` with `spread_seconds=None`. When set, generates events as `spread_seconds / sub_tick_seconds` sub-ticks stamped across the spread window ending at `datetime.now()`.

**Pros:** Keeps live/backfill distinction clean. **Cons:** Duplicates logic that `backfill_ticks` already has. **Fit:** Moderate.

### Option C — Jitter timestamps inside a single `live_tick(tick_seconds=3600)` call

**Description.** Keep one `live_tick` call per hourly run; distribute event timestamps uniformly across the previous 60 minutes inside the function.

**Pros:** Smallest code diff. **Cons:** Semantic divergence from `backfill_ticks`, harder to test equivalence. **Fit:** Weak.

### Option D — Hourly job loops `live_tick` 60 times internally

**Description.** In `main.py`'s live branch, loop 60 times calling `live_tick` with an explicit `as_of` override per minute.

**Pros:** Zero changes to `live_tick`. **Cons:** Once `live_tick` accepts `as_of`, Option A is strictly better. **Fit:** Weak.

## Recommendation

**Go with Option A: reuse `backfill_ticks()` for the live branch, scheduled hourly.**

Implementation:

1. **`src/generator/main.py`** — In the live branch, compute `end_dt = now().replace(minute=0, second=0, microsecond=0)` and `start_dt = end_dt - timedelta(hours=1)`. Call `backfill_ticks(registry, backfill_months=1, tick_seconds=live_tick_seconds, base_orders=..., start_dt=start_dt)` with an explicit `end_dt` stop condition, instead of `live_tick(...)`. Keep `live_tick_seconds=60` as the sub-tick granularity.

2. **`resources/generator_job.yml`** — Change `quartz_cron_expression` from `0 * * * * ?` to `0 0 * * * ?`.

3. **`databricks.yml`** — Change `live_tick_seconds` default from `"60"` to `"60"` (unchanged semantically — it's now the sub-tick granularity within the hour).

4. **Tests** — Add a unit test verifying that live mode produces events spanning a full hour with distinct minute buckets.

5. **Branch** — `feat/hourly-live-generator`

**Top risks:**

1. **Latency expectation shift.** Events for minute :05 now land at the top of the next hour instead of within ~60 seconds. Confirm no downstream component has a near-real-time expectation.

2. **Idempotence on reruns / overlapping schedules.** Two batches for the same `[13:00, 14:00)` window could be written if the job is manually triggered while a scheduled run is also firing. The blast radius is now 60 minutes of duplicates (vs. 1 minute today). Rely on downstream MERGE/dedup logic, or add a lightweight check.
