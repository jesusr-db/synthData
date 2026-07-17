# Handoff for Next Agent — synthData

**As of**: 2026-07-17 17:05 EDT
**Branch / HEAD**: `fix/otel-incremental-refresh-frozen` @ `14b0390 fix(otel-orders): incremental refresh filtered on unresolvable SELECT alias` — **NOT pushed** (7 local commits incl. the 6 from `feat/otel-live-order-bolt-on`, no upstream)

> Single canonical handoff. Detail lives behind §4 pointers, not inlined.

## §1 — Launchpad (act on this first)   [HARD CAP: ≤10 lines total]

- **State**: clean working tree · branch `fix/otel-incremental-refresh-frozen` **not pushed** (7 commits) · frozen-refresh bug FIXED + deployed + verified end-to-end this session
- **Next actions** (≤3, each specific and startable):
  1. **Push branch + open PR** (7 local commits). Tip: run `/review` (Isaac Review) first — these changes haven't been reviewed this session.
  2. **Consider the two-clock fix** (§3 gotcha, still open): `mvm_pipeline` is only triggered hourly by `generator_job`, so staging (2-min) → silver freshness is ~hourly. For a live demo, give the otel job a `trigger_pipeline` task or a ~2-min pipeline trigger. Not a bug — a demo-latency choice.
  3. Optional: the notebook body `try/except → exit 0` still masks silent failures. Consider narrowing it or emitting a row-count assertion so a future zero-append run fails loudly instead of green.
- **Landmines** (≤2): The otel refresh job's whole body is wrapped in `try/except → [WARN] → exit 0`, so **"SUCCESS" ≠ "appended data"** — always verify by `MAX(event_ts) WHERE source='otel'` moving, not job status. · `staging.order_events` is a DLT streaming source — any fix MUST stay append-only (never MERGE/UPDATE/DELETE).

## §2 — This session   [HARD CAP: ≤5 bullets, each with evidence]

- **FIXED the frozen incremental otel refresh** (root cause = SELECT-alias in WHERE, NOT either handoff suspect) — evidence: commit `14b0390`; live repro showed buggy filter → `UNRESOLVED_COLUMN`, fixed filter → 2,829 rows; **230 tests pass** (red-green verified).
- **Root cause**: incremental filter was `AND event_ts > TIMESTAMP …` but `event_ts` is a SELECT alias (base col is `time_unix_nano`); SQL can't reference an alias in WHERE → error caught by step-3 try/except → `log_rows=[]` → `SystemExit(0)` → green, zero appends. Backfill mode (no ts_filter) worked, hiding it. Fix extracts `otel_logs_time_expr()`+`build_ts_filter()` into the adapter as single source of truth.
- **Deployed + verified end-to-end in prod**: `bundle deploy` → incremental run → HWM moved `07-15 16:05` → **`07-17 15:04`** (staging otel rows 78,239 → 103,442) → `mvm_pipeline` COMPLETED → `order_reconciliation` for `4e672275` = **reconciled:true, amount_diff:$0.00, member 19559, gold, 51 orders/$2998**.
- (Prior branch context) OTel bolt-on adapter + best-effort notebook + reconciliation view + member_id capture + Genie wiring — commits `b97d2dc`..`df5097a`; original backfill appended 9,284 orders to silver.

## §3 — Gotchas this session   [HARD CAP: ≤5]

- **Never filter on a SELECT alias in `WHERE`** — this codebase has now been bitten twice (frozen-refresh here; earlier the staging-schema fix). `otel_logs` has no `event_ts` column; filter on the raw `time_unix_nano` expression via `build_ts_filter()` — (durable → memory: `otel-live-order-bolt-on`).
- `jmrdemo.zerobus.otel_logs` is a rolling/TTL window but **wider than feared**: oldest order row is `2026-06-13` (~34-day window, 7,470 order rows on 07-17). Incremental keeping up matters, but there is no hours-scale data-loss urgency — (durable → memory: `otel-live-order-bolt-on`).
- Two independent clocks to "reconciled in silver": otel refresh (2-min, staging append) AND `mvm_pipeline` (hourly, staging→silver via `generator_job` `trigger_pipeline` — pipeline is `continuous:false`, no own schedule). 2-min staging cadence does NOT mean 2-min silver freshness — (durable → memory: `otel-live-order-bolt-on`).
- `app.order.member_id` injection has SHIPPED on the storefront (order `4e672275` carries `member_id=19559`) — the demo customer-reconciliation path is now real, not hypothetical — (session-local).

## §4 — Pointers

- Plan: `docs/superpowers/plans/2026-07-14-otel-live-order-bolt-on.md` · roadmap Phase 5: `docs/roadmap.md`
- Key files: `src/refresh/otel_order_adapter.py` (pure reshape + `otel_logs_time_expr`/`build_ts_filter` helpers), `src/refresh/otel_refresh_notebook.py` (IO+HWM+append — frozen-refresh bug FIXED `14b0390`), `src/setup/create_metric_views.py` (`order_reconciliation` view), `genie_domains/_spaces.py` + `01_grounding.sql`
- Memory entries added/updated this session: `otel-live-order-bolt-on` (project)
- **Carried-forward open issues** (from prior handoff, still unresolved — agent workstream, dormant this session):
  1. **App-migration LOE** (commerce agent Model Serving → Databricks App; verify feature-store + recommender under App SP) — not started; detail `git show 4880670:docs/handoff.md`.
  2. **`propose_order` menu-id ↔ storefront catalog mismatch** — open; `docs/roadmap.md` → "Commerce Agent — Known Issues".
  3. **Data classification auto-tagging not active** — `configure_monitoring.py` sets `enabled=True` but tier returns `False`; `class.*` tags applied deterministically by `apply_governance.py` Step 3.
  4. **In-UI real-time trace view** — needs `databricks-agents>=1.2.0` + GenAI-monitoring beta; traces already API/inference-table queryable.
  - Agent-workstream landmines still live: trace PAT is owner-minted 90-day (rotate/SP for prod); re-test agent via one-off `build_commerce_agent`, NOT full `setup_job`; don't delete `jmr_gateway` in destroy (shared).
