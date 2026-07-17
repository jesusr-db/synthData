# Handoff for Next Agent — synthData

**As of**: 2026-07-17 15:20 EDT
**Branch / HEAD**: `feat/otel-live-order-bolt-on` @ `df5097a feat(genie): wire order_reconciliation into Orders & SOS + Guest 360 spaces` — **NOT pushed** (6 local commits, no upstream)

> Single canonical handoff. Detail lives behind §4 pointers, not inlined.

## §1 — Launchpad (act on this first)   [HARD CAP: ≤10 lines total]

- **State**: clean working tree · branch `feat/otel-live-order-bolt-on` **not pushed** (6 commits)
- **Next actions** (≤3, each specific and startable):
  1. **FIX the incremental otel refresh job (job id `82262113021893`).** It runs every 2 min and reports SUCCESS but has appended **nothing since 2026-07-15 16:05** — a ~2,819-order backlog (incl. order `4e672275…`) sits in `otel_logs` unreconciled. I was about to trigger a manual `mode=incremental` run to watch the HWM move (user interrupted). Prime suspects: the giant `trace_id IN (…)` list in the span query (`otel_refresh_notebook.py:117`) with ~2,819 traces, or the whole-body `try/except` (`:183`) swallowing the error as a green run.
  2. **Then** trigger `mvm_pipeline` (`c8995f2b-…`) so the backlog lands in silver; re-check `order_reconciliation` for `4e672275-81e2-11f1-a3d9-7e619b9a2692` → expect `reconciled=true`.
  3. Push branch + open PR (6 local commits). Tip: run `/review` (Isaac Review) first — these changes haven't been reviewed this session.
- **Landmines** (≤2): The otel refresh job's whole body is wrapped in `try/except → [WARN] → exit 0`, so **"SUCCESS" ≠ "appended data"** — always verify by `MAX(event_ts) WHERE source='otel'` moving, not job status. · `staging.order_events` is a DLT streaming source — any fix MUST stay append-only (never MERGE/UPDATE/DELETE).

## §2 — This session   [HARD CAP: ≤5 bullets, each with evidence]

- Built + deployed the **OTel live-order bolt-on** (adapter, best-effort notebook, DAB job, staging `source` col, backfill task) — evidence: commits `b97d2dc`,`6c7c960`,`cf164b0`; earlier this branch appended **9,284 real orders** to staging→silver (verified: 9,284 in `silver.guest_order`, `unit_price>0`).
- Added `synth_metrics.order_reconciliation` view (web UUID ↔ bridged synth `guest_order_id`) — evidence: `68ff88d`; live query showed **9,284 reconciled, amount_diff=$0.00**.
- Captured web-injected `app.order.member_id` → sets synth `member_id`+`profile_id` (range 1–50000), view joins `customer_features` — evidence: `7931691`; **227 tests pass**; live: member `19559`→gold/51 orders/$2998.
- Wired `order_reconciliation` into **Orders & SOS + Guest 360** Genie spaces w/ web-order-ID instructions — evidence: `df5097a`; `apply_grounding_sql`+`build_genie_spaces` both SUCCESS; serialized spaces verified to include the view.
- **Discovered live bug**: incremental otel refresh frozen — HWM stuck `2026-07-15 16:05`, 2,819-order backlog, order `4e672275` not in staging — evidence: SQL (`MAX(event_ts)`=07-15 16:05; backlog count=2,819; job runs 2-min all SUCCESS via `list-runs`).

## §3 — Gotchas this session   [HARD CAP: ≤5]

- Incremental otel refresh runs green but appends nothing for ~2 days (HWM frozen 07-15 16:05) — root cause not yet confirmed; suspects = oversized `trace_id IN()` span filter or body-level try/except masking failure — (session-local; diagnosis in §1 action 1).
- `jmrdemo.zerobus.otel_logs` is a **rolling/TTL window**: source now holds 7,458 order rows but staging has 9,284 — old orders age out before ingest, so "backfill once" is insufficient; incremental MUST keep up or orders are lost — (durable → memory: `otel-live-order-bolt-on`).
- Two independent clocks to "reconciled in silver": otel refresh (2-min, staging append) AND `mvm_pipeline` (hourly, staging→silver via `generator_job` `trigger_pipeline` — pipeline is `continuous:false`, no own schedule). 2-min staging cadence does NOT mean 2-min silver freshness — (durable → memory: `otel-live-order-bolt-on`).
- `app.order.member_id` injection has SHIPPED on the storefront (order `4e672275` carries `member_id=19559`) — the demo customer-reconciliation path is now real, not hypothetical — (session-local).

## §4 — Pointers

- Plan: `docs/superpowers/plans/2026-07-14-otel-live-order-bolt-on.md` · roadmap Phase 5: `docs/roadmap.md`
- Key files: `src/refresh/otel_order_adapter.py` (pure reshape), `src/refresh/otel_refresh_notebook.py` (IO+HWM+append — **bug lives here**), `src/setup/create_metric_views.py` (`order_reconciliation` view), `genie_domains/_spaces.py` + `01_grounding.sql`
- Memory entries added/updated this session: `otel-live-order-bolt-on` (project)
- **Carried-forward open issues** (from prior handoff, still unresolved — agent workstream, dormant this session):
  1. **App-migration LOE** (commerce agent Model Serving → Databricks App; verify feature-store + recommender under App SP) — not started; detail `git show 4880670:docs/handoff.md`.
  2. **`propose_order` menu-id ↔ storefront catalog mismatch** — open; `docs/roadmap.md` → "Commerce Agent — Known Issues".
  3. **Data classification auto-tagging not active** — `configure_monitoring.py` sets `enabled=True` but tier returns `False`; `class.*` tags applied deterministically by `apply_governance.py` Step 3.
  4. **In-UI real-time trace view** — needs `databricks-agents>=1.2.0` + GenAI-monitoring beta; traces already API/inference-table queryable.
  - Agent-workstream landmines still live: trace PAT is owner-minted 90-day (rotate/SP for prod); re-test agent via one-off `build_commerce_agent`, NOT full `setup_job`; don't delete `jmr_gateway` in destroy (shared).
