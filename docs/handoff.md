# Handoff for Next Agent — synthData

**As of**: 2026-06-18 22:40 EDT
**Branch / HEAD**: `feat/commerce-agent-plan` @ `f5e2720 fix(agent): constrain propose_order to the confirmed cart` — **NOT pushed** (15 commits ahead of `main`, no upstream)

> Single canonical handoff. Detail lives behind §4 pointers, not inlined.

## §1 — Launchpad (act on this first)

- **State**: clean re: source (only `.pyc` artifacts modified + 1 untracked `research/*.md`) · **not pushed**
- **Next actions**:
  1. Decide branch disposition: open PR / merge `feat/commerce-agent-plan` → `main`, or keep iterating (15 commits, the whole commerce-agent feature + live deploy).
  2. To make the live endpoint callable by the web SP: set `commerce_agent_query_principal` (currently `""` → no `CAN_QUERY` grant) and re-run `build_commerce_agent`.
  3. Optional follow-ups: enable in-serving MLflow tracing (§6 trace-stitch); wire live data for `get_order_history` + `get_occasion_context` (v1 returns `[]`).
- **Landmines**: branch NOT pushed (15 local commits — back up before any destructive git op). · To re-test the agent, do a one-off `jobs/runs/submit` of `build_commerce_agent` (notebook path: NO `.py` ext) — do NOT run the full `setup_job`, it regenerates all data.

## §2 — This session

- Built the PizzaTel commerce agent (Tasks 1–8, hermetic, TDD) via subagent-driven dev with per-task spec+quality reviews — evidence: commits `5eaef40`..`ddd3917`; 192 tests pass.
- Final adversarial whole-branch review (Opus) → fixed graceful tool-error handling, identity coercion, adapter-contract docs — evidence: `004508e`.
- Deployed `synth_qsr-commerce-agent` live to Model Serving (READY) and validated 3 real turns: propose_order ($37.35 cart), text-only, guest cold-start — evidence: this session's endpoint queries; build/deploy fixes `d9baeaf`,`225d96f`,`25e4bd1`,`0a9aa3f`.
- Fixed web-reported `propose_order` divergence (latched onto earlier-recommended item) by constraining the tool call via prompt — evidence: `f5e2720`; validated 5/5 (model) + 4/4 (web).
- Updated the web-team contract ledger to 🟩 (real envelope + examples, pricing authority, gateway/trace findings, bug-fix reply) — evidence: web repo commits `a12fc3c`,`3eeea73` on `feat/agentic-commerce-chatbot`.

## §3 — Gotchas this session

- Pay-per-token FMs can't be re-served in a new endpoint; enable AI Gateway in place via `PUT /ai-gateway` — (durable → memory: `ai-gateway-payg-fm`).
- MLflow ResponsesAgent log/serve cluster: output-item `id` required, `openai` dep + `load_context` runs at log time, `__getstate__` for picklability, ResponsesAgent injects a `status` field that breaks the chat call (whitelist keys) — (durable → memory: `mlflow-responsesagent-serving`).
- In-serving MLflow tracing is a no-op → `mlflow_trace_id` returns `MLFLOW_NO_OP_SPAN_TRACE_ID`; needs `ENABLE_MLFLOW_TRACING` + experiment + creds — (durable → memory: `mlflow-responsesagent-serving`).
- `propose_order` priced whatever ids the LLM emitted (no tie to agreed cart) → LLM latched onto in-context recommendations; fixed by prompt constraint + read-back — (session-local; fix `f5e2720`).

## §4 — Pointers

- Plan: `docs/superpowers/plans/2026-06-18-pizzatel-commerce-agent.md` · Agent API: `docs/api.md` (commerce-agent section) · Contract ledger (web repo): `gitRepos_FY26/opentelemetry-demo/docs/integration/agent-endpoint-contract.md`.
- Agent code: `src/agent/{pricing,tools,loop,commerce_agent,gateway,serving,prompts}.py`; setup: `src/setup/build_commerce_agent.py`; tests: `tests/test_agent_*.py`.
- **Legacy project reference** (full QSR-generator rebuild guide, governance/access model, data layers, resolved-gotchas catalog, verification SQL): `git show 0a9aa3f:docs/handoff.md` — this rewrite compressed that 343-line doc into the launchpad above; the detail is preserved in git history and partly in living-docs (`docs/architecture|dataflow|api`, noted stale since May 21).
- Memory entries added this session: `mlflow-responsesagent-serving`, `ai-gateway-payg-fm`.
- **Carried-forward open issues** (from prior handoff, still unresolved): Data classification auto-tagging not active — `configure_monitoring.py` passes `MonitorDataClassificationConfig(enabled=True)` but the workspace tier returns `enabled=False`; `class.*` PII tags are applied deterministically by `apply_governance.py` Step 3 (DDL), not by a scanner. Lights up automatically if the workspace tier is upgraded.
