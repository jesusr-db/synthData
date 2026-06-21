# Handoff for Next Agent — synthData

**As of**: 2026-06-21 18:10 EDT
**Branch / HEAD**: merging `feat/agent-tracing-inference-tables` → `main` (the full commerce-agent feature + tracing). **Not pushed** after merge — back up before destructive git ops.

> Single canonical handoff. Detail lives behind §4 pointers, not inlined.

## §1 — Launchpad (act on this first)

- **State**: clean re: source (only `.pyc` artifacts + 1 untracked `research/*.md`) · 192 tests pass · merged to `main`, not pushed.
- **Next actions**:
  1. **OPEN BUG (highest priority): `propose_order` menu-id mapping divergence.** Web (2026-06-21) found the agent's `menu_item_id` doesn't reliably match the storefront `ProductCatalog` ids beyond pepperoni(1)/cheese(2) — MeatZZa(4) returned `13`/`10` and once `2003` (outside catalog) in 4/5 runs. This is a DIFFERENT root cause than the earlier prompt fix (which stopped recommendation-latching). Investigate the agent's menu-id source (`ref.menu_item.menu_item_id` baked at log time) vs the storefront catalog ids.
  2. Write the §6 ledger clarification: usage tracking IS enabled on the LLM endpoint (`databricks-claude-sonnet-4-5`); it's only unavailable on the custom-pyfunc agent endpoint.
  3. To make the live endpoint callable by the web SP: set `commerce_agent_query_principal` and re-run `build_commerce_agent`.
- **Landmines**: trace PAT is owner-minted, 90-day (rotate / move to SP for prod). · Re-test the agent via a one-off `jobs/runs/submit` of `build_commerce_agent` (notebook path: NO `.py` ext) — NOT the full `setup_job` (regenerates all data).

## §2 — This session

- Enabled in-serving MLflow trace logging on `synth_qsr-commerce-agent` → real `mlflow_trace_id` (e.g. `tr-a9509b01…`), traces land in `/Shared/qsr-commerce-agent-traces` (exp `3025255582876496`) — evidence: commits `589950b`,`468cce3`; verified live + notebook rebuild SUCCESS.
- Enabled inference tables via AI Gateway `inference_table_config` → `jmrdemo.synth_silver.commerce_agent_payload_payload` (legacy `auto_capture_config` is deprecated/rejected) — evidence: live GET shows `inference_table_config.enabled=True`.
- Setup mints+stores a 90-day PAT (`qsr-synth` scope, revoke-then-mint) for the trace creds; destroy revokes the PAT + deletes experiment/secret/inference table — evidence: `589950b`.
- Ledger §3.2/§6 → 🟩; web confirmed trace-stitch live both directions (`agent.mlflow.trace_id` in zerobus) — evidence: web repo commit `3f4bbfb`.

## §3 — Gotchas this session

- Legacy `auto_capture_config` is deprecated ("can only be used with enabled=false") → inference tables must use AI Gateway `inference_table_config` — (durable → memory: `mlflow-responsesagent-serving`).
- AI Gateway **usage tracking is not supported on custom-pyfunc serving endpoints** in this workspace (rejected on the agent endpoint); it IS enabled on the FM endpoint `databricks-claude-sonnet-4-5` where token usage actually happens — (durable → memory: `ai-gateway-payg-fm`).
- Workspace-file experiments don't support real-time tracing → route traces to a dedicated `/Shared` experiment — (durable → memory: `mlflow-responsesagent-serving`).

## §4 — Pointers

- Plan: `docs/superpowers/plans/2026-06-18-pizzatel-commerce-agent.md` · Agent API: `docs/api.md` · Contract ledger (web repo): `gitRepos_FY26/opentelemetry-demo/docs/integration/agent-endpoint-contract.md` · Web journey results: `…/docs/journey-test-results-2026-06-21.md`.
- Agent code: `src/agent/{pricing,tools,loop,commerce_agent,gateway,serving,prompts}.py`; setup: `src/setup/build_commerce_agent.py`; teardown: `src/setup/destroy_notebook.py` (0h-1b); tests: `tests/test_agent_*.py`.
- Legacy QSR-generator reference (rebuild guide, governance, data layers, resolved-gotchas, verification SQL): `git show 0a9aa3f:docs/handoff.md` (preserved in git; partly in living-docs, stale since May 21).
- Memory entries this session: none new (`mlflow-responsesagent-serving`, `ai-gateway-payg-fm` already cover the learnings).
- **Carried-forward open issues**:
  1. **`propose_order` menu-id mapping divergence** (web 2026-06-21, §1 item 1) — agent menu_item_ids don't reliably map to storefront catalog ids; open, unaddressed.
  2. **§6 usage-tracking ledger clarification** — offered, not yet written (note that usage tracking is on at the LLM endpoint).
  3. **Data classification auto-tagging not active** — `configure_monitoring.py` passes `MonitorDataClassificationConfig(enabled=True)` but the workspace tier returns `enabled=False`; `class.*` PII tags applied deterministically by `apply_governance.py` Step 3, not a scanner. Lights up if the tier is upgraded.
