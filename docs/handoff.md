# Handoff for Next Agent — synthData

**As of**: 2026-06-22 00:30 EDT
**Branch / HEAD**: `main` @ `413850e` (merged commerce agent + tracing + jmr_gateway switch). **Not pushed** — back up before destructive git ops.

> Single canonical handoff. Detail lives behind §4 pointers, not inlined.

## §1 — Launchpad (act on this first)

- **State**: clean re: source (`.pyc` + 1 untracked `research/*.md`) · 192 tests pass · agent live on `synth_qsr-commerce-agent`, LLM via `jmr_gateway` → `claude-sonnet-4-5`.
- **Next actions** (priority order):
  1. **PRIORITY — explore LOE: migrate the agent from a Model Serving endpoint to a Databricks App** (ref: https://docs.databricks.com/aws/en/generative-ai/agent-framework/author-agent). **Must verify the feature-store (`synth_qsr-customer-features`) and recommender (`synth_qsr-recommender`) endpoints are invoked properly under the App's identity.** LOE drivers spelled out in §4 "App-migration exploration" — the pure core ports as-is; the real work is auth (App SP grants), live menu/price data access, tracing, and the web-facing URL change.
  2. **OPEN BUG: `propose_order` menu-id ↔ storefront catalog mismatch.** MeatZZa(4)→`13`/`10`/`2003` in 4/5 (web 2026-06-21). Two-namespace problem (`ref.menu_item.menu_item_id` baked at log time vs storefront `ProductCatalog`). Roadmapped (`docs/roadmap.md` → "Commerce Agent — Known Issues"); needs a joint call on which side owns the id map.
  3. To make the live endpoint callable by the web SP: set `commerce_agent_query_principal` and re-run `build_commerce_agent`.
- **Landmines**: trace PAT is owner-minted, 90-day (rotate / move to SP for prod). · Re-test the agent via a one-off `jobs/runs/submit` of `build_commerce_agent` (notebook path: NO `.py` ext) — NOT the full `setup_job` (regenerates all data). · Don't delete `jmr_gateway` in destroy — it's the user's shared gateway.

## §2 — This session

- Switched the agent's LLM hop to the standalone Unity AI Gateway `jmr_gateway` (base `…/ai-gateway/mlflow/v1`, `model="jmr_gateway"`, now → sonnet-4-5) — evidence: commits `1238e58`..`5647c70`, merged `413850e`; re-validated 5/5 finalize + simple orders, correct ids, traces real.
- Deploy-time fixes for the gateway path: bake `llm_base_url` into config (log validation), drop the gateway from declared `resources` (pre-deploy dep check), single system message (Bedrock rejects two) — `47aee58`,`4bf41c1`,`ffc9e08`.
- Prompt tuning (decisive propose, atomic read-back+propose, wording→nearest menu item) `ca3cd38`,`595728d` — fixed sonnet-4-6 finalize flakiness; harmless on 4.5.
- Prior session (already merged): built+deployed the agent, enabled MLflow tracing + inference tables. Contract ledger updated each step (web repo latest `b7d21e4`).

## §3 — Gotchas this session

- A standalone AI Gateway is NOT a serving endpoint: don't declare it as a `DatabricksServingEndpoint` model resource (pre-deploy dep check fails); invoke at `…/ai-gateway/mlflow/v1` with `model=<gateway-name>` — (durable → memory: `ai-gateway-payg-fm`).
- Bedrock-backed gateway routes reject a second system message ("System message must be at the beginning") — fold identity into one system message — (durable → memory: `mlflow-responsesagent-serving`).
- Model swap behind the gateway is config-only on the gateway side — the agent calls `model="jmr_gateway"` and needs no redeploy when the gateway repoints (4.6→4.5 was zero agent change).

## §4 — Pointers

- **App-migration exploration (priority #1 detail):** the pure core (`src/agent/{pricing,tools,loop,prompts}.py` + `parse_request`/`build_response`/`to_openai_messages`/`from_openai_response`) is framework-agnostic — a FastAPI route replaces `CommerceAgent.predict` (low effort). The LOE is in: (a) **auth** — an App runs as its own service principal; grant it CAN_QUERY on `synth_qsr-recommender` + `synth_qsr-customer-features` and query access to `jmr_gateway`, and wire tokens (today the served model's automatic-auth `resources` do this for free) — **verify both endpoints invoke correctly under the App SP**; (b) **menu/price/occasion data** — today baked at model log time, an App needs a live path (SQL warehouse / statement execution at startup or per request); (c) **tracing** — replace the `ENABLE_MLFLOW_TRACING` env-var path with in-app `mlflow.openai.autolog()`/manual spans to the experiment; (d) **inference tables** — were a gateway/endpoint feature, an App must log payloads itself; (e) **deploy** — `app.yaml` + DAB app resource + `databricks apps deploy`, update destroy. **Web-facing change:** BFF currently POSTs the serving `/invocations` URL; an App exposes an HTTPS route (URL + auth change) → coordinate in the ledger; request/response shape can stay (we own it). Trade-off: App gains streaming/custom-UI/session control and sheds managed-endpoint limits we hit (usage-tracking-unsupported, single-system, deprecated auto_capture); costs managed ResponsesAgent serving + gateway inference tables + the simple invocations contract, and you own auth/scaling/tracing.
- Plan: `docs/superpowers/plans/2026-06-18-pizzatel-commerce-agent.md` · Roadmap (menu-id issue): `docs/roadmap.md` → "Commerce Agent — Known Issues" · Agent API: `docs/api.md` · Contract ledger (web repo): `gitRepos_FY26/opentelemetry-demo/docs/integration/agent-endpoint-contract.md`.
- Agent code: `src/agent/{pricing,tools,loop,commerce_agent,gateway,serving,prompts}.py`; setup: `src/setup/build_commerce_agent.py` (gateway mode via `llm_gateway_name`); teardown: `src/setup/destroy_notebook.py` (0h-1b); tests: `tests/test_agent_*.py`.
- Memory: `mlflow-responsesagent-serving`, `ai-gateway-payg-fm` (cover the durable serving/gateway learnings). Legacy QSR-generator reference: `git show 0a9aa3f:docs/handoff.md`.
- **Carried-forward open issues**:
  1. **App-migration LOE exploration** (priority #1 above) — not started.
  2. **`propose_order` menu-id mapping mismatch** (§1 #2, roadmapped) — open.
  3. **Data classification auto-tagging not active** — `configure_monitoring.py` sets `MonitorDataClassificationConfig(enabled=True)` but the workspace tier returns `enabled=False`; `class.*` tags applied deterministically by `apply_governance.py` Step 3. Lights up if the tier is upgraded.
  4. **In-UI real-time trace view** — needs `databricks-agents>=1.2.0` + GenAI-monitoring beta; traces are already captured/queryable via API + inference table without it.
