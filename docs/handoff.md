# Handoff for Next Agent — synthData

**As of**: 2026-06-22 10:00 EDT
**Branch / HEAD**: `feat/genie-domains` @ `af0923f feat(genie): bake drill-down sample questions into spaces + deck one-pager` — **NOT pushed** (4 local commits, no upstream)

> Single canonical handoff. Detail lives behind §4 pointers, not inlined.

## §1 — Launchpad (act on this first)   [≤10 lines]

- **State**: clean re: source (only `.pyc` churn + 1 pre-existing untracked `research/*.md`) · branch `feat/genie-domains` **not pushed**.
- **Next actions** (≤3, specific):
  1. **Reload Discover → confirm the 4 PizzaTel domains show their Genie spaces**; for a demo, run `genie_domains/demo_onepager.md` flow (Orders&SOS → pivot to Workforce).
  2. **Push `feat/genie-domains` + open PR** (or merge to `main`) — 4 commits, currently local-only.
  3. Optional: **codify the one-time Genie patch into setup/destroy jobs** (today it's live-only on `jmrdemo`) — sources in `genie_domains/build_*.{py,sh}` + `teardown.sh`.
- **Landmines** (≤2): Domains/Discover is account-level **Beta** — both previews ("Domains and Discover Page" account + "Discover Page" workspace) must stay ON or Domains vanish from the UI (assets stay tagged). · Shared demo account already had **37 other teams' domains** — never bulk-delete domains/tags; use `genie_domains/teardown.sh` (scoped to our 4).

## §2 — This session   [≤5 bullets, each with evidence]

- Built **4 grounded Genie spaces** over `jmrdemo.synth_*` (Orders&SOS, Loyalty, Inventory, Workforce) — evidence: `a603bc4`; validated live (Genie calls `f_sos_compliance`, `MEASURE()` over metric views).
- Created `jmrdemo.synth_genie` grounding: **13 trusted SQL functions + 4 metric views + table comments** — evidence: `a603bc4`, `verify.sh` (functions=13).
- Created **4 governed tags** + applied to the 4 spaces (`entity-tag-assignments` API) and curated UC assets — evidence: `a603bc4`, `verify.sh` (4/4 space tag-assignments OK).
- Created+**published 4 Discover Domains** via `POST /api/2.0/domains` (`effective_draft=false`) — evidence: `07309ce`, `genie_domains/domains_created.json`.
- Baked **what/why/recommend drill-down sample questions** into all 4 spaces + scenarios doc + deck one-pager — evidence: `afac548`,`af0923f`; 3 hardest drill-downs (labor-per-order, below-par, tier breakage) validated live.

## §3 — Gotchas this session   [≤5]

- Discover **Domains** can be created+published via `POST /api/2.0/domains` (`tag_key`=governed tag; returns `effective_draft=false` = live) — earlier "UI-only" belief was wrong — (durable → memory: `genie-spaces-and-domains`).
- Tag a Genie space (workspace object) via `POST /api/2.0/entity-tag-assignments` (`entity_type=geniespaces`), NOT UC `SET TAGS` — (durable → memory: `genie-spaces-and-domains`).
- `serialized_space` v2: strict validator — `text_instructions` must be EXACTLY one item; all id-lists sorted by `id` (32-hex); `tables` sorted by identifier; curated SQL = UC functions in `sql_functions` — (durable → memory: `genie-spaces-and-domains`).
- `databricks genie update-space` needs `--serialized-space` flag; `create-space` takes the blob positionally — (durable → memory: `genie-spaces-and-domains`).
- `COMMENT ON COLUMN` is blocked on silver streaming tables (Lakeflow); `COMMENT ON TABLE` and `SET TAGS` both work on them — (session-local).

## §4 — Pointers

- Genie Domains patch: `genie_domains/` (`README.md`, `build_spaces.py`, `build_domains.sh`, `demo_scenarios.md`, `demo_onepager.md`, `verify.sh`, `teardown.sh`, `*_created.json`, `01_grounding.sql`).
- Memory added/updated this session: `genie-spaces-and-domains` (reference), `qsr-genie-domains` (project).
- **Carried-forward open issues** (prior handoff, still unresolved — agent workstream, dormant this session):
  1. **App-migration LOE exploration** (migrate commerce agent from Model Serving → Databricks App; verify feature-store + recommender invoke under the App SP) — not started. Detail: prior handoff §4 `git show 4880670:docs/handoff.md`.
  2. **`propose_order` menu-id ↔ storefront catalog mismatch** — open; roadmapped in `docs/roadmap.md` → "Commerce Agent — Known Issues".
  3. **Data classification auto-tagging not active** — `configure_monitoring.py` sets `enabled=True` but tier returns `False`; `class.*` tags applied deterministically by `apply_governance.py` Step 3.
  4. **In-UI real-time trace view** — needs `databricks-agents>=1.2.0` + GenAI-monitoring beta; traces already API/inference-table queryable.
  - Agent-workstream landmines still live: trace PAT is owner-minted 90-day (rotate/SP for prod); re-test agent via one-off `build_commerce_agent`, NOT full `setup_job` (regenerates all data); don't delete `jmr_gateway` in destroy (shared).
