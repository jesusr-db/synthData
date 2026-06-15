# Opus Brainstorm: ontos QSR Ontological Layer

*Generated: 2026-05-26 | Model: claude-opus-4*

---

## Framing

The synthData project is a Domino's-style QSR synthetic data generator that produces ~26 streaming silver tables, 4 gold tables, 5 metric views, plus reference (units, menu, weather, events) and staging Bronze layers — all under one Databricks catalog. **Ontos** (databrickslabs/ontos) is a deployed Databricks App that acts as a "Business Catalog for Unity Catalog," modeling four overlapping things on top of UC assets:

1. **Organizational structure** — Domains (hierarchical), Teams, Projects.
2. **Asset registration & contracts** — Datasets (pointer to `catalog.schema.table`), Data Contracts (ODCS v3.1.0 schema + SLOs + quality rules), Data Products (ODPS v3.x, with Input/Output ports + ownership).
3. **Semantic layer** — A knowledge graph (RDF/RDFS) of business Concepts and a Business Glossary, joined to physical assets via three tiers of "semantic links": contract → schema → property. Concepts can have skos:broader hierarchies and be grouped by industry ontology.
4. **Governance overlays** — Compliance DSL rules, Asset Review workflows, Entity Relationships (lineage/containment/consumption), tags.

The opportunity is to project the QSR data model — which already has natural domains (Order, Inventory, Guest, Loyalty, Workforce, Restaurant, Menu, Finance, Franchise, Procurement, FoodSafety, Marketing/External-Signals) and clear referential semantics — onto this ontos topology so the catalog has a true business-meaning layer, not just table descriptions. The constraint is that ontos is already deployed (we cannot redesign it), it primarily exposes its functionality via REST API at `/api/*` under OAuth/SP auth, and seed data normally arrives via SQL-on-startup or REST calls. We need a method that fits the existing Databricks Project Automation Standard (DAB setup/destroy jobs, idempotent, zero manual steps).

A secondary constraint: the synthData project must remain runnable without ontos. The ontology layer is an **additive overlay**, not a dependency of the generator or pipeline. If ontos is unreachable, setup must still succeed.

---

## Assumptions

1. **Catalog/schema layout (actual):** Catalog is `jmrdemo` (per `databricks.yml`), schemas are `synth_staging`, `synth_silver`, `synth_ref`, `synth_metrics`. The user's prompt said "synth/synth_ref/synth_metrics" — I'm assuming they meant the prefixed form and we'll use the bundle variables (`catalog_name`, `schema_prefix`) rather than hardcoding.
2. **Ontos deployment details:** Ontos is reachable at the known Databricks App URL. Authentication is via Databricks OAuth with the CLI token (verified working).
3. **API surface usable for seeding:** All 54 `*_routes.py` files expose standard `POST /api/<resource>` endpoints. Full OpenAPI spec fetched at runtime.
4. **ODCS/ODPS as authoring format:** We author contracts in ODCS v3.1.0 YAML and products in ODPS v3.x YAML, then POST them.
5. **Semantic concepts come from us, not FIBO:** No off-the-shelf QSR industry ontology exists in ontos. We will define a custom `qsr-ontology.ttl` (RDF/Turtle) in a future phase.
6. **Stakes:** This is a demo/internal asset; optimise for narrative completeness over enterprise-grade lineage.
7. **Cleanup:** Every ontos object created must be deletable via a destroy notebook calling `DELETE /api/<resource>/<id>` in reverse dependency order.
8. **No conflict with `apply_governance.py`:** The ontos layer is parallel to UC governance — same physical tables, different semantic store.
9. **Idempotency strategy:** Deterministic UUIDs or GET-then-POST/PATCH pattern.

---

## Perspectives

**The Data Mesh Architect.** Wants Domains aligned to bounded contexts, Data Products with explicit Input/Output ports, contracts as enforceable interfaces. Would push us to model 5–7 first-class domains with one Data Product per Gold table and one per consumer-facing Silver group. They care most about **port modeling and contracts**.

**The Semantic Modeler / Ontologist.** Wants a clean RDF taxonomy with `skos:broader` hierarchies, OWL classes for Order/Unit/Guest/Inventory, object properties (`hasItem`, `placedAt`, `fulfilledBy`, `memberOf`), and three-tier semantic links wiring property → concept. They care most about **the ontology TTL file and the concept-to-column links**.

**The Demo Curator / Field Engineer.** Wants ontos to "light up" in 5 minutes with a visually impressive graph, named domains, a few hero products, and AI-assistant-discoverable assets via MCP. Cares most about **breadth and visual density over depth**.

**The Platform Engineer.** Wants this to drop into the existing DAB cleanly, run idempotently, have a destroy path, not break existing tests, and not require keeping a private fork of ontos. Cares most about **the setup/destroy notebooks, secret handling, and not coupling generator/pipeline to ontos**.

**The Consumer / Analyst.** Wants to ask in natural language "what does `unit_performance_daily.sos_compliance_pct` mean and who owns it?" and get a coherent answer that pulls from the ontology + contract. Cares about **MCP integration, search quality, and that descriptions actually match what's in the table**.

---

## Options

### Option A — Code-First ODCS/ODPS YAML + Loader Notebook (Recommended)

**Description.** Author the entire QSR ontology as version-controlled artifacts in the synthData repo under `conf/ontos/`. A setup notebook `src/setup/apply_ontos.py` reads these files, calls ontos REST API (using `requests` + Databricks CLI token), upserts every object in dependency order, and logs results. A destroy notebook does the inverse.

**Pros.**
- Ontology is in git, code-reviewed, diffable, and re-runnable.
- Natural fit with existing `apply_governance.py` pattern.
- ODCS/ODPS YAML is portable.
- Idempotent via deterministic UUIDs.
- Destroy path is symmetric.

**Cons.**
- Largest upfront authoring effort.
- Requires reverse-engineering exact ontos JSON shapes (one-time research cost, done).
- Sensitive to ontos API drift.

**Fit.** Best fit for this codebase's automation standard.

### Option B — MCP-Driven Conversational Bootstrap

Non-deterministic, expensive per run, brittle destroy path. **Poor fit**.

### Option C — Direct SQL-Style Seed via Ontos DB Migration

Bypasses auth/audit layer, tightly coupled to internal schema. **Wrong layer**.

### Option D — Minimal Tag-Only Layer

Underdelivers vs. "full ontological layer". **Fallback only**.

---

## Recommendation

**Go with Option A (Code-First ODCS/ODPS YAML + Loader Notebook), structured into three phases.**

**Phase 1 — Skeleton & Connectivity (DONE — executed interactively 2026-05-26).**
- ✅ 8 QSR domains (QSR Operations → 7 sub-domains)
- ✅ 2 QSR teams (QSR Analytics, Restaurant Ops Data)
- ✅ 46 table assets registered (synth_silver: 19, synth_ref: 9, synth_staging: 5, synth_metrics: 13)
- ✅ 7 data contracts with asset links
- ✅ 6 data products with dataset links

**Phase 2 — ODCS Schema Contracts.**
Author full ODCS v3.1.0 contracts with column-level schemas, SLOs, and quality rules. Save as `conf/ontos/contracts/*.yaml`. Wire `apply_ontos.py` to read YAML and POST schemas via `/api/data-contracts/{id}/schemas`.

**Phase 3 — Semantic Layer.**
Author `conf/ontos/ontology/qsr-ontology.ttl` (~40 OWL classes + properties). Upload via `/api/semantic-models/upload`. Wire semantic links (contract → concept, schema → concept, property → concept) via `/api/semantic-links/`.

**Top risks:**
1. **API shape uncertainty for schemas/SLOs.** Mitigate: introspect OpenAPI before authoring Phase 2.
2. **Authentication for job-based invocation.** Current approach uses Databricks CLI token (personal). For DAB job tasks, needs service principal token from secret scope.

---

*Full brainstorm saved to `research/ontos-qsr-ontological-layer_2026-05-26.md`*
