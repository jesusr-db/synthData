# Brainstorm: Customer Feature Store + Basket-Aware Recommendation Model

_Generated 2026-06-14 by Opus brainstorming subagent for the qsr-synth-data-generator project._

**Topic:** Create a customer feature store for personalized features that help the website list recommendations for additional purchases based on user/loyalty profile + store + what's already in the basket (e.g., don't recommend soda if there's already a soda in the cart). Two-fold: (1) real-time customer feature serving on Databricks, and (2) a recommendation model that takes that info + context (store and basket) to recommend other items to buy on the website.

---

## Framing

The opportunity is a **two-layer personalization system** bolted onto the existing QSR synthetic-data medallion stack:

1. **A real-time customer feature store** — precomputed per-guest and per-store features (recency/frequency/monetary signals, category affinities, tier, daypart habits) served at low latency via Databricks Online Tables / Feature Serving, keyed by `guest_profile_id`/`member_id` and `unit_id`.
2. **A basket-aware recommendation model** — given (customer features + store context + current basket), it returns ranked "add-on" item suggestions, deployed behind a Model Serving endpoint that the website calls synchronously at cart time.

The defining business rule is **basket-aware suppression and complementarity**: don't recommend a soda when a soda is already in the cart; instead surface complements (dip with wings, dessert with a pizza, drink with a no-drink basket). This is a classic "next-best-item given partial basket" problem, distinct from a static popularity ranker.

**Constraints that shape every option:**
- This is a **synthetic-data demo project**, not a production retail system. The goal is a *credible, fully-automatable* showcase of Databricks personalization primitives — not state-of-the-art rec quality. Optimize for "shows the platform pattern end-to-end" over "maximizes NDCG."
- **Fully DAB-automatable from zero** (global rule): feature tables, online tables, model training, and the serving endpoint must be created by `resources/` bundle YAML where DAB supports it, and by the setup-job notebook pattern where it doesn't — each with a matching teardown in `destroy_notebook.py`/`destroy_job.yml`.
- Must build **on existing tables** (silver `guest_order`/`order_item`/`guest_profile`/`loyalty_transaction`, ref `unit`/`menu_item`), respect the `schema_prefix` convention, and slot into the existing setup-job task DAG after `start_pipeline`.
- The generator currently emits orders **without modeling basket complementarity** (items are drawn independently per `random_menu_item`). The "don't recommend soda if soda present" signal therefore has weak ground truth in the data today — affinities must either be mined from co-occurrence (noisy, since items are independent) or seeded from a curated complementarity matrix. This is the single biggest technical risk and the most important design decision.

## Assumptions

Made in lieu of interactive clarification (all should be confirmed with the user before planning):

1. **Demo-grade, not production-grade.** Success = an end-to-end working pattern (offline features → online serving → basket-aware model → queryable endpoint) that a field team can demo, not a model that beats a baseline by X%.
2. **Customer identity** = `guest_profile_id`; loyalty members additionally carry `member_id` (same integer in this dataset). Anonymous orders (60% of traffic, `profile_id` null) get a **cold-start / store-only path** rather than personalized features.
3. **"Store" context** = `unit_id`, enriched with `ref.unit` attributes (metro_area, region_id, franchisee_id, market_price_index) and optionally the existing `demand_risk_forecast` (weather/events) as contextual features.
4. **Basket** = a list of `menu_item_id`s the website passes in the serving request. The model must (a) suppress same-subcategory duplicates (the soda rule keys on `menu_item.subcategory == 'soda'` / `category == 'drinks'`) and (b) rank complements.
5. **Complementarity ground truth will be seeded**, not purely mined — a curated category-affinity matrix (pizza→drink/dessert/dip, wings→dip/drink, etc.) drives both an enhancement to the generator and the model's training labels, because the current generator produces independent item draws. (This is a recommendation, surfaced again under Options.)
6. **Latency target**: sub-100ms feature lookup, low-hundreds-ms end-to-end serving — well within Online Tables + Model Serving norms. No hard SLA given.
7. **No live website exists.** Deliverables stop at a **queryable Model Serving endpoint + a demo client** (notebook or small script showing a cart → recommendations round-trip). Optionally a thin Databricks App UI, but treated as out-of-scope for v1.
8. **Feature freshness**: features can be **batch-refreshed daily** (synced to online tables on a schedule), not streaming-updated per order. The "real-time" aspect is *serving latency*, not *feature recency*. Streaming feature computation is a possible later phase.
9. **Catalog/schema**: a new `${schema_prefix}features` schema for offline feature tables and a `${schema_prefix}ml` schema (or reuse) for model artifacts/registered model; UC-registered model + endpoint named per project convention.

## Perspectives

- **Field Engineer / demo owner (primary stakeholder):** Wants the *broadest, cleanest sweep of Databricks personalization features* — Feature Engineering in UC, Online Tables, Feature Serving, MLflow, Model Serving, and ideally the "feature-function-as-on-demand-feature" pattern — wired end-to-end and reproducible via `databricks bundle deploy`. Cares that destroy cleans up online tables and endpoints (they cost money if orphaned).
- **The website/app engineer (consumer of the endpoint):** Wants one synchronous call: send `{guest_id?, unit_id, basket:[item_ids]}`, get back `[{item_id, name, score, reason}]`. Doesn't want to assemble features client-side — the endpoint should fetch features internally (automatic feature lookup) so the contract stays thin.
- **The data scientist:** Cares that training labels are defensible. With independent item draws today, a co-occurrence model learns nothing meaningful; they'll push for either generator enhancement or a curated affinity prior. Also wants offline eval (hit-rate@k on held-out baskets) even if demo-grade.
- **The data engineer / governance owner:** Feature tables are new UC assets that must inherit the existing governance pack (descriptions, ABAC, monitoring, ontos semantic links) and the `franchisee_id`/`region_id` propagation pattern. New schemas need the same lifecycle treatment.
- **The "automation from zero" enforcer (CLAUDE.md):** Every object must be created by DAB or the setup job, and every object must be destroyable. Online tables and serving endpoints are partially DAB-manageable (resources exist) but the model-training + online-table-sync sequencing belongs in the setup job.
- **The basket-rule purist:** "Don't recommend soda if soda present" is a *constraint*, not a *score*. It should be enforced as a deterministic post-filter (subcategory exclusion), independent of the model, so the rule is guaranteed regardless of model output. The model handles ranking complements; the filter handles suppression.

## Options

### Option A — Rules + co-occurrence ("market-basket") recommender, no ML model training
**Description:** Compute an item-item co-occurrence / lift matrix offline from `order_item` joined by `guest_order_id` (Spark FP-growth or a simple co-occurrence count). Store as a feature table. At serve time, a lightweight pyfunc model: take basket → look up complements by lift → apply the subcategory-suppression filter → blend with a per-customer category-affinity feature (from the feature store) and store-level popularity → return top-k. "Model" is a deterministic pyfunc wrapped in MLflow and served.
- **Pros:** Simplest to build and explain; the recommendation logic is transparent (great for a demo — you can *show why* an item was recommended); no labeled-training step; still exercises Feature Store + Online Tables + Model Serving + automatic feature lookup. Robust to the weak-ground-truth problem because lift is computed directly, and the suppression rule is explicit.
- **Cons:** With the current independent-draw generator, co-occurrence lift is ~uniform (no real signal) — recommendations would look random unless the generator is enhanced. "Not really ML" may undersell the platform's ML serving story.
- **Fit:** High for a fast, legible demo *if* paired with generator enhancement or a seeded affinity matrix. This is the **pragmatic baseline**.

### Option B — Trained ranking/classification model with automatic feature lookup (recommended)
**Description:** Two parts.
  1. **Generator enhancement (prerequisite):** Add a curated category-complementarity matrix to the generator so baskets exhibit realistic co-purchase patterns (pizza pulls drinks/desserts/dips; wings pull dips/drinks; the soda-duplicate case is naturally rare). This gives the model real signal *and* makes the demo data self-consistent.
  2. **Model:** Train a candidate-ranking model (e.g., gradient-boosted classifier scoring P(add item X | customer features, store, current basket) over candidate items, or a two-tower-lite). Customer + store features come from UC feature tables via a `FeatureLookup`/`FeatureSpec` so the endpoint fetches them automatically by `guest_id`/`unit_id` at request time; basket is the runtime input. Deterministic subcategory-suppression filter runs as a post-step inside the pyfunc wrapper. Register in UC, serve via Model Serving, back features with Online Tables.
- **Pros:** Exercises the *full* Databricks personalization stack — Feature Engineering in UC, FeatureLookup/FeatureSpec, Online Tables, MLflow training + registry, Model Serving with automatic feature lookup. Thin client contract (just guest_id + unit_id + basket). Real (if synthetic) signal because the generator now produces complementary baskets. Strong, complete demo narrative.
- **Cons:** Largest scope — touches the generator, adds a training job, adds feature pipelines and online sync, adds serving. Generator change risks the existing 102-test suite (must be additive/guarded like the weather phase's `if weather_event_data:` pattern). More moving parts to destroy.
- **Fit:** **Best overall** for the stated two-fold vision (real-time feature serving *and* a context-aware rec model). Highest demo value; the generator enhancement is the load-bearing piece.

### Option C — Vector Search semantic/embedding recommender
**Description:** Embed items (and/or customer "taste vectors" from purchase history) and use Databricks Vector Search to retrieve nearest complementary items given basket + customer embedding; apply suppression filter.
- **Pros:** Showcases Vector Search; handles cold-start items via content embeddings; trendy GenAI angle.
- **Cons:** Overkill for an ~80-item menu — ANN retrieval adds infrastructure with little benefit at this catalog size; "complementarity" (pizza→drink) is *not* semantic similarity (pizza is similar to pizza, not to soda), so naive embeddings give the *wrong* recommendations unless you engineer complementarity into the vectors. Adds a VS endpoint/index to provision and destroy.
- **Fit:** Low for v1. Better as a future "GenAI add-on" phase if the project wants a Vector Search story. Note for the record but don't build first.

### Option D — Hybrid: Feature Functions / on-demand features + Option B
**Description:** Option B, plus express the basket-suppression and basket-derived signals (e.g., `basket_has_drink`, `basket_category_counts`) as **on-demand feature functions** (UC Python UDFs registered as features) computed at request time from the basket payload, composed with looked-up batch features in a single `FeatureSpec`.
- **Pros:** Demonstrates the most advanced Feature Store pattern (on-demand + batch features unified); keeps basket logic server-side and declarative.
- **Cons:** Most complex to author and debug; on-demand feature functions have sharper edges in setup/teardown. Easy to over-engineer for a demo.
- **Fit:** Excellent **stretch goal** layered on Option B once the core path works. Not v1-critical.

## Recommendation

**Build Option B, structured so Option A is the working fallback at every step, and treat Option D as an explicit phase-2 stretch.**

Concretely, a phased shape (mirroring how the weather-events feature was successfully delivered):

1. **Phase 1 — Generator complementarity + offline features.** Add a guarded, curated category-affinity matrix to the order generator (additive, behind a config like `conf/basket_affinity.yml`, defaulting to current behavior if absent — protecting the 102-test suite). Build offline UC feature tables in a new `${schema_prefix}features` schema: `customer_features` (RFM, tier, category-affinity vector, daypart habits, keyed by `guest_profile_id`) and `store_features` (popularity by item/daypart, AOV, keyed by `unit_id`), computed by a setup-job notebook from silver tables. Inherit the governance/ontos pattern.
2. **Phase 2 — Model + serving.** Train a basket-aware ranking model with `FeatureLookup` on those tables; wrap in a pyfunc that applies the deterministic subcategory-suppression filter (the soda rule lives here, guaranteed); register in UC; sync features to **Online Tables**; deploy a **Model Serving** endpoint with automatic feature lookup. Add a demo client notebook showing cart → recommendations. Wire creation into `setup_job.yml` (new tasks after `start_pipeline`/`create_metric_views`) and full teardown into `destroy_notebook.py` + `destroy_job.yml` (online tables and endpoint **must** be torn down — they accrue cost).
3. **Phase 3 (optional stretch) — on-demand feature functions (Option D) and/or a thin Databricks App UI.**

**Rationale:** This directly delivers the user's "two-fold" vision (real-time feature serving + context-aware model), exercises the broadest, most current set of Databricks personalization primitives for demo value, and keeps a transparent rules fallback so the demo never shows random output. Phasing isolates the riskiest change (the generator) and lets the offline-feature layer be validated before any serving infrastructure is provisioned.

**Top 2 risks:**

1. **Weak ground truth in current data (highest).** Because the generator draws items independently, any co-occurrence-learned model is meaningless until the generator emits complementary baskets. Mitigation: make the generator-complementarity enhancement a hard prerequisite (Phase 1, item 1), curated rather than learned, and additive/guarded so it can't regress existing tests. If the user rejects touching the generator, the project must fall back to a **seeded affinity matrix** as the model's prior — and expectations for "learned" recommendations should be lowered accordingly.
2. **Cost/teardown of online infrastructure (operational).** Online Tables and Model Serving endpoints are billable, always-on resources that DAB destroy may not fully reap if created imperatively in the setup job. Mitigation: prefer DAB-managed `resources/` declarations where supported (serving endpoint, online table), and for anything created imperatively, add explicit reverse-order deletion to `destroy_notebook.py` with best-effort `[WARN]` logging (the established pattern), plus a verification step that the endpoint and online tables are gone after destroy.

**Key open questions to confirm with the user before writing the plan:** (a) Is enhancing the order generator acceptable, or must the rec layer be strictly read-only on existing data? (b) Is a queryable endpoint + demo notebook a sufficient deliverable, or is a Databricks App UI in scope for v1? (c) Daily batch feature refresh acceptable, or is per-order streaming freshness required?
