# DPZ POC Requirements Matrix — synthData Coverage

> Brainstormed by Claude Opus · 2026-05-20

## Framing

The Domino's Pizza POC document defines a sprawling **Unity Catalog end-to-end demo** spanning seven workstreams over 8-10 weeks: UC object foundations, lakehouse/catalog federation, AI governance (MLflow/MCP/AI Gateway/Inference Tables/Lakehouse Monitoring/ABAC), Delta Sharing & Marketplace, Microsoft interop (Fabric/OneLake/Purview/Copilot Studio/Power BI), ServiceNow CMDB+ITSM integration, and a 3rd-party landing-page UI driven by UC REST APIs.

The `synthData` project's role in this POC is to be the **synthetic data substrate** — the realistic QSR (Quick Service Restaurant) dataset that gives every workstream something believable, governable, and shareable to act on. We are not the AI governance layer or the federation engine; we are the **fuel**. The question answered here: which of the POC's data-shaped requirements does our current generator already satisfy, which are inexpensive extensions, and which depend on external systems we don't (and shouldn't) generate.

Key constraint: synthData should remain a **single-mission tool** — it generates QSR domain data into Unity Catalog. It should not absorb responsibilities like "stand up a SQL Server" or "provision a ServiceNow tenant." Where the POC needs an external system, synthData's contribution is to produce data shaped *as if* it came from that system and to document the connection step as a separate gap.

## Assumptions

1. **synthData's deliverable for this POC is data, not infrastructure.** Connectors to SQL Server, Snowflake, Purview, ServiceNow, Fabric, etc. are out of scope for this repo — those are configured by the SA/SSA team during the POC.
2. **Schema prefix isolation works.** We can deploy multiple isolated instances (e.g., `dpz_staging`, `dpz_silver`) without colliding with other demo environments.
3. **Domain coverage is fixed in scope but extensible in detail.** We won't grow beyond QSR but will add fields/sub-domains within QSR that the POC needs.
4. **"Realistic" means demo-credible, not Domino's-accurate.** We mimic the *shape* (channels, store types, dayparts, loyalty tiers) but not real menu items, real stores, or real revenue.
5. **External catalog targets need parallel datasets, not real connections.** For federation demos, we can write copies of subsets of data to Snowflake/Postgres/MySQL/SQL Server *if* those systems are reachable from the workspace — and that "if" is the gap.
6. **PII generation is allowed and expected** — Faker already produces names, emails, phones, ZIPs. ABAC/PII demos depend on this. Generated PII is synthetic and safe to govern, mask, and share.
7. **The DPZ POC date (July-Sept 2026) gives runway** — we have time to extend the generator if there are gaps, but we should prioritize what's reusable across other accounts.
8. **AI/model data is largely synthesizable** — inference tables, MLflow registered models, monitoring metrics — these can be generated or produced by lightweight wrapper jobs that *consume* our data.

## Perspectives

**1. Solutions Architect (primary lead).** Needs data ready on day 1 so phase-end demos don't slip. Wants minimum-effort extensions that maximize storytelling.

**2. SSA — UC/Governance.** Needs tagged columns (PII, financial, supply-chain), realistic row-filter scenarios (per-franchisee data scoping), column-mask scenarios (mask `email`/`phone` for non-privileged roles).

**3. SSA — AI Governance.** Needs registered MLflow models with real lineage to UC tables, populated inference tables, AI Gateway traffic with cost/latency signals, drift-eligible features.

**4. SSA — Delta Sharing / Marketplace.** Needs a "publishable" share that mimics DoorDash-style partner sharing — curated subset (no PII, aggregated metrics).

**5. Microsoft Partner Engineer.** Needs the governed dataset to be readable via OneLake mirroring, Purview-scannable, Power BI-queryable with RLS on store-region or franchisee.

**6. DPZ Data Architect / Data Engineering Manager.** Will pattern-match what we show against their real estate. If volumes look toy-sized vs. their 7K stores and 541 TB EDW, credibility suffers.

**7. CISO / Security Sponsor.** Needs to see governed access on synthetic-but-realistic PII without risk.

**8. DPZ Marketing Decision Science / MLOps.** The "Voice of Pizza," "Next Best Action," "Crustopher" demos depend on data with predictive signal.

## Options

### Legend
- **MEET** = current generator already produces this data shape
- **EASY** = additive extension to the existing job; ≤ 1-3 dev days
- **MEDIUM** = nontrivial domain extension; new tables/columns/seeders; 1-2 weeks
- **GAP** = depends on external system / infrastructure / connector we don't control

### Matrix: POC Requirement → synthData Coverage

| # | POC Capability | What the POC Wants | synthData Today | Classification | Gap-Closing Action |
|---|---|---|---|---|---|
| 1 | **UC managed tables (governed)** | Tables in UC with consistent grants/lineage | Already produces 5 staging + silver + ref schemas in UC (Delta) via DLT | **MEET** | None — already there |
| 2 | **External tables / volumes** | `EXTERNAL` tables on ADLS + volumes for unstructured | Only managed tables today; no volumes | **EASY** | Add a volume in setup_job, drop sample PDFs/menu images/POS receipts |
| 3 | **MLflow models in UC** | Register models with lineage to UC tables | Not produced | **EASY** | Add a notebook task that trains a tiny sklearn churn/waste/demand forecast model from silver tables and registers in UC |
| 4 | **Functions in UC** | UDFs / Python functions registered in UC | Not produced | **EASY** | Add 2-3 SQL/Python UC functions (e.g., `tier_to_multiplier`, `mask_email`, `unit_to_region`) in setup_job |
| 5 | **Views in UC** | Governed views for semantic layer | Silver tables but no curated views | **EASY** | Add semantic-layer views: `vw_daily_sales_by_unit`, `vw_loyalty_member_360`, `vw_waste_by_supplier` |
| 6 | **Column-level tags (PII / financial / supply-chain)** | Tags drive ABAC, Purview reconciliation | Not produced | **EASY** | Add `ALTER COLUMN ... SET TAGS` in setup_job for `email`, `phone`, `first_name`, `last_name`, `zip_code` (PII), `subtotal`, `discount_amount`, `waste_cost` (financial), `stock_sku`, `supplier_id` (supply-chain) |
| 7 | **Row filters** (per-franchisee scoping) | ABAC row filters via UC | `franchisee_id` exists in ref.franchisee but not joined into events for filtering | **EASY** | Materialize `franchisee_id` onto silver_order, silver_inventory, then ship a `filter_by_franchisee_group()` row filter function |
| 8 | **Column masks** | Mask PII for non-privileged roles | Not produced | **EASY** | Setup_job creates `mask_email_udf`, `mask_phone_udf` and applies `SET MASK` on PII columns |
| 9 | **ABAC data classification policies** | End-to-end ABAC demo | Not produced; depends on (6) | **EASY** (after tag work) | Once tags exist, define classification policies in setup_job using ABAC API |
| 10 | **Lakehouse Federation — Snowflake** | Query Snowflake in-place | We don't write to Snowflake | **GAP (data shape MEET)** | SA team creates Snowflake account; synthData adds optional JDBC mirror task for `ref.unit` + `silver_order` subset |
| 11 | **Lakehouse Federation — Redshift** | Query Redshift in-place | None | **GAP (data shape MEET)** | Same pattern as Snowflake: SA provisions, synthData optionally mirrors a slice via JDBC |
| 12 | **Lakehouse Federation — SQL Server** | Query SQL Server in-place (mirrors DPZ EDW story) | None | **GAP (CRITICAL — headline DPZ story)** | (a) SA provisions Azure SQL Database; (b) synthData adds `mirror_to_sqlserver` task writing `silver_order` + `ref.unit` + `ref.menu_item` via JDBC — **highest-value gap-closer** |
| 13 | **Lakehouse Federation — Postgres / MySQL** | Query Postgres/MySQL in-place | None | **GAP (data shape MEET)** | Lakebase Postgres is easy; synthData adds `mirror_to_lakebase` task |
| 14 | **Catalog Federation (Glue / Purview / Dataplex)** | Mirror external catalogs into UC | None | **GAP** | Pure connection work — no synthData contribution needed |
| 15 | **Inference Tables** | Captured model inference traffic | None | **EASY** | Once (3) exists, deploy to model-serving endpoint with `auto_capture_config` — inference table populates naturally |
| 16 | **AI Gateway (rate-limit, PII guardrail, cost tracking)** | Route LLM traffic through Gateway | None | **EASY** | Add notebook task creating Gateway-enabled external model endpoint + synthetic prompt driver script |
| 17 | **Lakehouse Monitoring** | Data drift + model drift monitors | None | **EASY** | Configure monitors on `silver_order` (snapshot) and inference table (timeseries) via `databricks.lakehouse_monitoring` API |
| 18 | **Business Metrics / Semantic Layer** | Defined metrics, governed | None | **EASY** | Add `metrics` schema with view definitions: `total_revenue_by_day`, `aov_by_channel`, `waste_pct_by_unit` |
| 19 | **Delta Sharing — outbound (publish)** | Publish share to external recipient (DoorDash story) | None, but silver tables are perfectly shaped for this | **EASY** | setup_job creates a recipient + share containing `vw_daily_sales_by_unit` (no PII) |
| 20 | **Delta Sharing — inbound (consume)** | Consume a share from a partner | None | **GAP** | Stand up a second isolated synthData instance (`schema_prefix=partner_`) that publishes a share back to the main instance — real consume flow without a real partner |
| 21 | **Databricks Marketplace — consume** | List + consume a Marketplace listing | None | **GAP (zero work)** | Marketplace listings already exist; SA picks one during demo. No synthData work |
| 22 | **Microsoft Fabric / OneLake mirroring** | UC tables visible in OneLake via Delta Sharing | Tables exist; mirroring is config | **GAP (data MEET)** | Pure SA configuration — data already in right shape |
| 23 | **Microsoft Purview scan of UC** | Purview scans UC and reconciles tags | UC tables exist; tagging from (6) gives reconciliation story | **GAP (data MEET, depends on 6)** | Pure Purview configuration once (6) is done |
| 24 | **Microsoft Copilot Studio → Genie / MAS** | Natural-language UC access from Copilot Studio | Already creates a Genie space in setup_job | **MEET** | Validate Genie space description, sample questions, and table grants |
| 25 | **Power BI DirectQuery with RLS** | Power BI hits UC with row-level security | Tables exist; needs (7) row filter | **GAP (data MEET, depends on 7)** | Power BI desktop config work for the SA |
| 26 | **ServiceNow CMDB + incident ingestion** | ServiceNow data flows into UC | None — wholly separate domain | **MEDIUM (synthesizable) + GAP (real connector)** | (a) Add new `synth_itsm` domain: `cmdb_ci`, `incident`, `change_request` tables shaped like ServiceNow's data model, linked to `ref.unit` as CI for stores. (b) SA can swap for real connector if sandbox available |
| 27 | **ServiceNow consumption of UC data** | UC → ServiceNow share/API | None | **GAP (pure connection)** | If (26a) is done, a Delta Share back to ServiceNow can be demoed from synthetic data |
| 28 | **UC REST API — schema/table/lineage browse** | 3rd-party landing page hits UC REST APIs | UC tables exist; APIs are platform-level | **MEET** | No work — enough schemas/tables/columns/tags already for a rich landing page |
| 29 | **Landing-page prototype** | Small React/Flask app browsing UC | None | **EASY (separate but small)** | Add a tiny Databricks App (Flask) in `resources/` that calls UC REST and renders a catalog browser |
| 30 | **System Tables (billing, audit, lineage)** | System tables populated and queryable | Platform-managed | **MEET** | Existing job naturally creates audit, lineage, and serverless usage events |
| 31 | **Volume realism (7K stores, multi-TB)** | Demo feels enterprise-grade vs. toy | Currently 250 units, ~12 months backfill | **EASY** | Bump `num_units` to 2000-7000 and `backfill_months` to 24 in a `dpz_*` profile |
| 32 | **Geographic realism (US store locations)** | Mappable, regional analytics | `us_locations.py` already produces real US coordinates | **MEET** | No work |
| 33 | **Temporal realism (hourly/daily/weekly seasonality)** | Believable time-series for drift demos | Already in `demand_model.py` | **MEET** | No work — a clear strength |
| 34 | **Multi-channel order types** | Channel-aware analytics + AI | Channels exist in `orders.py` | **MEET** | Verify channel mix matches DPZ public ratios (~70% delivery). Trivial constant tweak |
| 35 | **GenAI scenario data (Crustopher / Voice of Pizza / Next Best Action)** | Data that supports AI use cases | Loyalty + guest behavior exists; no "voice/text" data | **MEDIUM** | Add synthetic `customer_feedback` table with Faker-generated review text + sentiment label; optionally generate menu Q&A pairs for Crustopher |
| 36 | **Supply chain / supplier domain depth** | Supplier risk, multi-tier supply | Have `ref.supplier` and `recipe_ingredient`; no POs / shipments | **MEDIUM** | Add `purchase_order`, `shipment`, `supplier_invoice` tables to inventory domain |
| 37 | **Workforce / labor analytics depth** | Labor cost vs. demand, scheduling | Have `shift` + `time_punch` | **MEET** | Already there. Maybe add `employee` ref table with hire_date for tenure analytics — trivial |
| 38 | **Cross-cloud demonstration** | Same UC governs AWS + Azure + GCP | DPZ is Azure-heavy; we're cloud-agnostic | **GAP** | Pure deployment / workspace setup. synthData runs anywhere |
| 39 | **Profisee / MDM replacement narrative** | UC replaces MDM tool | None | **GAP (no work)** | Storytelling/positioning, not data |
| 40 | **Cost showback (system.billing.usage)** | Demonstrate cost attribution | Platform-managed | **MEET (just run the job)** | DLT pipeline + setup/destroy jobs naturally emit billing usage |

### Summary Roll-Up

| Category | Count | Implication |
|---|---|---|
| **MEET (no work)** | 10 | Validate during phase-0 dry run |
| **EASY extensions** | 15 | One sprint of additive work in synthData repo |
| **MEDIUM extensions** | 3 | New domain (ITSM), supply-chain depth, GenAI scenario data — each ~1-2 weeks |
| **GAP — external system / pure config** | 12 | Owned by SA/SSA team, not synthData. Provide *data-shape parity* (JDBC mirrors) where it accelerates the demo |

## Recommendation

**Three deliverables, in priority order:**

**1. "DPZ governance profile" of the existing job** (Sprint 1, ~1 dev week, highest leverage)
Column tags, masks, row filters, semantic views, classification policies, a populated volume. These are setup_job SQL/Python additions — they don't change the generator at all. Unblocks every governance, ABAC, federation, Purview, and Power BI workstream simultaneously.

- Column tags (PII / financial / supply-chain)
- UC functions, semantic-layer views, metric views
- Volume (`volume_assets`) + sample unstructured files
- Bump `num_units` / `backfill_months` for DPZ profile
- Column masks + row filter function
- ABAC classification policies wired up

**2. AI governance scaffolding pack** (Sprint 2, ~1 dev week)
Tiny MLflow models, inference table, AI Gateway endpoint, Lakehouse Monitoring. Additive notebooks invoked from setup_job, not new domains. Unlocks Phase 3 (AI Governance) end-to-end without depending on Domino's real models.

- Toy MLflow models registered in UC (churn, demand forecast, waste prediction)
- Model serving endpoint + inference table
- AI Gateway external model endpoint + synthetic prompt driver
- Lakehouse Monitoring on silver_order + inference table
- Delta Share (outbound) for DoorDash-pattern demo

**3. DPZ-specific extensions** (Sprint 3, ~1-2 dev weeks)
ServiceNow-shaped synthetic ITSM domain, customer feedback for Voice of Pizza, supply-chain depth, JDBC mirroring.

- ServiceNow-shaped synthetic ITSM domain (`synth_itsm.cmdb_ci`, `incident`, `change_request`) — schema patterned directly off ServiceNow's documented table specs
- Customer feedback / sentiment data for Voice of Pizza
- Supply chain depth (purchase_order, shipment, supplier_invoice)
- JDBC mirroring task — opt-in via env vars, skip cleanly if target system not configured

**Landing page (Sprint 4, 3-5 days — optional)** — better owned by Patrick Bergman's team; synthData provides data + tags + lineage.

**Explicitly out of scope:**
- Standing up SQL Server / Snowflake / Redshift / ServiceNow / Purview / Fabric environments
- Real DPZ data ingestion
- Cross-cloud deployment automation

### Top Risks

**Risk 1: Scope creep into infrastructure.** Keep JDBC mirror tasks **opt-in via env vars** and skip cleanly if the target system isn't configured. Never block the main job on missing connections.

**Risk 2: ServiceNow synthesis credibility.** Pattern the `synth_itsm` schema directly off ServiceNow's documented `cmdb_ci`, `incident`, `change_request` table specs (publicly available) rather than inventing a shape. If credibility risk is high, de-prioritize Sprint 3 ITSM and lean on storytelling that "synthData provides the data shape, real ServiceNow connector replaces it when ready."

---

*Key files: `src/generator/domains/`, `src/generator/reference/seeder.py`, `resources/setup_job.yml`, `resources/destroy_job.yml`, `databricks.yml`*
