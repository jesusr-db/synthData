# QSR Synthetic Data Generator — Agent Handoff

## Project State: Deployed and Running

**Workspace:** `adb-7405605519549535.15.azuredatabricks.net` (Azure)  
**Catalog:** `jmrdemo`  
**DAB target:** `dev` (profile: `DEFAULT`)  
**Bundle root:** `/Users/jesus.rodriguez/Documents/ItsAVibe/gitrepos_FY27/synthData`  
**GitHub:** `https://github.com/jesusr-db/synthData`

---

## What's Built

A fully automated QSR synthetic data generator (Domino's-style, 250 units):

- **Staging layer:** Python generator writes to 5 Delta tables in `jmrdemo.synth_staging`
- **Silver layer:** Lakeflow Declarative Pipeline reads staging via `readStream`, produces 14 Silver tables in `jmrdemo.synth_silver` — table/column comments, PK, and FK constraints declared inline in `@dp.table` decorators so they survive every pipeline refresh. `guest_profile` uses CDC (`dp.create_auto_cdc_flow`, SCD Type 1). All silver tables include `franchisee_id` (left-joined from `ref.unit`).
- **Gold layer:** 4 Gold aggregate tables co-located in `jmrdemo.synth_silver` (DLT managed)
- **Metrics layer:** 4 Unity Catalog metric views in `jmrdemo.synth_metrics` (WITH METRICS LANGUAGE YAML — reusable measures + dimensions)
- **Genie Space:** Pre-configured with domain instructions, 10 seed questions, all silver + metrics tables
- **Governance Pack:** UC column tags (`class.*` for PII, `financial`, `supply_chain`), column masks (email/phone via per-table `SET MASK`), row filter by franchisee on 6 silver/ref tables, 3 UC scalar functions, UC Volume with sample files. 4 Lakehouse Monitors active (3 snapshot + 1 timeseries on `silver.guest_order`) with 12h auto-refresh schedule.

---

## Deployed Resources

| Resource | Type | Notes |
|---|---|---|
| QSR MVM Pipeline [dev] | DLT Pipeline (triggered) | Triggered per generator run |
| QSR Setup [dev] | Job (8 tasks) | Run to rebuild from scratch |
| QSR Generator Live [dev] | Job (every-minute cron) | UNPAUSED — running live |
| QSR Destroy [dev] | Job (teardown) | Ready — tears down non-DAB objects |

All resources tagged: `project: qsr-synth-data-generator`

---

## Setup Job Task Graph

```
setup ──→ backfill ──→ start_pipeline ──→ create_metric_views ──→ create_genie_space ──┐
                                      └──→ apply_governance ──→ configure_monitoring ───┤
                                                                                         ↓
                                                                             unpause_generator
```

`unpause_generator` waits for `backfill` (via `start_pipeline`) + `create_genie_space` + `configure_monitoring`.

> **Why `start_pipeline` is after `backfill`:** The DLT pipeline full_refresh needs staging data to exist. Placing it after `backfill` ensures the pipeline processes real data on first run, and avoids transient failures when the pipeline starts before staging tables are populated.

### Important behaviors

- **`setup_notebook.py`** uses `CREATE TABLE IF NOT EXISTS` for all 5 staging tables — never drops them. This preserves Delta table IDs so DLT streaming checkpoints survive re-runs.
- **`start_pipeline_notebook.py`** waits for any active pipeline update; falls back to `full_refresh=True` if the update fails (clears broken streaming checkpoint state). Retry logic: `max_attempts=2`.
- **`backfill`** is incremental — reads `MAX(event_ts)` across staging tables and resumes from the next full hour. Safe to re-run.
- **`apply_governance`** is fully idempotent — uses `CREATE OR REPLACE` / `IF NOT EXISTS` throughout. Per-table `SET MASK` (not ABAC) — see Known Gotchas.
- **`configure_monitoring`** creates/updates 4 Lakehouse Monitors. GET-before-CREATE pattern updates existing monitors (schedule + classification config) rather than skipping them.

---

## Key Files

```
databricks.yml                              # bundle config, catalog_name=jmrdemo, schema_prefix=synth_
src/generator/main.py                       # notebook entrypoint (backfill + live modes)
src/generator/runner.py                     # GeneratorConfig, backfill_ticks, live_tick
src/generator/domains/orders.py             # order + item + payment generation
src/generator/domains/inventory.py          # inventory events (weighted waste categories)
src/generator/domains/loyalty.py            # loyalty earn + redeem transactions
src/generator/domains/guest.py              # guest profiles + churn events
src/generator/reference/us_locations.py     # unit seeder (seed=42, deterministic)
src/generator/reference/seeder.py           # seed_all() — overwriteSchema=true for ref.unit
src/generator/entity_registry.py           # FK registry (unit_price_index, item_price_multiplier)
src/pipeline/mvm_pipeline.py               # Lakeflow Declarative Pipeline (14 silver + 4 gold; franchisee_id on 5 tables)
src/setup/setup_notebook.py                # schemas + staging tables (IF NOT EXISTS) + ref seed
src/setup/start_pipeline_notebook.py       # idempotent pipeline start with full_refresh fallback
src/setup/create_metric_views.py           # 4 UC metric views (WITH METRICS LANGUAGE YAML)
src/setup/create_genie_space.py            # Genie Space via REST API
src/setup/apply_governance.py             # UC tags (class.*/financial/supply_chain), masks, functions, row filter, volume
src/setup/configure_monitoring.py         # 4 Lakehouse Monitors (3 snapshot + 1 timeseries), 12h schedule, classification config
src/setup/destroy_notebook.py             # teardown logic (includes governance cleanup)
src/setup/unpause_generator_notebook.py   # unpauses generator job after setup
resources/pipeline.yml                    # DLT pipeline DAB resource
resources/setup_job.yml                   # 8-task setup job (start_pipeline depends on backfill)
resources/generator_job.yml              # live generator job (UNPAUSED)
resources/destroy_job.yml               # destroy job
```

---

## Data Layers

### Staging Tables (`jmrdemo.staging`)

| Table | Event types |
|---|---|
| `order_events` | guest_order, order_item, payment, status_event, delivery_order |
| `inventory_events` | on_hand_balance, waste_log, replenishment_order, receiving_order |
| `guest_events` | guest_profile |
| `loyalty_events` | loyalty_transaction, reward_redemption |
| `workforce_events` | shift, time_punch |

### Reference Tables (`jmrdemo.ref`)

| Table | Contents |
|---|---|
| `unit` | 250 units with `unit_volume_bias`, `market_price_index` |
| `menu_item` | Menu catalog by daypart |
| `recipe_ingredient` | Bill of materials per menu item |
| `financial_period` | Monthly periods with fiscal quarter |
| `item_price` | Per (menu_item, period) price multiplier — quarterly drift ±3-6% |
| `franchisee` | Franchisee entities |
| `supplier` | 6 suppliers |

### Metric Views (`jmrdemo.metrics`)

These are Unity Catalog metric views (`WITH METRICS LANGUAGE YAML`), not plain SQL views.
They define named measures and dimensions that can be sliced ad-hoc without rewriting SQL.

| View | Source | Key Measures |
|---|---|---|
| `order_performance` | `silver.guest_order` | Total Orders, Total Revenue, AOV, Fulfilled/Cancelled Orders, SOS Breach Rate |
| `loyalty_performance` | `silver.loyalty_transaction` | Unique Members, Points Earned, Points Redeemed, Redemption Value |
| `inventory_waste` | `silver.waste_log` | Total Waste Qty, Total Waste Cost, Waste Events |
| `staff_hours` | `silver.time_punch` | Total Hours Worked, Total Shifts, Unique Employees, Avg Hours/Shift |

---

## Full Rebuild from Scratch

```bash
# 1. Deploy bundle
databricks bundle deploy --target dev -p DEFAULT

# 2. Run setup_job (fully automated, ~15-25 min)
databricks jobs run-now <setup_job_id> -p DEFAULT
```

setup_job handles everything: catalog check → schemas → staging tables → ref seed →
backfill (staging data) → pipeline full_refresh (silver/gold) → metric views → Genie Space →
governance (tags, masks, row filters) → monitoring (4 monitors) → unpause generator.

### Re-running after partial failure

The job is designed to be re-run safely:
- Staging tables are `IF NOT EXISTS` — no data loss
- Backfill is incremental — picks up from last staging timestamp
- `create_genie_space` checks for existing space and skips if found
- `start_pipeline` waits for active updates before starting new ones

---

## Verifying After Deployment

```sql
-- Check silver data exists
SELECT COUNT(*) FROM jmrdemo.silver.guest_order;
SELECT COUNT(*) FROM jmrdemo.silver.waste_log;

-- Check metric views (UC metric views — slice by any dimension)
SELECT * FROM jmrdemo.metrics.order_performance LIMIT 5;
SELECT * FROM jmrdemo.metrics.loyalty_performance LIMIT 5;
SELECT * FROM jmrdemo.metrics.inventory_waste LIMIT 5;
SELECT * FROM jmrdemo.metrics.staff_hours LIMIT 5;

-- Check generator distributions
SELECT waste_category, COUNT(*) FROM jmrdemo.silver.waste_log GROUP BY 1 ORDER BY 2 DESC;
-- Expected: overproduction ~50%, spoilage ~25%, theft/expired ~10% each, damaged ~5%

SELECT item_status, COUNT(*) FROM jmrdemo.silver.order_item GROUP BY 1;
-- Expected: fulfilled ~87%, cancelled ~12%, refunded ~1%

-- Check catalog metadata
DESCRIBE TABLE EXTENDED jmrdemo.silver.guest_order;
-- Expected: Comment column shows descriptions on columns

-- Genie Space: verify via UI at /genie/spaces
-- or: GET /api/2.0/genie/spaces  (look for "QSR Synthetic Data — jmrdemo")

-- Check class.* tags on PII columns (expect 8 rows)
SELECT table_name, column_name, tag_name
FROM system.information_schema.column_tags
WHERE catalog_name = 'jmrdemo' AND tag_name LIKE 'class.%'
ORDER BY table_name, column_name;

-- Check column masks are bound (expect 4 rows: email/phone on guest_events + guest_profile)
SELECT table_schema, table_name, column_name, mask_name
FROM system.information_schema.column_masks
WHERE table_catalog = 'jmrdemo'
ORDER BY table_name, column_name;

-- Confirm masking fires (values should be redacted, e.g. x*****@example.com)
SELECT email, phone FROM jmrdemo.synth_staging.guest_events LIMIT 3;

-- Check monitors via SDK
-- python3 -c "
-- from databricks.sdk import WorkspaceClient
-- w = WorkspaceClient(profile='DEFAULT')
-- for t in ['jmrdemo.synth_staging.order_events','jmrdemo.synth_staging.inventory_events',
--           'jmrdemo.synth_staging.loyalty_events','jmrdemo.synth_silver.guest_order']:
--     m = w.quality_monitors.get(table_name=t)
--     print(t.split('.')[-1], m.status, m.schedule.quartz_cron_expression if m.schedule else 'no-sched')
-- "
```

---

## Test Suite

75 tests, all passing:

```bash
cd /path/to/synthData
pytest tests/ -v
```

| File | Tests | Coverage |
|---|---|---|
| `tests/test_orders.py` | 15 | Orders, items, discounts, waste flags, AOV variance |
| `tests/test_guest_loyalty_workforce.py` | 8 | Churn, loyalty redeem txns, shifts |
| `tests/test_inventory.py` | 4 | Inventory events, waste categories |
| `tests/test_runner.py` | 8 | Runner/tick integration |
| `tests/test_seeder.py` | 5 | Seeder functions incl. item_price_data |
| `tests/test_smoke.py` | 5 | End-to-end smoke (24 ticks) |
| `tests/test_menu_catalog.py` | 4 | Menu catalog |
| `tests/test_us_locations.py` | 4 | Unit generation incl. market_price_index |

---

## Open Issues

### ABAC masking not yet wired (DLT compatibility workaround pending)

`apply_governance.py` currently uses per-table `SET MASK` (Step 5). The intended architecture is tag-driven ABAC (`CREATE POLICY ... MATCH COLUMNS (has_tag('class.email_address'))`), but Databricks rejects ABAC catalog-level policies on DLT-managed tables during `full_refresh` with `ABAC_POLICIES_NOT_SUPPORTED`.

**Workaround path (not yet implemented):** Drop ABAC policies in `start_pipeline_notebook.py` before triggering the full_refresh, then `apply_governance` recreates them after. This allows ABAC to work end-to-end while keeping the pipeline compatible. See branch `feat/abac-data-discovery` for the ABAC implementation — it just needs the drop-before-refresh step in `start_pipeline_notebook.py`.

### Data classification auto-tagging not active

`configure_monitoring.py` passes `MonitorDataClassificationConfig(enabled=True)` to all 4 monitors, but the workspace returns `enabled=False` — this workspace tier does not support the Data Classification feature. The `class.*` tags on PII columns are applied deterministically by `apply_governance.py` Step 3 (hardcoded DDL), not by any automated scanner. If the workspace is upgraded, the monitors will start writing `class.*` tags automatically on each 12h refresh.

---

## Known Gotchas

| Issue | Root Cause | Fix Applied |
|---|---|---|
| Column comments and PK/FK constraints lost on every pipeline refresh | DLT owns metadata for tables it materializes — externally-applied `COMMENT ON TABLE`, `ALTER COLUMN COMMENT`, and `ADD CONSTRAINT` are reset on each update | Declare `comment=` + `schema=` (with inline column `COMMENT` and `CONSTRAINT ... NOT ENFORCED`) directly in `@dp.table` decorators in `mvm_pipeline.py`; deleted `apply_catalog_metadata.py` |
| DLT silver flows all fail after setup re-run | `CREATE OR REPLACE TABLE` on staging tables changes Delta table ID, breaking streaming checkpoints | Changed to `CREATE TABLE IF NOT EXISTS` in `setup_notebook.py` |
| `start_pipeline` fails if generator triggers pipeline mid-run | Race condition — pipeline already has active update | `start_pipeline_notebook.py` waits for active update, falls back to `full_refresh` on failure |
| `DELTA_METADATA_MISMATCH` on ref.unit | Phase 2.5 added `market_price_index` column; write lacked `overwriteSchema` | Added `.option("overwriteSchema", "true")` in `seeder.py` |
| `COUNTIF` not available | Runtime doesn't support `COUNTIF` | Replaced with `COUNT(CASE WHEN ... THEN 1 END)` throughout |
| `browserHostName()` throws `None.get` on serverless | Unavailable in serverless cluster context | Use `WorkspaceClient().config.host` instead |
| Genie Space API returns 401 | `w.config.token` is `None` on serverless (OAuth credentials) | Use `ctx.apiToken().get()` for bearer token |
| Genie Space API returns 400 missing `warehouse_id` | Field required by API | Look up warehouse via SDK and add to payload |
| PK duplication 83–87% across all order-domain tables | Module-level global counters (`_order_counter`, `_inv_counter`, etc.) reset to 0 on every Databricks serverless notebook execution (fresh Python process per job run) | Replaced all counters with `make_id(*parts)` in `src/generator/id_utils.py` — deterministic SHA-256 hash keyed on `(domain_prefix, unit_id, tick_ts, seq/sku)`, produces a stable 56-bit int that is idempotent across re-runs |
| `silver.guest_profile` had ~19 duplicate `guest_profile_id` rows | `generate_guest_churn()` emits a `guest_profile` event reusing the original guest's ID as a churn/deactivation record | **Fixed.** Migrated to CDC via `dp.create_streaming_table` + `dp.create_auto_cdc_flow` (SCD Type 1, keyed on `guest_profile_id`). |
| `configure_monitoring` fails with `TABLE SELECT required` | The `[WARN]` about table-level SELECT was a red herring. The real gap was the compute service principal lacking USE CATALOG and USE SCHEMA privileges — table-level grants are ignored when the principal can't even see the catalog/schema. Fix: grant USE CATALOG and USE SCHEMA to the service principal (or `account users`) at the catalog and schema level, not just the table. | Permissions granted at catalog and schema level; monitors now create successfully with status MONITOR_STATUS_ACTIVE. |
| `start_pipeline` fails with "FAILED unexpectedly" even though all flows completed | Transient platform-level DLT update coordinator error — all 19 flows completed but the update status reported FAILED | Added `max_attempts=2` retry loop to `run_full_refresh()` in `start_pipeline_notebook.py`. |
| Catalog-level ABAC policies (`CREATE POLICY ON CATALOG`) cause DLT full_refresh to fail | DLT rejects `full_refresh` on tables that have catalog-level ABAC policies bound: `ABAC_POLICIES_NOT_SUPPORTED`. Both staging and silver tables are DLT-managed, so `ON CATALOG` ABAC covers them. | Reverted Step 5 of `apply_governance.py` to per-table `SET MASK`. ABAC is the intended end state — drop-before-refresh pattern in `start_pipeline_notebook.py` is the remaining work. See Open Issues. |
| `class.*` UC tag values must be empty string, not `'true'` | The `jmrdemo` workspace has a UC tag policy governing the `class.*` namespace with an empty allowed-values list. Writing `SET TAGS ('class.email_address' = 'true')` fails. | All `class.*` tag values in `apply_governance.py` Step 3 use `''` (empty string). `financial` and `supply_chain` tags keep `'true'` — different policy. |
| `CREATE POLICY IF NOT EXISTS` / `DROP POLICY IF EXISTS` not supported | Syntax not available in this UC version. | Guard existence with `SHOW POLICIES ON CATALOG {c}` → `.filter(...)` → `.count()` before issuing `CREATE POLICY` or `DROP POLICY`. |

---

## Destroy / Data Survival

**`databricks bundle destroy`** removes DAB-managed *resources* (job definitions, pipeline definition) but does **not** drop Unity Catalog tables. The Delta data in `jmrdemo.staging.*`, `jmrdemo.silver.*`, and `jmrdemo.ref.*` survives.

**The QSR Destroy job** tears down non-DAB objects (Genie Space, metric views, schemas) but is not written to DROP the data tables themselves.

**To fully wipe data**, you must explicitly run `DROP TABLE` (or `DROP SCHEMA ... CASCADE`) on the catalog objects after running the destroy job and `bundle destroy`.
