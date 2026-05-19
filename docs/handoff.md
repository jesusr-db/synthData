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

- **Staging layer:** Python generator writes to 5 Delta tables in `jmrdemo.staging`
- **Silver layer:** Lakeflow Declarative Pipeline reads staging via `readStream`, produces 14 Silver tables in `jmrdemo.silver` — table/column comments, PK, and FK constraints declared inline in `@dp.table` decorators so they survive every pipeline refresh
- **Gold layer:** 4 Gold aggregate tables in `jmrdemo.silver` (co-located with silver in DLT)
- **Metrics layer:** 4 Unity Catalog metric views in `jmrdemo.metrics` (WITH METRICS LANGUAGE YAML — reusable measures + dimensions)
- **Genie Space:** Pre-configured with domain instructions, 10 seed questions, all silver + metrics tables

---

## Deployed Resources

| Resource | Type | Notes |
|---|---|---|
| QSR MVM Pipeline [dev] | DLT Pipeline (triggered) | Triggered per generator run |
| QSR Setup [dev] | Job (6 tasks) | Run to rebuild from scratch |
| QSR Generator Live [dev] | Job (every-minute cron) | UNPAUSED — running live |
| QSR Destroy [dev] | Job (teardown) | Ready — tears down non-DAB objects |

All resources tagged: `project: qsr-synth-data-generator`

---

## Setup Job Task Graph

```
setup (seeds ref tables incl. ref.item_price)
  ├── start_pipeline (full_refresh if checkpoint broken) → silver + gold tables
  │     └── create_metric_views (4 UC metric views in metrics schema)
  │           └── create_genie_space (REST API — requires warehouse_id)
  │                 └── unpause_generator ←┐
  └── backfill (1-month history, incremental from last staging ts) ──┘
```

`unpause_generator` waits for both `backfill` AND `create_genie_space`.

### Important behaviors

- **`setup_notebook.py`** uses `CREATE TABLE IF NOT EXISTS` for all 5 staging tables — never drops them. This preserves Delta table IDs so DLT streaming checkpoints survive re-runs.
- **`start_pipeline_notebook.py`** waits for any active pipeline update; falls back to `full_refresh=True` if the update fails (clears broken streaming checkpoint state).
- **`backfill`** is incremental — reads `MAX(event_ts)` across staging tables and resumes from the next full hour. Safe to re-run.

---

## Key Files

```
databricks.yml                              # bundle config, catalog_name=jmrdemo
src/generator/main.py                       # notebook entrypoint (backfill + live modes)
src/generator/runner.py                     # GeneratorConfig, backfill_ticks, live_tick
src/generator/domains/orders.py             # order + item + payment generation
src/generator/domains/inventory.py          # inventory events (weighted waste categories)
src/generator/domains/loyalty.py            # loyalty earn + redeem transactions
src/generator/domains/guest.py              # guest profiles + churn events
src/generator/reference/us_locations.py     # unit seeder (includes market_price_index)
src/generator/reference/seeder.py           # seed_all() — overwriteSchema=true for ref.unit
src/generator/entity_registry.py           # FK registry (unit_price_index, item_price_multiplier)
src/pipeline/mvm_pipeline.py               # Lakeflow Declarative Pipeline (14 silver + 4 gold tables; all metadata inline)
src/setup/setup_notebook.py                # schemas + staging tables (IF NOT EXISTS) + ref seed
src/setup/start_pipeline_notebook.py       # idempotent pipeline start with full_refresh fallback
src/setup/create_metric_views.py           # 4 UC metric views (WITH METRICS LANGUAGE YAML)
src/setup/create_genie_space.py            # Genie Space via REST API
src/setup/destroy_notebook.py             # teardown logic
src/setup/unpause_generator_notebook.py   # unpauses generator job after setup
resources/pipeline.yml                    # DLT pipeline DAB resource
resources/setup_job.yml                   # 7-task setup job
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
pipeline full_refresh (silver/gold) → metric views → Genie Space →
backfill → unpause generator.

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
```

---

## Test Suite

71 tests, all passing:

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
