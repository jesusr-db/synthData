# QSR Synthetic Data Generator — Agent Handoff

## Project State: Deployed and Running

**Workspace:** `adb-7405605519549535.15.azuredatabricks.net` (Azure)  
**Catalog:** `jmrdemo`  
**DAB target:** `dev` (profile: `DEFAULT`)  
**Bundle root:** `/Users/jesus.rodriguez/Documents/ItsAVibe/gitrepos_FY27/synthData`  
**GitHub:** `https://github.com/jesusr-db/synthData`

## What's Built

A fully automated QSR synthetic data generator (Domino's-style, 250 units):

- **Bronze layer:** Python generator writes to 5 staging Delta tables in `jmrdemo.staging`
- **Silver layer:** DLT pipeline reads staging via `readStream` and produces 14 Silver tables in `jmrdemo.silver`
- **Gold layer:** 4 Gold aggregate tables in `jmrdemo.silver` (co-located with silver in DLT)
- **Metrics layer:** 5 UC metric views in `jmrdemo.metrics` (created by setup_job after pipeline first run)
- **Catalog metadata:** UC table/column comments + informational PK/FK constraints on all silver tables
- **Genie Space:** Pre-configured with domain instructions, 10 seed questions, all silver + metrics tables

## Deployed Resources

| Resource | Type | Status |
|---|---|---|
| QSR MVM Pipeline [dev] | DLT Pipeline (triggered) | Triggered per generator run |
| QSR Setup [dev] | Job (7 tasks) | Run to rebuild from scratch |
| QSR Generator Live [dev] | Job (every-minute cron) | UNPAUSED — running |
| QSR Destroy [dev] | Job (teardown) | Ready |

All resources tagged: `project: qsr-synth-data-generator`

## Setup Job Task Graph

```
setup (seeds ref tables incl. ref.item_price)
  ├── start_pipeline (full_refresh=true) → silver + gold tables
  │     └── apply_catalog_metadata (comments + PK/FK)
  │           └── create_metric_views (5 views in metrics schema)
  │                 └── create_genie_space (REST API)
  │                       └── unpause_generator ←┐
  └── backfill (1-month history, all fixes applied) ──────────┘
```

`unpause_generator` waits for both `backfill` AND `create_genie_space`.

## Key Files

```
databricks.yml                              # bundle config, catalog_name=jmrdemo
src/generator/main.py                       # notebook entrypoint (backfill + live modes)
src/generator/runner.py                     # GeneratorConfig, backfill_ticks, live_tick
src/generator/domains/orders.py             # order + item + payment generation (all fixes applied)
src/generator/domains/inventory.py          # inventory events (weighted waste categories)
src/generator/domains/loyalty.py            # loyalty earn + redeem transactions
src/generator/domains/guest.py              # guest profiles + churn events
src/generator/reference/us_locations.py     # unit seeder (includes market_price_index)
src/generator/reference/seeder.py           # seed_all() including ref.item_price
src/generator/entity_registry.py           # FK registry (unit_price_index, item_price_multiplier)
src/pipeline/mvm_pipeline.py               # DLT pipeline (14 silver + 4 gold tables)
src/setup/setup_notebook.py                # Steps 1-4: schemas + staging tables + ref seed
src/setup/apply_catalog_metadata.py        # UC comments + PK/FK constraints
src/setup/create_metric_views.py           # 5 metric views over silver tables
src/setup/create_genie_space.py            # Genie Space via REST API
src/setup/destroy_notebook.py             # teardown logic
src/setup/unpause_generator_notebook.py    # unpauses generator job after setup
resources/pipeline.yml                    # DLT pipeline DAB resource
resources/setup_job.yml                   # 7-task setup job
resources/generator_job.yml               # live generator job (UNPAUSED)
resources/destroy_job.yml                 # destroy job
```

## Staging Tables

| Table | Event types written |
|---|---|
| `jmrdemo.staging.order_events` | guest_order, order_item, payment, status_event, delivery_order |
| `jmrdemo.staging.inventory_events` | on_hand_balance, waste_log, replenishment_order, receiving_order |
| `jmrdemo.staging.guest_events` | guest_profile |
| `jmrdemo.staging.loyalty_events` | loyalty_transaction, reward_redemption |
| `jmrdemo.staging.workforce_events` | shift, time_punch |

## Reference Tables

| Table | Contents |
|---|---|
| `jmrdemo.ref.unit` | 250 units with `unit_volume_bias`, `market_price_index` |
| `jmrdemo.ref.menu_item` | Menu catalog by daypart |
| `jmrdemo.ref.recipe_ingredient` | Bill of materials per menu item |
| `jmrdemo.ref.financial_period` | Monthly periods with fiscal quarter |
| `jmrdemo.ref.item_price` | Per (menu_item, period) price multiplier — quarterly drift ±3-6% |
| `jmrdemo.ref.franchisee` | Franchisee entities |
| `jmrdemo.ref.supplier` | 6 suppliers |

## Metric Views

| View | Description |
|---|---|
| `jmrdemo.metrics.unit_daily_summary` | Orders, revenue, AOV, SOS breach % per unit per day |
| `jmrdemo.metrics.loyalty_tier_distribution` | Member counts, earn/redeem transactions by tier and month |
| `jmrdemo.metrics.inventory_waste_rate` | Waste qty/cost as % of usage by unit/week/SKU |
| `jmrdemo.metrics.staff_utilization` | Scheduled vs actual hours, no-show rate per unit per day |
| `jmrdemo.metrics.channel_mix_trend` | Order share and revenue share by channel per unit per week |

---

## Deploying Both Feature Branches

### Feature branches

| Branch | Status | Contents |
|---|---|---|
| `feat/phase-25-generator-realism` | Ready to merge | 7 generator data quality fixes (71 tests) |
| `feat/phase-2-catalog-enrichment` | Ready to merge | Metadata + metric views + Genie Space |

### Deploy and rebuild (recommended — do both together)

Phase 2.5 adds `ref.item_price` to the workspace reference tables and `market_price_index`
to `ref.unit`. The generator's `EntityRegistry.from_spark()` now loads `ref.item_price` at
startup — if this table doesn't exist the generator job will crash. A full rebuild is required.

```bash
# 1. Merge both branches
git checkout main
git merge feat/phase-25-generator-realism
git merge feat/phase-2-catalog-enrichment

# 2. Deploy bundle
databricks bundle deploy --target dev

# 3. Destroy existing silver data (drops staging + silver + gold rows)
databricks jobs run-now --job-id <destroy_job_id>
# Wait for completion (~2 min)

# 4. Run setup_job — seeds ref tables (including ref.item_price),
#    starts pipeline, applies metadata, creates metric views + Genie Space,
#    runs full backfill, then unpauses generator
databricks jobs run-now --job-id <setup_job_id>
# Takes ~15-25 min (backfill is the long pole)
```

### Phase 2 only (if skipping Phase 2.5)

Phase 2 is fully additive — no rebuild needed. Run only the new tasks:

```bash
git checkout main && git merge feat/phase-2-catalog-enrichment
databricks bundle deploy --target dev

# Run individual tasks (silver tables must already exist)
databricks jobs run-now --job-id <setup_job_id> \
  --only apply_catalog_metadata,create_metric_views,create_genie_space
```

Or just run the full setup_job — it is idempotent.

---

## Verifying After Deployment

```sql
-- Phase 2.5: check generator distributions are fixed
SELECT waste_category, COUNT(*) FROM jmrdemo.silver.waste_log GROUP BY 1 ORDER BY 2 DESC;
-- Expected: overproduction ~50%, spoilage ~25%, theft/expired ~10% each, damaged ~5%

SELECT item_status, COUNT(*) FROM jmrdemo.silver.order_item GROUP BY 1;
-- Expected: fulfilled ~87%, cancelled ~12%, refunded ~1%

SELECT COUNT(*) FROM jmrdemo.ref.item_price;
-- Expected: > 0 (menu_items × financial_periods)

SELECT MIN(unit_price), MAX(unit_price), AVG(unit_price)
FROM jmrdemo.silver.order_item;
-- Expected: meaningful spread, not a single flat value

-- Phase 2: check metadata
DESCRIBE TABLE EXTENDED jmrdemo.silver.guest_order;
-- Expected: Comment column shows descriptions on guest_order_id, channel, etc.

SELECT * FROM jmrdemo.metrics.unit_daily_summary LIMIT 5;
SELECT * FROM jmrdemo.metrics.channel_mix_trend LIMIT 5;
SELECT * FROM jmrdemo.metrics.loyalty_tier_distribution LIMIT 5;
SELECT * FROM jmrdemo.metrics.inventory_waste_rate LIMIT 5;
SELECT * FROM jmrdemo.metrics.staff_utilization LIMIT 5;
-- Expected: all 5 return rows

-- Genie Space: verify via UI at workspace /genie/spaces
-- or query via REST:
-- GET /api/2.0/genie/spaces  (look for "QSR Synthetic Data — jmrdemo")
```

---

## Full Rebuild from Scratch (zero state)

If starting from a fresh workspace or after a full teardown:

```bash
# 1. Deploy bundle (creates all DAB resources)
databricks bundle deploy --target dev

# 2. Run setup_job — fully automated end-to-end
databricks jobs run-now --job-id <setup_job_id>
```

setup_job handles: catalog verification → schemas → staging tables → ref seed →
pipeline (silver/gold) → catalog metadata → metric views → Genie Space → backfill → unpause.

---

## Test Suite

71 tests, all passing:

```bash
cd /Users/jesus.rodriguez/Documents/ItsAVibe/gitrepos_FY27/synthData
pytest tests/ -v
```

| File | Tests |
|---|---|
| `tests/test_orders.py` | 15 — orders, items, discounts, waste flags, AOV variance |
| `tests/test_guest_loyalty_workforce.py` | 8 — churn, loyalty redeem txns, shifts |
| `tests/test_inventory.py` | 4 — inventory events, waste categories |
| `tests/test_runner.py` | 8 — runner/tick integration |
| `tests/test_seeder.py` | 5 — seeder functions incl. item_price_data |
| `tests/test_smoke.py` | 5 — end-to-end smoke |
| `tests/test_menu_catalog.py` | 4 — menu catalog |
| `tests/test_us_locations.py` | 4 — unit generation incl. market_price_index |
