# QSR Synthetic Data Generator — Agent Handoff

## Project State: Deployed and Running

**Workspace:** `adb-7405605519549535.15.azuredatabricks.net` (Azure)  
**Catalog:** `jmrdemo`  
**DAB target:** `dev` (profile: `DEFAULT`)  
**Bundle root:** `/Users/jesus.rodriguez/Documents/ItsAVibe/gitrepos_FY27/synthData`

## What's Built

A fully automated QSR synthetic data generator (Domino's-style, 250 units):

- **Bronze layer:** Python SCM generator writes to 5 staging Delta tables in `jmrdemo.staging`
- **Silver/Gold layer:** DLT pipeline reads staging via `readStream` and writes 15 Silver + 4 Gold tables into `jmrdemo.silver`
- **Metrics layer:** UC Metric Views in `jmrdemo.metrics` on top of Gold (created after pipeline first run)

## Deployed Resources

| Resource | Type | Status |
|---|---|---|
| QSR MVM Pipeline [dev] | DLT Pipeline (continuous, PREVIEW) | Started by setup job |
| QSR Setup [dev] | Job (3 tasks: setup → pipeline + backfill) | Last run: 2026-05-18 |
| QSR Generator Live [dev] | Job (every-minute cron) | PAUSED — unpause after backfill |
| QSR Destroy [dev] | Job (teardown) | Ready |

All resources tagged: `project: qsr-synth-data-generator`

## Setup Job Task Graph

```
setup (notebook)
  ├── start_pipeline (pipeline_task) — starts DLT continuous pipeline
  └── backfill (notebook_task, mode=backfill) — generates 12 months of history
```

## Key Files

```
databricks.yml                          # bundle config, catalog_name=jmrdemo
conf/params.yml                         # num_units=250, backfill_months=12
src/generator/main.py                   # notebook entrypoint (backfill + live modes)
src/generator/runner.py                 # GeneratorConfig, backfill_ticks, live_tick
src/pipeline/mvm_pipeline.py            # DLT pipeline (411 lines)
src/setup/setup_notebook.py             # setup logic
src/setup/destroy_notebook.py           # teardown logic
resources/pipeline.yml                  # DLT pipeline DAB resource
resources/setup_job.yml                 # setup job with 3-task graph
resources/generator_job.yml             # live generator job (PAUSED)
resources/destroy_job.yml               # destroy job
```

## Staging Table Names (important — not stg_* prefix)

| Schema | Table | Event types |
|---|---|---|
| jmrdemo.staging | order_events | guest_order, order_item, payment, status_event, delivery_order |
| jmrdemo.staging | inventory_events | on_hand_balance, waste_log, replenishment_order, receiving_order |
| jmrdemo.staging | guest_events | guest_profile |
| jmrdemo.staging | loyalty_events | loyalty_transaction, reward_redemption |
| jmrdemo.staging | workforce_events | shift, time_punch |

## Next Task: Convert Clusters to Serverless

Where possible, update DAB resource YAML files to use serverless compute:

### 1. `resources/pipeline.yml` — DLT pipeline
Replace the `clusters` block with serverless:
```yaml
# Remove:
clusters:
  - label: default
    num_workers: 2

# Replace with:
serverless: true
```
DLT serverless is GA on Azure. PREVIEW channel is compatible.

### 2. `resources/setup_job.yml` — Setup job
Add `queue` and serverless flag to each task or at job level. For notebook tasks, use:
```yaml
# At job level or per-task:
environments:
  - environment_key: default
    spec:
      client: "1"
```
Or use `job_clusters` with a serverless policy, or simply set `new_cluster` with serverless node type.

Actually for Databricks jobs on Azure, serverless jobs use:
```yaml
tasks:
  - task_key: setup
    environment_key: default
    notebook_task: ...

environments:
  - environment_key: default
    spec:
      client: "1"
```

### 3. `resources/generator_job.yml` — Generator job
Same pattern as setup_job.

### 4. `resources/destroy_job.yml` — Destroy job
Same pattern.

### Notes
- The `backfill` task in setup_job runs the generator for 12 months — this may be long-running on serverless; consider whether a dedicated cluster is better for the backfill task specifically.
- `pipeline_task` in setup_job has no cluster config (inherits from the pipeline resource itself).
- After updating, run `databricks bundle validate` and `databricks bundle deploy --target dev` to apply.

## After Backfill Completes

1. Unpause the generator job: `databricks jobs update <job_id> --json '{"schedule": {"pause_status": "UNPAUSED"}}'`  
   Or via the UI at the job page.
2. Metric views in `jmrdemo.metrics` will be created on next setup job run (Gold tables now exist).

## Test Suite

55 tests, all passing:
```bash
cd /Users/jesus.rodriguez/Documents/ItsAVibe/gitrepos_FY27/synthData
pytest tests/ -v
```
