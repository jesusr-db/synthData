# Schema Prefix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `schema_prefix` bundle variable (default `synth_`) so all schemas created by the setup job are named `synth_staging`, `synth_silver`, `synth_ref`, and `synth_metrics` instead of bare names.

**Architecture:** Thread a single `schema_prefix` string (e.g. `"synth_"`) through every layer: bundle variable → pipeline.yml target → setup_job.yml base_parameters → notebook widgets → Python widget reads → all SQL/table references. The DLT pipeline reads the prefix from a Spark conf key `pipeline.schema_prefix`. All notebooks read it via `dbutils.widgets.get("schema_prefix")` with a try/except fallback to `"synth_"`.

**Tech Stack:** Databricks Asset Bundles (YAML), Python notebooks, Lakeflow Declarative Pipelines (`pyspark.pipelines`)

---

## File Map

| File | Change |
|------|--------|
| `databricks.yml` | Add `schema_prefix` variable, default `synth_` |
| `resources/pipeline.yml` | `target: ${var.schema_prefix}silver` + add `pipeline.schema_prefix` conf |
| `resources/setup_job.yml` | Add `schema_prefix: ${var.schema_prefix}` to every notebook task that has `base_parameters` (except `unpause_generator`) |
| `src/setup/setup_notebook.py` | Widget read + prefix `staging`, `ref`, `metrics` schema names and table refs |
| `src/setup/create_metric_views.py` | Widget read + prefix `metrics` schema and `silver` source refs |
| `src/setup/create_genie_space.py` | Widget read + prefix `silver.*` and `metrics.*` table identifiers |
| `src/setup/destroy_notebook.py` | Widget read + prefix all schema names in DROP statements |
| `src/generator/main.py` | Widget read + prefix all `staging.*` table names in routing map |
| `src/generator/reference/seeder.py` | Add `schema_prefix` param to `seed_all()` + prefix `.ref.` table writes |

---

## Task 1: Bundle config — `databricks.yml`, `pipeline.yml`, `setup_job.yml`

**Files:**
- Modify: `databricks.yml`
- Modify: `resources/pipeline.yml`
- Modify: `resources/setup_job.yml`

No tests to write — pure YAML config.

- [ ] **Step 1.1: Add `schema_prefix` to `databricks.yml`**

Add after `start_dt_override`:

```yaml
  schema_prefix:
    default: "synth_"
    description: "Prefix for all Unity Catalog schemas (e.g. 'synth_' → synth_staging, synth_silver, synth_ref, synth_metrics). Use empty string for no prefix."
```

Full `variables` section after edit:

```yaml
variables:
  catalog_name:
    default: jmrdemo
  num_units:
    default: "250"
  backfill_months:
    default: "1"
  live_tick_seconds:
    default: "60"
    description: "Sub-tick granularity (seconds) within each hourly live run. 60 = one sub-tick per minute, matching per-minute historical cadence."
  base_orders_per_unit_per_hour:
    default: "18"
  start_dt_override:
    default: ""
    description: "Backfill window start (ISO datetime, e.g. 2026-05-19T00:00:00). Empty string = use staging MAX(event_ts) or backfill_months default."
  schema_prefix:
    default: "synth_"
    description: "Prefix for all Unity Catalog schemas (e.g. 'synth_' → synth_staging, synth_silver, synth_ref, synth_metrics). Use empty string for no prefix."
```

- [ ] **Step 1.2: Update `resources/pipeline.yml`**

Replace `target: silver` with `target: ${var.schema_prefix}silver` and add `pipeline.schema_prefix` to the `configuration:` block:

```yaml
resources:
  pipelines:
    mvm_pipeline:
      name: "QSR MVM Pipeline [${bundle.target}]"
      channel: PREVIEW
      continuous: false
      catalog: ${var.catalog_name}
      target: ${var.schema_prefix}silver
      libraries:
        - notebook:
            path: ../src/pipeline/mvm_pipeline.py
      serverless: true
      configuration:
        pipeline.catalog: ${var.catalog_name}
        pipeline.schema_prefix: ${var.schema_prefix}
      tags:
        project: qsr-synth-data-generator
```

- [ ] **Step 1.3: Update `resources/setup_job.yml` — add `schema_prefix` to every notebook task**

Add `schema_prefix: ${var.schema_prefix}` to the `base_parameters` of `setup`, `start_pipeline`, `create_metric_views`, `create_genie_space`, and `backfill` tasks. Leave `unpause_generator` alone (it has no `base_parameters`).

Full file after edit:

```yaml
resources:
  jobs:
    setup_job:
      name: "QSR Setup [${bundle.target}]"
      tags:
        project: qsr-synth-data-generator
      environments:
        - environment_key: generator
          spec:
            client: "1"
            dependencies:
              - faker>=20.0.0
      tasks:
        - task_key: setup
          notebook_task:
            notebook_path: ../src/setup/setup_notebook.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              num_units: ${var.num_units}
              schema_prefix: ${var.schema_prefix}

        - task_key: start_pipeline
          depends_on:
            - task_key: setup
          notebook_task:
            notebook_path: ../src/setup/start_pipeline_notebook.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              schema_prefix: ${var.schema_prefix}

        - task_key: create_metric_views
          depends_on:
            - task_key: start_pipeline
          notebook_task:
            notebook_path: ../src/setup/create_metric_views.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              schema_prefix: ${var.schema_prefix}

        - task_key: create_genie_space
          depends_on:
            - task_key: create_metric_views
          notebook_task:
            notebook_path: ../src/setup/create_genie_space.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              schema_prefix: ${var.schema_prefix}

        - task_key: backfill
          depends_on:
            - task_key: setup
          environment_key: generator
          notebook_task:
            notebook_path: ../src/generator/main.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              num_units: ${var.num_units}
              backfill_months: ${var.backfill_months}
              live_tick_seconds: ${var.live_tick_seconds}
              base_orders_per_unit_per_hour: ${var.base_orders_per_unit_per_hour}
              start_dt_override: ${var.start_dt_override}
              schema_prefix: ${var.schema_prefix}
              mode: backfill

        - task_key: unpause_generator
          depends_on:
            - task_key: backfill
            - task_key: create_genie_space
          notebook_task:
            notebook_path: ../src/setup/unpause_generator_notebook.py
            base_parameters:
              generator_job_id: ${resources.jobs.generator_job.id}
```

- [ ] **Step 1.4: Commit**

```bash
git add databricks.yml resources/pipeline.yml resources/setup_job.yml
git commit -m "feat: add schema_prefix bundle variable (default synth_) for all UC schema names"
```

---

## Task 2: Generator source code — `main.py`, `seeder.py`

**Files:**
- Modify: `src/generator/main.py`
- Modify: `src/generator/reference/seeder.py`

- [ ] **Step 2.1: Update `src/generator/main.py` — add widget and prefix staging table map**

Add `schema_prefix` widget after the `start_dt_override` widget (around line 26). Then prefix all `.staging.` strings in the `TABLE_MAP` dict.

Add widget (right after `start_dt_override = _widget("start_dt_override", "")`):

```python
schema_prefix       = _widget("schema_prefix", "synth_")
```

Replace the `TABLE_MAP` dict (around lines 43–60) — change every `f"{catalog_name}.staging.` to `f"{catalog_name}.{schema_prefix}staging.`:

```python
TABLE_MAP = {
    "guest_order":         f"{catalog_name}.{schema_prefix}staging.order_events",
    "order_item":          f"{catalog_name}.{schema_prefix}staging.order_events",
    "order_modifier":      f"{catalog_name}.{schema_prefix}staging.order_events",
    "payment":             f"{catalog_name}.{schema_prefix}staging.order_events",
    "status_event":        f"{catalog_name}.{schema_prefix}staging.order_events",
    "delivery_order":      f"{catalog_name}.{schema_prefix}staging.order_events",
    "on_hand_balance":     f"{catalog_name}.{schema_prefix}staging.inventory_events",
    "waste_log":           f"{catalog_name}.{schema_prefix}staging.inventory_events",
    "receiving_order":     f"{catalog_name}.{schema_prefix}staging.inventory_events",
    "replenishment_order": f"{catalog_name}.{schema_prefix}staging.inventory_events",
    "stock_transfer":      f"{catalog_name}.{schema_prefix}staging.inventory_events",
    "adjustment":          f"{catalog_name}.{schema_prefix}staging.inventory_events",
    "guest_profile":       f"{catalog_name}.{schema_prefix}staging.guest_events",
    "digital_account":     f"{catalog_name}.{schema_prefix}staging.guest_events",
    "loyalty_transaction": f"{catalog_name}.{schema_prefix}staging.loyalty_events",
    "reward_redemption":   f"{catalog_name}.{schema_prefix}staging.loyalty_events",
    "shift":               f"{catalog_name}.{schema_prefix}staging.workforce_events",
    "time_punch":          f"{catalog_name}.{schema_prefix}staging.workforce_events",
}
```

Also find the INFO log line that prints the mode and catalog (around line 34) and update it to include `schema_prefix`:

```python
print(f"[INFO] Generator config: catalog={catalog_name}, schema_prefix={schema_prefix}, units={num_units}, backfill_months={backfill_months}")
```

- [ ] **Step 2.2: Update `src/generator/reference/seeder.py` — add `schema_prefix` param**

Change the `seed_all` signature (line 71):

```python
def seed_all(spark, catalog: str, num_units: int = 250, backfill_months: int = 12, schema_prefix: str = "synth_"):
```

Change the docstring on line 72:

```python
    """Write all reference tables to {catalog}.{schema_prefix}ref.*"""
```

Replace all occurrences of `f"{catalog}.ref.` with `f"{catalog}.{schema_prefix}ref.` in the function body. There are two occurrences (lines ~79 and ~92):

Line ~79 (DataFrame write):
```python
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema_prefix}ref.{table}")
```

Line ~92 (stub table CREATE):
```python
            CREATE TABLE IF NOT EXISTS {catalog}.{schema_prefix}ref.{stub_table}
```

- [ ] **Step 2.3: Run tests to verify no regressions**

```bash
cd /Users/jesus.rodriguez/Documents/ItsAVibe/gitrepos_FY27/synthData
pytest tests/ -v
```

Expected: 75 passed. No tests directly test schema names, so this is a regression guard only.

- [ ] **Step 2.4: Commit**

```bash
git add src/generator/main.py src/generator/reference/seeder.py
git commit -m "feat: thread schema_prefix through generator and seeder for prefixed staging/ref schemas"
```

---

## Task 3: Notebooks — setup, metrics, genie, destroy, pipeline

**Files:**
- Modify: `src/setup/setup_notebook.py`
- Modify: `src/setup/create_metric_views.py`
- Modify: `src/setup/create_genie_space.py`
- Modify: `src/setup/destroy_notebook.py`
- Modify: `src/pipeline/mvm_pipeline.py`

The widget-reading pattern for all setup notebooks is:
```python
try:
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    schema_prefix = "synth_"
```

- [ ] **Step 3.1: Update `src/setup/setup_notebook.py`**

Add the widget block after the existing widget reads (after `num_units = ...`):

```python
try:
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    schema_prefix = "synth_"
```

Update the INFO log line to include schema_prefix:

```python
print(f"[INFO] Setup: catalog={catalog_name}, schema_prefix={schema_prefix}, num_units={num_units}")
```

Replace all 3 schema creation SQL statements:

```python
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_prefix}staging")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_prefix}ref")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_prefix}metrics")
print(f"[INFO] Schemas ready: {schema_prefix}staging, {schema_prefix}ref, {schema_prefix}metrics")
```

Replace ALL occurrences of `{catalog_name}.staging.` with `{catalog_name}.{schema_prefix}staging.` throughout the file (5 staging table CREATE statements, each has one occurrence in the SQL string and one in the print). Use replace_all=True for the f-string pattern.

Also update the `seed_all` call at the bottom to pass `schema_prefix`:

```python
seed_all(spark, catalog_name, num_units=num_units, schema_prefix=schema_prefix)
```

- [ ] **Step 3.2: Update `src/setup/create_metric_views.py`**

Read the current file to see the exact widget-reading pattern (it uses a short variable `c` for catalog).

Add `schema_prefix` widget after the catalog widget:

```python
try:
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    schema_prefix = "synth_"
```

Replace the schema creation:

```python
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {c}.{schema_prefix}metrics")
```

Replace all `{c}.metrics.` with `{c}.{schema_prefix}metrics.` (metric view CREATE statements).

Replace all `{c}.silver.` with `{c}.{schema_prefix}silver.` (source table references in the WITH METRICS YAML).

- [ ] **Step 3.3: Update `src/setup/create_genie_space.py`**

Add `schema_prefix` widget after the catalog widget (same try/except pattern).

Replace the table identifier list constructions. Currently:
```python
{"identifier": f"{catalog_name}.silver.{t}"} for t in sorted(SILVER_TABLES)
{"identifier": f"{catalog_name}.metrics.{v}"} for v in sorted(METRIC_VIEWS)
```

Replace with:
```python
{"identifier": f"{catalog_name}.{schema_prefix}silver.{t}"} for t in sorted(SILVER_TABLES)
{"identifier": f"{catalog_name}.{schema_prefix}metrics.{v}"} for v in sorted(METRIC_VIEWS)
```

- [ ] **Step 3.4: Update `src/setup/destroy_notebook.py`**

Add `schema_prefix` widget after the catalog widget:

```python
try:
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    schema_prefix = "synth_"

print(f"[INFO] Destroy: catalog={catalog_name}, schema_prefix={schema_prefix}")
```

Replace the metrics view DROP loop — change `{catalog_name}.metrics.` to `{catalog_name}.{schema_prefix}metrics.`:

```python
for view_name in METRIC_VIEWS:
    spark.sql(f"DROP VIEW IF EXISTS {catalog_name}.{schema_prefix}metrics.{view_name}")
    print(f"[INFO] Dropped view: {catalog_name}.{schema_prefix}metrics.{view_name}")
```

Replace the metrics schema DROP:

```python
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_prefix}metrics CASCADE")
print(f"[INFO] Dropped schema: {catalog_name}.{schema_prefix}metrics")
```

Replace the ref table DROP loop:

```python
for table in REF_TABLES:
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_prefix}ref.{table}")
    print(f"[INFO] Dropped table: {catalog_name}.{schema_prefix}ref.{table}")
```

Replace the ref schema DROP:

```python
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_prefix}ref CASCADE")
print(f"[INFO] Dropped schema: {catalog_name}.{schema_prefix}ref")
```

Update the final print:

```python
print(f"[INFO] Destroy complete. {schema_prefix}staging schema preserved. Run `databricks bundle destroy` to remove DAB-managed resources.")
```

- [ ] **Step 3.5: Update `src/pipeline/mvm_pipeline.py`**

The pipeline reads its configuration via `spark.conf.get()`. After the existing `catalog = spark.conf.get(...)` line at the top, add:

```python
schema_prefix = spark.conf.get("pipeline.schema_prefix", "synth_")
```

Replace ALL occurrences of `f"{catalog}.staging.` with `f"{catalog}.{schema_prefix}staging.` throughout the file. There are multiple `readStream.table(f"{catalog}.staging.*")` calls — every domain's stream source. Use replace_all semantics carefully since the pattern appears in many functions.

The exact string to replace (appears ~10 times across all domain functions):
- Find: `spark.readStream.table(f"{catalog}.staging.order_events")`  → `spark.readStream.table(f"{catalog}.{schema_prefix}staging.order_events")`
- Find: `spark.readStream.table(f"{catalog}.staging.inventory_events")` → `spark.readStream.table(f"{catalog}.{schema_prefix}staging.inventory_events")`
- Find: `spark.readStream.table(f"{catalog}.staging.guest_events")` → `spark.readStream.table(f"{catalog}.{schema_prefix}staging.guest_events")`
- Find: `spark.readStream.table(f"{catalog}.staging.loyalty_events")` → `spark.readStream.table(f"{catalog}.{schema_prefix}staging.loyalty_events")`
- Find: `spark.readStream.table(f"{catalog}.staging.workforce_events")` → `spark.readStream.table(f"{catalog}.{schema_prefix}staging.workforce_events")`

- [ ] **Step 3.6: Run tests to verify no regressions**

```bash
pytest tests/ -v
```

Expected: 75 passed.

- [ ] **Step 3.7: Commit**

```bash
git add src/setup/setup_notebook.py src/setup/create_metric_views.py src/setup/create_genie_space.py src/setup/destroy_notebook.py src/pipeline/mvm_pipeline.py
git commit -m "feat: apply schema_prefix to all notebooks and DLT pipeline staging reads"
```

---

## Self-Review

**Spec coverage:**
- ✅ `synth_staging`, `synth_silver`, `synth_ref`, `synth_metrics` — achieved via `schema_prefix = "synth_"` default in all layers
- ✅ Configurable at deploy time via `databricks.yml` variable — Task 1
- ✅ DLT pipeline target uses prefix — Task 1 Step 1.2 (`target: ${var.schema_prefix}silver`)
- ✅ DLT pipeline staging reads use prefix — Task 3 Step 3.5 (`pipeline.schema_prefix` conf)
- ✅ Backfill/live generator uses prefix — Task 2 Step 2.1
- ✅ Ref tables use prefix — Task 2 Step 2.2
- ✅ Metric views use prefix — Task 3 Step 3.2
- ✅ Genie Space table identifiers use prefix — Task 3 Step 3.3
- ✅ Destroy job uses prefix — Task 3 Step 3.4
- ✅ Setup notebook creates prefixed schemas and tables — Task 3 Step 3.1
- ✅ `seed_all()` updated with `schema_prefix` param — Task 2 Step 2.2 (callers updated in Task 3 Step 3.1)

**Placeholder scan:** None. All steps include exact code.

**Type consistency:**
- `schema_prefix` is a `str` throughout — consistent
- Default `"synth_"` used in all fallback values — consistent
- `f"{catalog}.{schema_prefix}staging."` pattern used identically in `main.py` and `mvm_pipeline.py` — consistent
