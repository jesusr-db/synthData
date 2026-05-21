# Gotchas

Non-obvious platform behaviors, sharp edges, and workarounds organized by subsystem.

---

## DAB / Bundle

**`databricks bundle destroy` does not drop Unity Catalog tables.**
`bundle destroy` removes the DAB-managed resource definitions (job configs, pipeline config) from the workspace but leaves all Delta tables and schemas intact in the catalog. To fully wipe data you must run the destroy_job first (drops non-DAB objects), then `bundle destroy`, then explicitly `DROP SCHEMA ... CASCADE` on `{prefix}staging`, `{prefix}silver`, and `{prefix}ref`.

**Bundle variables are strings — cast explicitly in notebooks.**
All bundle variables arrive as strings in notebook widgets (e.g. `num_units` comes in as `"250"`). The notebooks cast them: `num_units = int(dbutils.widgets.get("num_units"))`. Adding a new numeric variable without casting causes silent type errors downstream.

**`pipeline.catalog` and `pipeline.schema_prefix` must be read from `spark.conf`, not widgets.**
DLT notebooks do not have access to `dbutils.widgets`. Pipeline-level config is injected via `spark.conf.get("pipeline.catalog")` and `spark.conf.get("pipeline.schema_prefix")` — these are declared in `resources/pipeline.yml` under `configuration:`.

---

## Lakeflow Declarative Pipelines (DLT)

**Column comments, PK/FK constraints, and table descriptions are reset on every pipeline update.**
DLT owns the metadata for tables it materializes. Any externally-applied `COMMENT ON TABLE`, `ALTER COLUMN COMMENT`, or `ADD CONSTRAINT` is overwritten each time the pipeline runs. The fix: declare `comment=` and `schema=` (with inline column `COMMENT` and `CONSTRAINT ... NOT ENFORCED`) directly in `@dp.table` decorators. The `apply_governance.py` notebook still sets comments on staging and ref tables, but silver table metadata must live in the pipeline file.

**`CREATE OR REPLACE TABLE` on staging tables breaks DLT streaming checkpoints.**
Streaming tables maintain internal state keyed on the Delta table ID. If `setup_notebook.py` used `CREATE OR REPLACE TABLE`, every re-run would generate a new table ID and invalidate all downstream streaming checkpoints. All staging tables use `CREATE TABLE IF NOT EXISTS` to preserve the ID across re-runs.

**CDC tables (`dp.create_auto_cdc_flow`) require the join in the source view, not the target.**
`guest_profile` is a streaming table populated by `dp.create_auto_cdc_flow` from the `guest_profile_changes` view. There is no `@dp.table` decorator to add columns to. To add `franchisee_id`, the broadcast join must go into the `@dp.view(name="guest_profile_changes")` function, and the column must be declared in `dp.create_streaming_table(schema=...)`.

**`broadcast` is a separate import from `functions as F`.**
The pipeline uses `from pyspark.sql import functions as F`. Adding a broadcast join requires either a second import `from pyspark.sql.functions import broadcast` or calling `F.broadcast(...)`. Both work; the explicit import is clearer at call sites.

**Lakehouse Monitoring cannot be applied to DLT-managed streaming tables by a setup job user.**
The Lakehouse Monitoring API requires `TABLE SELECT` privilege. DLT streaming tables are owned by the pipeline identity, not the setup job user. `GRANT SELECT` workarounds do not resolve the underlying permission model. The fix in this project: `configure_monitoring.py` creates monitors on staging tables (`order_events`, `inventory_events`, `loyalty_events`) which are owned by the setup job user. Monitor output tables land in `{prefix}metrics`.

**Gold tables live in the silver schema, not their own schema.**
The DLT pipeline's `target` is `{prefix}silver`. Gold aggregate tables (`unit_performance_daily`, `sos_compliance_summary`, `loyalty_cohort_metrics`, `inventory_waste_summary`) are co-located in `{prefix}silver` — not in a separate `{prefix}gold` schema. DAB destroys the entire pipeline-managed schema on `bundle destroy`.

---

## Destroy Job

**`destroy_notebook.py` METRIC_VIEWS list targets the wrong schema.**
The METRIC_VIEWS list in `destroy_notebook.py` (`unit_performance_daily`, `sos_compliance_summary`, etc.) attempts to drop views from `{prefix}metrics`, but those names are the DLT-managed gold tables in `{prefix}silver`. The actual UC metric views created by `create_metric_views.py` (`order_performance`, `loyalty_performance`, `inventory_waste`, `staff_hours`) are not dropped by the destroy job. They are removed when `DROP SCHEMA {prefix}metrics CASCADE` runs in Step 2. The stale list has no runtime impact (views don't exist in metrics so DROP VIEW IF EXISTS is a no-op) but is misleading.

**Governance objects must be destroyed before schema drops.**
`destroy_notebook.py` Steps 0a/0b/0c (drop UC functions, volume, and monitors) must run before the schema drops in Steps 1–4. SDK calls to delete monitors fail if the parent table has already been dropped by a preceding schema cascade. The current destroy order is correct; do not re-order the steps.

**`staging` schema is intentionally preserved by the destroy job.**
The destroy job does not drop `{prefix}staging`. This allows historical data to survive destroy/redeploy cycles so backfill doesn't need to regenerate from scratch. To fully wipe staging, manually run `DROP SCHEMA {catalog}.{prefix}staging CASCADE` after the destroy job completes.

---

## Generator / ID Stability

**Module-level global counters reset to 0 on every serverless notebook execution.**
Serverless cluster notebooks run each job task in a fresh Python process. A module-level counter like `_order_counter = 0` starts from 0 on every run, producing duplicate IDs across runs. This was the root cause of 83–87% PK collisions on all order-domain tables. The fix: all IDs are now generated by `make_id(*parts)` in `src/generator/id_utils.py` — a deterministic 56-bit SHA-256 hash keyed on `(domain_prefix, unit_id, tick_ts, seq/sku)`. The same inputs always produce the same ID, making backfill idempotent.

**`spark.createDataFrame` fails on columns that are `None` in every row.**
PySpark cannot infer a type for a column where every value is `None`. The `write_batch()` function in `main.py` drops such columns before calling `createDataFrame`, then relies on `mergeSchema=true` (Delta) to fill the missing columns with `NULL` when they do appear in future rows.

---

## UC / Unity Catalog

**Genie Space API requires `ctx.apiToken().get()`, not `WorkspaceClient().config.token`.**
On serverless clusters, `WorkspaceClient().config.token` is `None` because the runtime uses OAuth credentials, not PATs. Calling `WorkspaceClient().config.host` works for the host URL, but the bearer token for REST API calls must come from `dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()`.

**`spark.conf.get("spark.databricks.workspaceUrl")` (or `browserHostName()`) is unavailable on serverless.**
`browserHostName()` raises `None.get`. Use `spark.conf.get("spark.databricks.workspaceUrl")` or `WorkspaceClient().config.host` instead.

**UC metric views (`WITH METRICS LANGUAGE YAML`) are not standard SQL views.**
They appear in the catalog like views but behave differently: they expose named measures and dimensions, can be queried with optional slice-by filters, and are the backing object for Genie. Querying them with `SELECT *` returns aggregated results, not raw rows. `DROP VIEW` removes them; they are not affected by `DROP TABLE`.

**`COUNTIF` is not available on all Databricks runtimes.**
Some serverless and older DBR versions do not support `COUNTIF`. Use `COUNT(CASE WHEN condition THEN 1 END)` as the universal alternative. The metric views and pipeline use the `CASE WHEN` form throughout.

**`overwriteSchema=true` is required when adding columns to ref tables written with `mode("overwrite")`.**
When Phase 2.5 added `market_price_index` to the `ref.unit` schema, the seeder's `df.write.format("delta").mode("overwrite")` call failed with `DELTA_METADATA_MISMATCH` because the new column wasn't in the existing schema. Fix: add `.option("overwriteSchema", "true")` to the seeder write. The current `seeder.py` includes this option.

---

## Spark CSV Writer

**`spark.write.csv()` creates a directory of part-files, not a single file.**
`apply_governance.py` exports `menu_catalog_csv/` and `franchise_locations_csv/` as directories of Spark part-files to the UC Volume. The spec refers to `menu_catalog.csv` (singular), but Spark always writes directories. This is acceptable for demo use. If a single file is required, switch to `df.toPandas().to_csv(local_path)` then `dbutils.fs.cp(local_path, volume_path)`.
