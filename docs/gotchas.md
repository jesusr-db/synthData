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

**Lakehouse Monitoring on DLT silver tables requires USE CATALOG + USE SCHEMA on the compute service principal — TABLE SELECT alone is not enough.**
The real gap is not table-level SELECT but catalog and schema visibility. The Lakehouse Monitoring API silently fails or reports permission errors when the setup job's service principal lacks `USE CATALOG` and `USE SCHEMA` at the catalog and schema level; table-level grants are ignored when the principal cannot see the parent scope. Fix: grant `USE CATALOG` and `USE SCHEMA` to `account users` (or the specific SP) before running `configure_monitoring.py`. The current setup task includes these grants before each monitor create.

**Gold tables live in the silver schema, not their own schema.**
The DLT pipeline's `target` is `{prefix}silver`. Gold aggregate tables (`unit_performance_daily`, `sos_compliance_summary`, `loyalty_cohort_metrics`, `inventory_waste_summary`) are co-located in `{prefix}silver` — not in a separate `{prefix}gold` schema. DAB destroys the entire pipeline-managed schema on `bundle destroy`.

**ABAC catalog-level `CREATE POLICY` is not supported on DLT-managed tables.**
Unity Catalog ABAC policies at the catalog level apply to non-DLT tables. When a DLT pipeline owns silver or staging tables, attempting to create a catalog-level ABAC mask policy causes DLT pipeline failures. Use per-table `ALTER COLUMN SET MASK` DDL instead — this works correctly on DLT-managed tables. This is why `apply_governance.py` uses per-table `SET MASK` for `email` and `phone` rather than `CREATE POLICY`.

---

## Destroy Job

**`destroy_notebook.py` METRIC_VIEWS list targets the wrong schema.**
The METRIC_VIEWS list in `destroy_notebook.py` (`unit_performance_daily`, `sos_compliance_summary`, etc.) attempts to drop views from `{prefix}metrics`, but those names are the DLT-managed gold tables in `{prefix}silver`. The actual UC metric views created by `create_metric_views.py` (`order_performance`, `loyalty_performance`, `inventory_waste`, `staff_hours`) are not dropped by the destroy job. They are removed when `DROP SCHEMA {prefix}metrics CASCADE` runs in Step 2. The stale list has no runtime impact (views don't exist in metrics so DROP VIEW IF EXISTS is a no-op) but is misleading.

**Governance objects must be destroyed in strict dependency order before schema drops.**
`destroy_notebook.py` must clean up governance objects in this order before the schema drops in Steps 1–4:

```
Step 0a: DROP MASK (column masks on staging.guest_events and silver.guest_profile)
Step 0d: DELETE Lakehouse Monitors (no function dependency — safe to remove first)
Step 0b: DROP FUNCTION (mask_email, mask_phone, tier_to_multiplier, filter_by_franchisee)
Step 0c: DROP VOLUME (ref.assets)
Steps 1+: DROP schemas
```

Column masks (Step 0a) must precede function drops (Step 0b): if the mask functions are dropped while column masks still reference them, any query on `guest_events` or `guest_profile` fails with `UC_DEPENDENCY_DOES_NOT_EXIST` — including DLT streaming reads. SDK calls to delete monitors or drop functions fail if the parent table or catalog has already been dropped by a preceding schema cascade. Do not re-order these steps.

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

**`class.*` tags are the Data Classification namespace — use them consistently for monitor integration.**
Lakehouse Monitors configured with `MonitorDataClassificationConfig(enabled=True)` write detected PII using `class.*` tag keys (e.g., `class.email_address`, `class.phone_number`). The `apply_governance.py` notebook sets deterministic `class.*` tags as a fallback so PII columns are properly classified before the first monitor refresh runs. Mixing namespaces (e.g., keeping `pii=true` tags alongside `class.*`) creates confusion about which is authoritative and is incompatible with the Data Classification integration.

---

## Spark CSV Writer

**`spark.write.csv()` creates a directory of part-files, not a single file.**
`apply_governance.py` exports `menu_catalog_csv/` and `franchise_locations_csv/` as directories of Spark part-files to the UC Volume. The spec refers to `menu_catalog.csv` (singular), but Spark always writes directories. This is acceptable for demo use. If a single file is required, switch to `df.toPandas().to_csv(local_path)` then `dbutils.fs.cp(local_path, volume_path)`.
