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

**Serverless tasks cannot use task-level `libraries:` — declare dependencies in `environments:` instead.**
The Databricks Terraform provider rejects any serverless notebook task that includes a `libraries:` field at the task level with: `"Libraries field is not supported for serverless task, please specify libraries in environment."` This error only appears at `bundle deploy` time — `databricks bundle validate` (schema-only) passes without catching it. The fix: add an `environments:` block at the job level with an `environment_key` name and a `spec.dependencies` list; then set `environment_key: <name>` on the task instead of `libraries:`. `setup_job.yml` declares three environments — `generator` (`faker`), `refresh` (`requests`, `pyyaml`), and `ml` (`databricks-feature-engineering`, `scikit-learn`, `joblib`, `pandas`, `pyyaml`) — and `refresh_weather_events.yml` and `feature_refresh_job.yml` declare `refresh` and `ml` respectively using this same pattern.

**DAB auto-prepends `[dev <short_user>]` to all job names — adding an explicit prefix in the yml produces a duplicate.**
If a yml `name:` field includes `[${bundle.target} ${workspace.current_user.userName}]` (or any explicit prefix), and DAB's `presets` also adds an auto-prefix, the deployed job name will show two `[dev ...]` prefixes. This is exactly what happened to `weather_events_refresh_job` — its deployed name is `[dev jesus_rodriguez] [dev jesus.rodriguez@databricks.com] Weather & Events Refresh [dev]`. To avoid this, rely solely on DAB's auto-prefix (remove the explicit prefix from the yml), or suppress the auto-prefix in `databricks.yml` with `presets.name_prefix: ""` and use only the explicit yml prefix.

**`ontos_enabled: false` skips ontos steps in both setup and destroy — set it before deploying if the ontos app is unavailable.**
Both `apply_ontos.py` (setup) and `destroy_notebook.py` (destroy) check the `ontos_enabled` widget. If the ontos Databricks App is not deployed in the target workspace, set `--var ontos_enabled=false` at deploy time to suppress all ontos API calls. The task graph is unchanged — the `apply_ontos` task still runs, but exits immediately when `ontos_enabled` is `false`. Forgetting this flag in an environment without the ontos app causes `apply_ontos` to fail with a connection error against `ontos_app_url`.

**`features_enabled: false` skips feature store + recommender steps in both setup and destroy.**
The `features_enabled` variable (default `true`) gates the `build_feature_tables` and `train_recommender` setup tasks and their destroy-side teardown. Set `--var features_enabled=false` at deploy time to provision the core data platform without the feature store or recommender — for example, in a workspace that does not need the PizzaTel recommendation endpoint, or where the `ml` environment dependencies cannot be installed. As with `ontos_enabled`, the gating is inside the notebooks; the task graph structure does not change. (Note: destroy Step 0h currently runs unconditionally regardless of this flag — see Destroy Job below.)

**`initial_otel_backfill` must run after `setup` and gate `backfill` — insert it in series, not parallel.**
The otel live-order refresh task (`initial_otel_backfill`) mirrors `initial_weather_refresh`: it depends on `setup` (which creates the `staging.order_events` streaming table it appends to) and it gates `backfill`. In `setup_job.yml`, `backfill.depends_on` is now `[setup, initial_weather_refresh, initial_otel_backfill]`. Because the otel source is best-effort, this task must still SUCCEED (printing `[WARN]`) even when otel is unreachable — it must never block `setup` or `backfill`. Adding it as a parallel branch to `backfill` would race the staging-table creation and produce a table-not-found error on day 1.

---

## Lakeflow Declarative Pipelines (DLT)

**Column comments, PK/FK constraints, and table descriptions are reset on every pipeline update.**
DLT owns the metadata for tables it materializes. Any externally-applied `COMMENT ON TABLE`, `ALTER COLUMN COMMENT`, or `ADD CONSTRAINT` is overwritten each time the pipeline runs. The fix: declare `comment=` and `schema=` (with inline column `COMMENT` and `CONSTRAINT ... NOT ENFORCED`) directly in `@dp.table` decorators. The `apply_governance.py` notebook still sets comments on staging and ref tables, but silver table metadata must live in the pipeline file.

**`CREATE OR REPLACE TABLE` on staging tables breaks DLT streaming checkpoints.**
Streaming tables maintain internal state keyed on the Delta table ID. If `setup_notebook.py` used `CREATE OR REPLACE TABLE`, every re-run would generate a new table ID and invalidate all downstream streaming checkpoints. All staging tables use `CREATE TABLE IF NOT EXISTS` to preserve the ID across re-runs.

**Appending to a DLT streaming source (`staging.order_events`) must be append-only — never MERGE/UPDATE/DELETE.**
`staging.order_events` is a DLT streaming source. The otel refresh notebook (`otel_refresh_notebook.py`) writes new real-order rows with `.write.mode("append").option("mergeSchema","true")` and NEVER runs MERGE, UPDATE, or DELETE against it — any of those would break the stream's checkpoint. This is the opposite idiom from the weather-events `refresh_notebook.py`, which MERGEs into a batch ref table. Do not copy the MERGE pattern from the weather refresh into anything that writes to a streaming source. Idempotency across refresh runs comes from the high-water-mark (`WHERE source='otel'`), not from MERGE.

**Adding a `source` column to `staging.order_events` avoids a forced DLT full-refresh only because it is not threaded downstream.**
The `source` column (`'synth'` vs `'otel'`) exists purely so the otel refresh can compute its high-water-mark (`MAX(event_ts) WHERE source='otel'`). It is deliberately NOT read by `mvm_pipeline.py` — silver/gold/Genie never see it, so real orders are indistinguishable from synthetic ones downstream, and the streaming-table schema consumed by DLT is effectively unchanged (no forced full-refresh). If you thread `source` into a `@dp.table` decorator or a downstream view, you reintroduce a schema change that can force a DLT full refresh.

**`order_item.unit_price` must be floored > 0 or otel line items silently vanish from silver.**
`mvm_pipeline.py`'s `order_item` table has `@dp.expect_or_drop("positive_price","unit_price > 0")`. When the otel adapter distributes an order total across line items, each per-line price is floored at `0.01` (`max(0.01, round(subtotal*qty/total_qty, 2))`). Without the floor, a rounding-to-zero line price is dropped by the expectation and the item disappears from silver with no error.

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

## Weather & Events Refresh

**Missing Ticketmaster or SeatGeek secrets silently skip those providers — this is expected.**
`refresh_notebook.py` wraps each third-party secret fetch in a bare `except` block. If `dbutils.secrets.get(scope=..., key=...)` raises (scope missing, key missing, or insufficient permissions), that provider is skipped and the notebook logs `[INFO] ... secret not configured — skipping`. The job still completes successfully with holidays from Nager.Date populated. Only configure secret scopes/keys if you have API credentials; do not treat the `[INFO]` skip message as an error.

**NOAA alert `onset` and `expires` fields can be `None` — date-range matching requires a null guard.**
The NOAA NWS API occasionally returns alerts where `onset` or `expires` is `None` (e.g., alerts with no defined expiry). `refresh_notebook.py` guards this with `alert["onset"][:10] if alert["onset"] else ""`. A row-level comparison `"" <= date_str <= ""` always evaluates to `False`, so null-bounded alerts are simply not applied. If NOAA alerts appear to have no effect on a date range, verify the raw alert has non-null `onset`/`expires` values.

**`ref.weather_conditions` and `ref.local_events` must exist before the MERGE — setup_notebook.py creates them.**
`refresh_notebook.py` uses `MERGE INTO {catalog}.{prefix}ref.weather_conditions` without a CREATE-if-not-exists guard. If the ref schema or tables don't exist, the notebook fails. The setup job task ordering enforces this: `initial_weather_refresh` depends on `setup`, which runs `setup_notebook.py` to create the ref schema and empty tables. Running `refresh_notebook.py` standalone against a workspace where setup has not completed will fail with a table-not-found error.

**The weather refresh job reads distinct metros from `ref.unit` — an empty or missing `ref.unit` produces zero weather rows.**
The notebook opens with `spark.sql("SELECT DISTINCT metro_area, state, AVG(lat) AS lat, AVG(lon) AS lon FROM {catalog}.{prefix}ref.unit GROUP BY metro_area, state")`. Note: `ref.unit` stores coordinates as `lat` and `lon` (not `latitude`/`longitude`). If `ref.unit` is empty (setup never seeded it) or the table doesn't exist, `metro_rows` is empty and all subsequent API calls are skipped silently. Weather and events tables will remain empty. This is why `initial_weather_refresh` in the setup job depends on `setup` (which seeds `ref.unit`) rather than running in parallel with it.

**Ticketmaster and SeatGeek events are deduplicated by `event_id` — SeatGeek cannot overwrite Ticketmaster rows.**
`event_rows_by_id` is keyed on `event_id`. The SeatGeek loop only inserts rows where `r["event_id"] not in event_rows_by_id`, so SeatGeek events that share an ID with a Ticketmaster event are silently dropped. Cross-provider deduplication is intentional (to avoid duplicate event rows in `ref.local_events`) but means SeatGeek data for any event already fetched by Ticketmaster will never appear.

**`demand_risk_forecast` view returns zero rows until the first refresh job completes.**
The view's `CASE` expression joins against `ref.weather_conditions`. Until `initial_weather_refresh` runs during setup and populates that table, the join produces no matches and the view returns an empty result set. This is not a bug — the view "lights up" automatically once the setup job's `initial_weather_refresh` task finishes. If the view is empty after a fresh deploy, check whether the setup job completed all 12 tasks including `initial_weather_refresh`.

**WMO weather code → condition mapping overrides for extreme temperatures may mask the raw code.**
`openmeteo_client.py` maps WMO codes 0–3 to `clear` by default, but overrides to `extreme_heat` when `temp_max > 100°F` or `extreme_cold` when `temp_min < 15°F`. If you observe a `clear`-coded day being stored as `extreme_heat`, this override is intentional — it catches summer heatwaves and winter cold snaps that WMO would otherwise classify as fair weather. Debugging weather condition values requires checking both the raw WMO code and the daily temperature bounds from Open-Meteo.

**`CausalContext.build_context()` silently ignores `weather_event_data=None` — passing `None` is safe.**
The `weather_event_data` parameter added in Phase 3 defaults to `None` and is guarded by an `if weather_event_data:` check. Any caller (including backfill tasks that predate Phase 3) that omits the parameter or passes `None` gets identical behavior to the pre-Phase-3 baseline. This guard is the reason the 75 existing tests stayed green without modification.

---

## OTel Live Orders Refresh

**The refresh degrades to a graceful no-op — a missing `SELECT` grant leaves the pipeline green but shows no real orders.**
The otel refresh job reads `jmrdemo.zerobus.otel_logs` and `otel_spans`. The job principal must have `SELECT` on both. This grant cannot be verified by any build-time check — if it is missing, the refresh catches the read failure, prints `[WARN]`, and appends zero rows. The pipeline stays green and the demo simply shows no live orders. Grant `SELECT` on both source tables to the job principal before demoing.

**Correlation between otel logs and spans is strictly `trace_id` — never equate `app.order.id` and `order.id`.**
The otel log field `app.order.id` (a UUID) and the span field `order.id` (an int) are different identifiers and must never be treated as equal. The adapter (`otel_order_adapter.py`) reads neither; the notebook projects them only as descriptive columns. Every `make_id` seed and the log⋈span join key on `trace_id` alone. If real orders fail to correlate or IDs collide, check that the join and all ID seeds use `trace_id` and nothing else.

**Live-load-generator and fee-test rows are filtered out before appending.**
The adapter drops synthetic load-generator traffic and fee-test orders so only genuine orders reach `staging.order_events`. If an expected order is missing from the live feed, confirm it was not classified as load-gen or fee-test traffic by the adapter's filters.

**The `source` column is the sole idempotency mechanism for otel appends.**
Because appends to a streaming source cannot use MERGE, re-runs stay idempotent by computing a high-water-mark over `MAX(event_ts) WHERE source='otel'` and only appending rows newer than it. All otel rows carry `source='otel'`; the generator's synthetic order rows default to `source='synth'` via `write_batch`. Do not remove or repurpose the `source` column — the refresh would then re-append every historical row on each run.

---

## Feature Store / Recommender

**`build_feature_tables.py` runs in two jobs — keep it idempotent.**
The same notebook (`src/setup/build_feature_tables.py`) is invoked by both the `build_feature_tables` task in `setup_job` and the standalone weekly `feature_refresh_job`. Any change to it affects both the initial build and every weekly refresh. It must be safe to re-run against existing feature tables (overwrite/MERGE, not append-and-duplicate), because the refresh job re-executes it on a schedule with no setup-time guard.

**Feature tables depend on the silver layer — the build task is gated on `start_pipeline`.**
`build_feature_tables` derives customer- and store-level features from silver tables, so it depends on `start_pipeline` having materialized them. Running the feature build before the pipeline has produced silver rows yields empty or zero-row feature tables. The standalone `feature_refresh_job` assumes the silver layer already exists (setup has completed at least once) — running it against a fresh workspace where setup has not run produces empty features.

**`train_recommender` depends on `build_feature_tables`, not just `start_pipeline`.**
The training notebook reads the customer and store feature tables to assemble per-candidate feature vectors. Its `depends_on` is `build_feature_tables` (which in turn depends on `start_pipeline`), so the feature tables are guaranteed present before training. Do not re-point this dependency at `start_pipeline` directly — that would race the feature build and train on missing tables.

**The model-serving endpoint must be created/updated via raw REST, not the SDK `serving_endpoints` wrapper.**
In serverless notebooks the SDK `serving_endpoints` create/update wrapper is unreliable — its submit retries were observed to exhaust (all 3 attempts failing) and the task then raised its own `RuntimeError`. The working pattern (Fix 9) is raw REST via `api_client.do()` to the serving endpoints REST API, which terminated SUCCESS in a serverless notebook. `destroy_notebook.py` Step 0h-1 uses the same raw-REST pattern for endpoint deletion. If you see persistent ~8–11-minute training-task failures during `train_recommender`, the cause is the SDK endpoint submit, not the model training itself — switch the failing call to raw REST.

**Model-load failures from version drift — pin the full scientific stack to the training versions.**
The recommender pyfunc failed to load at serving time with a numpy 2.x incompatibility (`ComplexWarning` removed from `numpy.core.numeric`). The fix was to pin the full scientific stack — `numpy`, `scipy`, `joblib`, `pandas`, and `scikit-learn` — to the exact versions used at training in the model's `pip_requirements`. Pinning only `scikit-learn` was insufficient. If a newly trained model serves but won't load, suspect a transitive numpy/scipy ABI mismatch and pin the whole stack.

**Cart signal must be threaded into the FE training set as scalar columns.**
`create_training_set()` only includes the `profile_id`/`store_id` lookup keys by default, so `cart_product_ids` (and `member_id`, `viewed_product_id`, `num_recommendations`) are absent from the FE training set and never reach the pyfunc. The fix adds them to the training set as scalar columns. Note FE `RequestSource` supports only scalar types (`ScalarDataType`); array columns like `cart_product_ids` are not natively supported, which is why the extra request fields appear in the serving signature.

**Empty `recommender_query_principal` skips the `CAN_QUERY` grant — the endpoint deploys, but PizzaTel cannot call it.**
`train_recommender` only grants `CAN_QUERY` on the serving endpoint when `recommender_query_principal` is non-empty. With the default empty value the endpoint is created without the grant, so the consuming PizzaTel principal will get a permission error when it calls the endpoint. Set `--var recommender_query_principal=<sp_name>` at deploy time once the consuming principal exists, or grant `CAN_QUERY` manually afterward.

**The `ml` environment must install successfully or `build_feature_tables`/`train_recommender` fail at deploy/run.**
Both tasks use the `ml` serverless environment (`databricks-feature-engineering`, `scikit-learn`, `joblib`, `pandas`, `pyyaml`). If the workspace cannot resolve these dependencies, the tasks fail. In environments where the recommender is not needed, set `--var features_enabled=false` to skip these tasks rather than fighting the dependency install.

**Store MAP columns (`popularity`, `top_item_per_category`) are stored as JSON strings in Online Tables.**
Online Tables do not support `MAP<string, float>` or `MAP<string, string>` column types — writing them causes a schema validation error at sync time. `store_features.py` serializes these maps to JSON strings (`json.dumps(...)`) before writing to the feature table. The recommender pyfunc deserializes them at inference time with `json.loads(...)`. If you add new map-typed columns to the store feature table, apply the same JSON serialization or the Online Table sync will fail.

**Model artifacts are baked into the pickled pyfunc instance — not stored as `artifacts` in `fe.log_model`.**
`recommender_model.py` stores `scoring_params`, the affinity config, and all lookup state directly on `self` inside the `RecommenderModel` pyfunc class. When MLflow serializes the model via `fe.log_model`, the pickled instance carries everything. There is no `artifacts=` kwarg in the log call. This means the model is self-contained: loading it from the registry does not require any additional file or config lookup. The downside is that the model binary grows slightly (~1–2 MB for the affinities + menu catalog); this is acceptable at demo scale.

**Online Tables and Feature Serving / Model Serving endpoints are billable — they are torn down by `destroy_notebook.py` Step 0h.**
Unlike Delta tables (which persist until schema drop), Online Tables and serving endpoints incur ongoing costs while active. The destroy notebook's Step 0h explicitly deletes both online tables (`customer_features_online`, `store_features_online`) and the serving endpoints (`synth_qsr-customer-features`, `synth_qsr-recommender`) via raw REST (`api_client.do()`, matching the create-path workaround). Step 0h runs unconditionally and is best-effort (each delete is wrapped in try/except), so it is safe to run even if the feature objects were never created. The `features_enabled` variable is declared for future gating but does not currently skip setup or teardown steps. Running `bundle destroy` alone does NOT delete these objects — always run the destroy job first.

**The recommender endpoint and feature tables "light up" only after `build_feature_tables` + `train_recommender` both complete.**
Until those two setup tasks finish, the Feature Serving endpoint returns empty lookups and the recommender falls back to cold-start (store-popularity) results for every caller. This is expected for a fresh deploy. The setup job task graph guarantees the ordering: `build_feature_tables` depends on `start_pipeline` (silver data present), `train_recommender` depends on `build_feature_tables` (features present), and `unpause_generator` depends on `train_recommender` (model live before live writes resume). Check task status in the setup job run log if the endpoint appears to return only cold-start results after setup completes.

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

Column masks (Step 0a) must precede function drops (Step 0b): if the mask functions are dropped while column masks still reference them, any query on `guest_events` or `guest_profile` fails with `UC_DEPENDENCY_DOES_NOT_EXIST` — including DLT streaming reads. SDK/REST calls to delete monitors or drop functions fail if the parent table or catalog has already been dropped by a preceding schema cascade. Do not re-order these steps.

**`staging` schema is intentionally preserved by the destroy job.**
The destroy job does not drop `{prefix}staging`. This allows historical data to survive destroy/redeploy cycles so backfill doesn't need to regenerate from scratch (the generator resumes from `MAX(event_ts)`). To fully wipe staging, manually run `DROP SCHEMA {catalog}.{prefix}staging CASCADE` after the destroy job completes.

**Ontos teardown requires the ontos app to be reachable — set `ontos_enabled=false` if it is not.**
`destroy_notebook.py` receives `ontos_app_url` and `ontos_enabled` as parameters (passed from `destroy_job.yml`). When `ontos_enabled` is `true`, the notebook calls the ontos REST API to remove registered schemas, data products, and semantic links before the schema drops proceed. If the ontos app is unreachable or has already been decommissioned, the destroy job will fail at the ontos teardown step. Run with `--var ontos_enabled=false` (or update the variable default) to skip ontos teardown in those cases.

**Feature store + recommender teardown in Step 0h is best-effort and runs unconditionally.**
Step 0h in `destroy_notebook.py` always runs regardless of `features_enabled` — every delete call is wrapped in try/except so deleting non-existent objects is safe and logged as `[WARN]`. If setup was run with `features_enabled=false` (and the feature objects were never created), the teardown step still runs but each sub-step exits cleanly with a warning rather than an error. Endpoint deletes use raw REST (`api_client.do()`) matching the create path.

---

## Generator / ID Stability

**Module-level global counters reset to 0 on every serverless notebook execution.**
Serverless cluster notebooks run each job task in a fresh Python process. A module-level counter like `_order_counter = 0` starts from 0 on every run, producing duplicate IDs across runs. This was the root cause of 83–87% PK collisions on all order-domain tables. The fix: all IDs are now generated by `make_id(*parts)` in `src/generator/id_utils.py` — a deterministic 56-bit SHA-256 hash keyed on `(domain_prefix, unit_id, tick_ts, seq/sku)`. The same inputs always produce the same ID, making backfill idempotent.

**Namespace the `make_id` domain prefix to keep otel and synth IDs collision-proof.**
Real orders from otel are bridged into the same ID space via `make_id("otel", trace_id)`, distinct from synthetic orders' `make_id("o", ...)`. The different domain prefixes guarantee the two sources can never collide even if a `trace_id` numerically resembles a synth seed. Stability comes from the hash inputs (same `trace_id` → same id across refresh runs), so idempotency is preserved without any MERGE.

**`write_batch` defaults `source` to `synth` only for order-domain event types.**
`main.py`'s `write_batch()` calls `row.setdefault("source","synth")`, but scoped to order-domain rows only — the other four staging tables are left untouched so their schemas don't churn. If you widen this default to all event types, you force a schema change on tables that don't need the `source` column.

**`spark.createDataFrame` fails on columns that are `None` in every row.**
PySpark cannot infer a type for a column where every value is `None`. The `write_batch()` function in `main.py` drops such columns before calling `createDataFrame`, then relies on `mergeSchema=true` (Delta) to fill the missing columns with `NULL` when they do appear in future rows.

---

## UC / Unity Catalog

**Genie Space API requires `ctx.apiToken().get()`, not `WorkspaceClient().config.token`.**
On serverless clusters, `WorkspaceClient().config.token` is `None` because the runtime uses OAuth credentials, not PATs. Calling `WorkspaceClient().config.host` works for the host URL, but the bearer token for REST API calls must come from `dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()`.

**`spark.conf.get("spark.databricks.workspaceUrl")` (or `browserHostName()`) is unavailable on serverless.**
`browserHostName()` raises `None.get`. Use `spark.conf.get("spark.databricks.workspaceUrl")` or `WorkspaceClient().config.host` instead.

**UC metric views (`WITH METRICS LANGUAGE YAML`) are not standard SQL views.**
They appear in the catalog like views but behave differently: they expose named measures and dimensions, can be queried with optional slice-by filters, and are the backing object for Genie. Querying them with `SELECT *` returns aggregated results, not raw rows. `DROP VIEW` removes them; they are not affected by `DROP TABLE`. (`demand_risk_forecast`, by contrast, is a standard view and does return raw rows.)

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

---

## Local Dev / Testing

**The project `.venv` is self-ignored and `requirements.txt` does not pin `pandas`/`mlflow` — a fresh machine fails at test collection.**
Python's `venv` module writes `.venv/.gitignore` containing `*`, so `.venv` never appears in `git status` even though the repo's top-level `.gitignore` does not list it. It is safe from accidental commit only if you use explicit `git add <paths>` and never `git add .`. Separately, `requirements.txt` is missing `pandas` and `mlflow`, which `tests/test_recommender_model.py` imports — so even after `pip install -r requirements.txt` the pytest suite dies at collection on a fresh venv. Until those two are pinned in `requirements.txt`, install them manually into `.venv` before running the suite. Run all pytest gates via `.venv/bin/python -m pytest -q`, never bare `python3` (the machine default is a depless Homebrew interpreter). The true full-suite baseline is 192 tests (220 with the 28 new otel tests) — the plan doc's "~118" estimate is stale.
