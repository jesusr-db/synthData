# qsr-synth-data-generator — Project Memory

## Introspection

### Feature "governance-pack" — Phase 1: governance-implementation (2026-05-20T23:35:00-04:00)

#### What worked
- governance-engineer: 4 deliverables landed cleanly. The shared `_unit_franchisee()` helper avoids repeating the `spark.read.table(...).select("unit_id","franchisee_id")` boilerplate across 5 table functions, while keeping each table self-contained for DLT analysis.
- governance-deploy-engineer: setup_job.yml edits were a straightforward DAG insertion. `databricks bundle validate` passed first try.
- The 75 existing pytest suite is hermetic (no Spark/Databricks), so the pipeline changes had no test impact — tests passed in 1.29s after all edits.

#### What failed or needed fixing
- None. Both agents' work landed on first attempt with no rework required.

#### Patterns to watch for
- **CDC tables in Lakeflow Declarative Pipelines need their join in the source view, not the target.** `guest_profile` is a streaming table populated by `dp.create_auto_cdc_flow` from `guest_profile_changes` view. To add `franchisee_id`, the join must go in the `@dp.view(name="guest_profile_changes")` function AND the column must be declared in `dp.create_streaming_table(schema=...)`. There is no `@dp.table` decorator to modify.
- **`broadcast` is in `pyspark.sql.functions`, separate from the `F` alias.** Existing pipeline does `from pyspark.sql import functions as F` but uses `F.col(...)`. Adding broadcast required a second import line `from pyspark.sql.functions import broadcast` — could also be `F.broadcast(...)` but the explicit import is clearer at call sites.
- **Sample volume files**: spark CSV writer creates a directory of part-files, not a single `.csv`. The spec says "menu_catalog.csv" but the implementation writes to `menu_catalog_csv/` (directory). Acceptable for demo purposes; if a single-file is required, switch to pandas via `df.toPandas().to_csv(...)` using a local path then `dbutils.fs.cp`. Documented as a known trade-off, not a defect.
- **Wrap every external API call in try/except for governance setup.** Lakehouse Monitoring API, classification scan API, and SET MASK / SET ROW FILTER DDL all depend on workspace tier and prior state. Treating them as best-effort with `[WARN]` logging (per spec) keeps the setup job green across environments.
- **destroy order matters**: governance objects must be cleaned up BEFORE schema drops cascade them away, otherwise the SDK calls (monitor deletes) fail because the parent table is gone. Step 0a/0b/0c precede the existing Step 1.

#### QA iterations
- Attempt 1: PASS
  - pytest: 75/75 passed (1.29s)
  - bundle validate: OK
  - syntax check: 4/4 files parse
  - contract validation: 18/18 structural checks passed (steps, deps, schemas, joins)
