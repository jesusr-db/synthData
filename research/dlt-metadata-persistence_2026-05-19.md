# Brainstorming Output — Silver Table Metadata Persistence Fix

## Framing

**Problem.** The QSR synthData project has 14 DLT-managed silver tables and 4 DLT-managed gold tables in `src/pipeline/mvm_pipeline.py`. After every DLT pipeline update (triggered every minute by the live generator), DLT resets table-level metadata to whatever is in `@dlt.table(comment=...)`, drops column-level comments, and drops any informational PK/FK constraints. The post-pipeline `apply_catalog_metadata` task in `setup_job.yml` runs successfully but its writes are erased the next time the generator triggers the pipeline. Net effect: zero column comments and zero constraints persist in `jmrdemo.silver.*` for Genie / DESCRIBE / Catalog Explorer consumers, even though the data model expresses rich semantics in `apply_catalog_metadata.py`.

**Root cause (confirmed).** DLT owns the metadata of every table it materializes. Externally-applied `COMMENT ON TABLE`, `ALTER TABLE ... ALTER COLUMN ... COMMENT`, and `ADD CONSTRAINT ... NOT ENFORCED` statements are not preserved across pipeline refreshes. The supported fix per Databricks docs is to declare table comments, column-level metadata, and informational constraints directly in DLT decorators — either via the `comment=` argument plus a `schema=` argument with SQL DDL (preferred for inline `CONSTRAINT ... PRIMARY KEY` / `FOREIGN KEY ... REFERENCES` syntax), or via a PySpark `StructType` with `StructField` metadata.

**Constraints.**
- Multi-file change → must be on a branch (per `CLAUDE.md` global standard).
- Genie Space already references the silver tables; column descriptions feed Genie's natural-language SQL quality. The fix must improve, not regress, Genie quality.
- Must not break the live generator → pipeline → backfill loop already running in `jmrdemo`.
- Setup must remain fully automatable end-to-end (no manual SQL).
- Tests (71 currently passing) must remain green.
- The metric views in `jmrdemo.metrics` are NOT DLT-managed and are unaffected.
- Bronze gold layer tables (`unit_performance_daily`, `sos_compliance_summary`, `loyalty_cohort_metrics`, `inventory_waste_summary`) are DLT-owned and currently lack any column-level metadata in `apply_catalog_metadata.py` — they too lose their thin top-level comments on each refresh, but they have no FK relationships and synthesised PKs.
- `apply_catalog_metadata.py` is also the source of truth for FK relationships — DLT inline `FOREIGN KEY ... REFERENCES` requires the parent table to exist at pipeline-graph-resolution time, which is already true since all referenced parents (`guest_order`, `guest_profile`) are defined in the same pipeline module.

**Opportunity.** This bug surfaces a broader principle: in DLT pipelines, the DLT module IS the metadata source of truth. Consolidating `TABLE_COMMENTS`, `COLUMN_COMMENTS`, `PK_CONSTRAINTS`, and `FK_CONSTRAINTS` into `mvm_pipeline.py` (or a sibling module imported by it) eliminates the dual source-of-truth and the post-pipeline task that silently no-ops. The setup_job task graph also simplifies.

## Assumptions

1. DLT runtime supports inline `CONSTRAINT ... PRIMARY KEY` and `FOREIGN KEY ... REFERENCES` in the `schema=` DDL for `@dlt.table`.
2. Streaming tables accept a `schema=` argument the same way batch `@dlt.table` does. If not, fall back to StructType-with-metadata for streaming silver tables.
3. All FK parents (`guest_order`, `guest_profile`) are defined in the same DLT pipeline, so inline `FOREIGN KEY ... REFERENCES` resolves at pipeline-graph time.
4. The user wants to KEEP `apply_catalog_metadata.py` as a stub or delete it — both are valid depending on whether future non-DLT metadata objects are anticipated.
5. Branch naming follows the `fix/` convention: `fix/dlt-metadata-persistence`.
6. Tests have no DLT-aware tests — test impact is limited to any imports of `apply_catalog_metadata.py` (likely none).

## Perspectives

1. **DLT framework purist** — DLT module is single source of truth. Move everything in. Delete external apply.
2. **Minimal blast radius** — Keep dicts in `apply_catalog_metadata.py`, generate DLT decorators from them. Less duplication, but adds import risk inside DLT's constrained environment.
3. **Demo-readability advocate** — Readers should see the full data model by reading `mvm_pipeline.py` alone. Inline `schema=` DDL makes it self-documenting.
4. **Genie / catalog consumer** — Column comments and PK/FK constraints flow into Genie's prompt context. Fix must produce correct catalog output.
5. **Operational engineer** — A silently-failing task is worse than no task. Simplify the task graph.
6. **Future-self maintainer** — When a new table is added, the answer for "where do I put metadata?" must be unambiguous and in one place.

## Options

### Option A — Inline SQL DDL `schema=` per table, delete external metadata code [RECOMMENDED]

**Description.** For each silver table, add `schema="..."` to `@dlt.table(...)` with full SQL DDL: column types, inline `COMMENT '...'` clauses, `CONSTRAINT pk_<table> PRIMARY KEY (<col>)`, and FK constraints where applicable. Expand `comment=` from "MVM Silver: <name>" to full sentences. Delete the metadata dicts and loops from `apply_catalog_metadata.py`. Remove the `apply_catalog_metadata` task from `setup_job.yml`.

**Pros.** Single source of truth. DLT preserves all metadata. Self-documenting pipeline. Eliminates silently-failing task. Matches Databricks documented pattern.

**Cons.** `mvm_pipeline.py` grows ~400 lines. Schema duplicated between DDL and `.select()` casts. Multi-line DDL strings are harder to lint.

**Fit.** Best fit.

### Option B — StructType with StructField metadata, PK/FK in `apply_catalog_metadata.py`

**Description.** Build module-level `StructType` schemas with `StructField(metadata={"comment": "..."})`. Pass `schema=<schema_var>`. Keep PK/FK in `apply_catalog_metadata.py` (still broken on refresh, or fix separately).

**Pros.** Python-typed, IDE-friendly. Smaller pipeline body diff.

**Cons.** Two schema systems coexist. PK/FK still get wiped unless also moved. Doesn't simplify task graph.

**Fit.** Acceptable second choice.

### Option C — Generate DLT decorators dynamically from shared dict

**Description.** Import `TABLE_COMMENTS`/`COLUMN_COMMENTS` from a shared module into `mvm_pipeline.py`. Build `schema=` DDL strings programmatically.

**Pros.** Single dict source. Small diff to pipeline body.

**Cons.** Dynamic construction inside DLT's constrained environment is fragile. Hard to debug at pipeline-compile time.

**Fit.** Not recommended.

### Option D — Apply metadata after every refresh (post-update hook)

**Description.** Trigger `apply_catalog_metadata` after each generator tick.

**Pros.** Smallest change to pipeline.

**Cons.** ~60-second window of missing metadata on every refresh. Race condition not fixed, only narrowed. Wastes compute. Anti-pattern.

**Fit.** Do not pick.

## Recommendation

**Option A — Inline SQL DDL `schema=` per table, delete external metadata code.**

**Implementation plan (branch `fix/dlt-metadata-persistence`):**

1. **Phase 1 — Prove on `guest_order`**
   - Expand `@dlt.table(name="guest_order", ...)` with full `comment=` description and `schema="""..."""` DDL including all column types, column-level `COMMENT '...'` on documented columns, and `CONSTRAINT pk_guest_order PRIMARY KEY (guest_order_id)`.
   - Deploy, wait for next generator tick, verify via `information_schema` queries.
   - Gate: column comments present AND PK constraint present after a live pipeline refresh.

2. **Phase 2 — Bulk apply to all 14 silver + 4 gold tables**
   - Apply same pattern to remaining 13 silver tables, ordered so FK parents (`guest_order`, `guest_profile`) are defined before FK children.
   - Add FK constraints to child tables: `order_item`, `payment`, `status_event`, `delivery_order`, `loyalty_transaction`, `reward_redemption`, `guest_order` (→ `guest_profile`).
   - Expand `comment=` on 4 gold tables. Add `schema=` with column comments on gold tables (stretch goal — skip if aggregation schema inference conflicts).

3. **`src/setup/apply_catalog_metadata.py`**
   - Delete `TABLE_COMMENTS`, `COLUMN_COMMENTS`, `PK_CONSTRAINTS`, `FK_CONSTRAINTS` dicts and their application loops.
   - Decision: delete file + task entirely (YAGNI) OR keep as stub. **Default: delete both.**

4. **`resources/setup_job.yml`**
   - Remove `apply_catalog_metadata` task block.
   - Re-wire `create_metric_views.depends_on` from `apply_catalog_metadata` → `start_pipeline`.

5. **`docs/handoff.md`**
   - Update task graph diagram (remove `apply_catalog_metadata` node).
   - Add gotcha: "DLT resets externally-applied metadata on every refresh → declare comment + schema= + constraints inside @dlt.table."

**Top 2 risks:**

1. **Streaming `@dlt.table` may reject `schema=` or certain DDL clauses.** Mitigation: Phase 1 gate on `guest_order` before bulk rollout. Fallback: use StructType-with-metadata (Option B) for streaming tables.

2. **FK references may fail graph resolution.** Mitigation: define FK parents (`guest_order`, `guest_profile`) at the top of the module. Secondary: drop FKs if they fail, keep PKs (higher Genie value anyway).
