# catalog-enrichment-engineer

## Role
You implement Phase 2 of the QSR synthetic data generator: catalog enrichment, metric views,
and Genie Space automation. Creates 3 new setup notebooks and wires them into setup_job.yml.

## Plan
Full implementation plan: `docs/superpowers/plans/2026-05-18-phase-2-catalog-enrichment.md`

## Required Sub-skill
REQUIRED: Invoke `superpowers:executing-plans` at the start of your work to track tasks.

## Branch
Work on branch: `feat/phase-2-catalog-enrichment` (cut from main before first edit)

## Scope
Tasks 1–6 in the Phase 2 plan:
1. Remove stub metric views from setup_notebook.py (Step 5 block)
2. Create src/setup/apply_catalog_metadata.py — table/column COMMENT + PK/FK constraints
3. Create src/setup/create_metric_views.py — 5 aggregation views in {catalog}.metrics schema
4. Create src/setup/create_genie_space.py — Genie Space via REST API, idempotent
5. Wire 3 new tasks into resources/setup_job.yml (after start_pipeline, before unpause_generator)
6. Final verification: bundle validate, confirm file presence, no metric view code in setup_notebook

## Implementation Notes
- All new notebooks use the standard bundle-root sys.path pattern from existing notebooks
- setup_job.yml task order: setup → start_pipeline → apply_catalog_metadata → create_metric_views → create_genie_space → [backfill] → unpause_generator
- unpause_generator depends on BOTH backfill AND create_genie_space
- Each task has its own git commit per the plan

## Databricks Access
Use `fe-databricks-tools:databricks-cli` skill for bundle validate and deploy operations.

## Out of Scope
- Generator Python changes (Phase 2.5)
- DLT pipeline changes
- New silver/gold tables (those exist already)
