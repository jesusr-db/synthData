# Agent: governance-deploy-engineer

## Role
You update DAB resource configurations for the Governance Pack feature.
You work on the `feat/governance-pack` branch.

## Scope
You own 1 deliverable:

1. **`resources/setup_job.yml`** (modify) — add 2 new tasks, update `unpause_generator` deps

## Must Read First
- `docs/superpowers/specs/2026-05-20-governance-pack-design.md` — spec section "setup_job.yml additions"
- `resources/setup_job.yml` — current 6-task DAG structure
- `databricks.yml` — variable names (catalog_name, schema_prefix)

## Implementation Guide

### New task: apply_governance
Add after `start_pipeline` task. Depends on `start_pipeline` (silver tables exist after pipeline completes).

```yaml
- task_key: apply_governance
  depends_on:
    - task_key: start_pipeline
  notebook_task:
    notebook_path: ../src/setup/apply_governance.py
    base_parameters:
      catalog_name: ${var.catalog_name}
      schema_prefix: ${var.schema_prefix}
```

### New task: configure_monitoring
Add after `apply_governance`. Depends on `apply_governance`.

```yaml
- task_key: configure_monitoring
  depends_on:
    - task_key: apply_governance
  notebook_task:
    notebook_path: ../src/setup/configure_monitoring.py
    base_parameters:
      catalog_name: ${var.catalog_name}
      schema_prefix: ${var.schema_prefix}
```

### Update unpause_generator deps
Add `configure_monitoring` as a new dependency (in addition to existing `backfill` and `create_genie_space`):

```yaml
- task_key: unpause_generator
  depends_on:
    - task_key: backfill
    - task_key: create_genie_space
    - task_key: configure_monitoring   # new
  notebook_task:
    ...
```

### Resulting DAG
```
setup ──→ backfill ──────────────────────────────────────────────────────┐
      └─→ start_pipeline ──→ create_metric_views ──→ create_genie_space ─┤
                         └──→ apply_governance ──→ configure_monitoring ─┤
                                                                          ↓
                                                              unpause_generator
```

## Constraints
- Do NOT touch any other resource files (generator_job.yml, pipeline.yml, destroy_job.yml)
- Do NOT touch databricks.yml
- Do NOT modify notebook files — that's governance-engineer's scope
- All variable references must use `${var.catalog_name}` and `${var.schema_prefix}` — no hardcoded values
- Preserve the existing `environment_key: generator` on the `backfill` task

## Verification
After updating setup_job.yml:
```bash
cd /Users/jesus.rodriguez/Documents/ItsAVibe/gitrepos_FY27/synthData
databricks bundle validate
```
Bundle must validate without errors.
