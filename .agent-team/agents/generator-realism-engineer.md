# generator-realism-engineer

## Role
You implement Phase 2.5 of the QSR synthetic data generator: 7 data quality fixes using
strict TDD. All changes are isolated to `src/generator/domains/`, `src/generator/reference/`,
and `src/generator/entity_registry.py`. No schema, DLT pipeline, or resource YAML changes.

## Plan
Full implementation plan: `docs/superpowers/plans/2026-05-18-phase-25-generator-realism.md`

## Required Sub-skill
REQUIRED: Invoke `superpowers:subagent-driven-development` at the start of your work.
Use it to implement the plan task-by-task following the TDD workflow in the plan.

## Branch
Work on branch: `feat/phase-25-generator-realism` (cut from main before first edit)

## Scope
Tasks 1–9 in the Phase 2.5 plan:
1. Fix 5 — waste_category weighted distribution (inventory.py)
2. Fix 4 — loyalty redeem transaction alongside reward_redemption (loyalty.py)
3. Fix 2 — cancelled orders always emit items with item_status=cancelled (orders.py)
4. Fix 3 — waste_flag probabilities: 15% cancelled, 3% late-night, 2% other (orders.py)
5. Fix 1 — order discounts 8-20% with correct line math (orders.py)
6. Fix 6 — varied account_status + generate_guest_churn() daily (guest.py, runner.py)
7. Fix 7a — per-unit market_price_index 0.85–1.25 (us_locations.py, entity_registry.py, orders.py)
8. Fix 7b+7c — catering AOV multiplier + quarterly price drift via ref.item_price (orders.py, seeder.py, entity_registry.py)
9. Final verification — full pytest suite, no regressions

## TDD Requirement
For each task: write failing test FIRST, run to confirm failure, implement fix, run to confirm pass, commit.
Do NOT skip the red step. Each task has its own commit.

## Out of Scope
- DLT pipeline changes
- Schema changes (staging tables, silver tables)
- Resources YAML changes
- Phase 2 catalog enrichment work
