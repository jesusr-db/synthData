# Brainstorm: Data Discovery + ABAC + Scheduled Monitoring Implementation

> **Note on process:** This brainstorm was produced by an Opus subagent. Assumptions are stated explicitly below.

## Framing

The current `apply_governance.py` performs **manual, per-column governance** — it hardcodes which columns are PII, attaches `pii=true` tags directly, defines per-table column masks via `ALTER COLUMN … SET MASK`, and creates a custom row filter via `IS_ACCOUNT_GROUP_MEMBER`. This works but has three problems for a Unity Catalog showcase POC:

1. **It does not demonstrate Databricks' newer governance primitives.** The headline UC story is now: *Data Classification automatically discovers sensitive columns and applies `class.*` tags → ABAC policies at the catalog level automatically mask anything tagged → no per-table DDL required.* The current implementation hides this story behind manual SQL.
2. **Two known environmental failures** mute parts of the existing pack: the workspace tag policy rejects `pii=true` (only `pii=salary` is allowed for that key), and the Data Classification API returns 404 (feature may not be enabled for this workspace yet).
3. **Operationally, the data-quality monitoring side already has a schedule** (`0 0 0/12 * * ?` 12-hour cron on the three snapshot monitors). The task is to verify and lock that in, and plan ABAC + classification around the same lifecycle.

The opportunity: rebuild `apply_governance.py` around **tag-driven ABAC** with classification as the discovery source, while keeping the existing column-mask approach as a deterministic fallback so the demo works regardless of workspace-feature state. Tighten the monitoring story by making the cron schedule a single source of truth and surfacing schedule state in setup logs.

**Constraints:**
- Azure workspace `adb-7405605519549535`, catalog `jmrdemo`, schema_prefix `synth_`.
- Workspace tag policy disallows `pii=true` — only `salary` is permitted for the `pii` key.
- Data Classification REST endpoint returns 404 — cannot rely on automated tag application yet.
- ABAC is documented as available on this workspace.
- Setup job must be fully automatable from zero and idempotent.
- Monitoring code lives in `configure_monitoring.py`, already wired as `apply_governance → configure_monitoring` in `setup_job.yml`.

## Assumptions

1. Demo audience is governance/SSA stakeholders (DPZ-style POC). Implementation should make the *modern* governance flow visible even if a fallback is needed.
2. The tag policy cannot be modified — design around it using the `class.*` namespace instead of the `pii` key.
3. ABAC policy DDL/API is the target driver (`CREATE POLICY` at the catalog/schema level, conditioned on column tags).
4. Data Classification 404 is environmental, not permanent — keep best-effort call, supply manual `class.*` fallback tags.
5. The 12-hour cron cadence is approved and acceptable.
6. Destroy ordering rule holds: ABAC policies → masks → functions → schemas (prevents UC_DEPENDENCY_DOES_NOT_EXIST).
7. Snapshot monitors are correct for staging tables; a timeseries monitor on `silver.guest_order` (keyed on `placed_at`) is worth adding for the drift story.

## Perspectives

**Demo storyteller (SA/SSA):** Wants the flow to be: "Databricks automatically discovered PII, tagged it, and ABAC masked it with one policy." The compelling thing is the *automation*. Current code looks like 1990s GRANT/REVOKE.

**Operations (you, re-deploying):** Cares that destroy → redeploy never breaks. Every governance object — including ABAC policies — must have a clean teardown step that runs *before* parent objects are dropped.

**Auditor / CISO sponsor:** Wants to see schedule on monitoring (12h cron ✓), evidence PII columns are tagged/classified (`class.*` tags), masking is automatic and centralized (ABAC win), and audit trail in monitor output tables.

**Workspace admin (tag policy enforcer):** Will reject any tag write violating the `pii` key policy. Code must not emit `pii=true`.

**Future engineer extending the pack:** Wants to know *where* to add a new tagged column or monitor. Today `apply_governance.py` is procedural with repeated lists; tag lists should become single-source-of-truth.

## Options

### Option A — Tag-Driven ABAC, Classification-Discovered, with Deterministic Fallback (recommended)

**Description:** Replace manual `pii=true` tagging and per-column `SET MASK` with a layered approach:
1. Run Data Classification scan (best-effort). If successful, UC writes `class.email_address`, `class.phone_number`, etc.
2. Apply deterministic `class.*` tags as fallback in `apply_governance.py` for known PII columns — bypasses the tag-policy issue.
3. Define ABAC policies at the catalog/schema level binding `class.email_address` → `mask_email`, `class.phone_number` → `mask_phone`. Created once, not per-table.
4. Drop per-table `ALTER COLUMN SET MASK` — replaced by policy application.
5. Keep row filter as-is (row filters not yet ABAC-friendly enough to move).
6. Drop `pii=true` tag writes entirely.
7. Add monitor schedule as a single constant; add timeseries monitor on `silver.guest_order`; surface schedule state in logs.
8. Add ABAC policy teardown to destroy_notebook.py Step 0e.

**Pros:** Showcases modern UC story; sidesteps `pii=salary` policy; works whether or not classification is enabled; reduces per-table DDL; monitoring schedule becomes single-source-of-truth.

**Cons:** New dependency on ABAC policy API (validate before implementing); more moving parts at destroy time; `class.*` namespace may be reserved for Databricks-managed writes.

**Fit: Strong.** Matches stated end-state, handles both blockers, highest demo value per dev day.

---

### Option B — Custom Tag Key + ABAC (backup if `class.*` namespace is reserved)

**Description:** Same as Option A but use a custom tag key (e.g., `governance_class` with values `email`, `phone`, `name`, `zip`) instead of `class.*`. ABAC policies bind to `governance_class = 'email'`.

**Pros:** Guaranteed to bypass reserved-namespace restriction; no conflict with future classification writes.

**Cons:** Loses headline demo narrative ("Databricks auto-tagged and ABAC masked"); less compelling story.

**Fit: Reasonable fallback only.** Prototype Option A first; drop to B only if blocked.

---

### Option C — Status Quo Plus Cleanup (minimal)

**Description:** Remove failing `pii=true` writes and 404 classification calls. Lock in monitoring schedule. Add timeseries monitor. Nothing else.

**Pros:** Smallest delta; setup job becomes 100% green.

**Cons:** Doesn't deliver the ABAC + classification demo story. Future PII column additions still require editing both tag and mask lists.

**Fit: Weak.** Addresses failures but doesn't deliver requested capability.

---

### Option D — Pure ABAC, no Fallback (classification-only)

**Description:** Wait until Data Classification API is enabled. No manual masks or tags.

**Pros:** Cleanest demo of intended end-state.

**Cons:** Classification API currently 404 → no path to green today.

**Fit: Poor.** Blocks on external dependency we don't control.

## Recommendation

**Implement Option A — Tag-Driven ABAC, Classification-Discovered, with Deterministic Fallback.**

**Implementation skeleton:**

1. **Rewrite `apply_governance.py`** into clearly separated steps:
   - Step 1 (unchanged): volume + sample files
   - Step 2 (unchanged): table/column comments
   - Step 3 (**new**): apply `class.*` tags deterministically to known PII columns (replaces failing `pii=true` writes)
   - Step 4 (unchanged): UC scalar functions (`mask_email`, `mask_phone`, `tier_to_multiplier`)
   - Step 5 (**new**): create catalog-level ABAC policies binding `class.email_address` → `mask_email`, `class.phone_number` → `mask_phone`
   - Step 6 (unchanged): row filter function + per-table attach
   - Step 7 (**best-effort**): trigger classification scan; on 404, log that fallback tags from Step 3 are authoritative
   - Step 8 (**removed**): `pii=true` writes
   - Step 9 (**removed**): per-table `ALTER COLUMN SET MASK` (ABAC drives masking now)

2. **Refactor `configure_monitoring.py`:**
   - Promote cron expression to a `MONITOR_SCHEDULE` constant
   - Use `(table, monitor_spec)` tuple list for easy extension
   - Add timeseries monitor on `silver.guest_order` keyed on `placed_at`, same cron
   - Print resolved schedule from returned monitor object in every run
   - Call `quality_monitors.update` when schedule drifts (idempotent re-runs converge)

3. **Update `destroy_notebook.py`:**
   - Add Step 0e: drop ABAC policies before functions
   - Include new timeseries monitor in the monitor-delete loop

4. **No changes to `setup_job.yml` DAG** — existing `apply_governance → configure_monitoring` chain is correct.

5. **Add `docs/governance.md`** explaining the layered tag-fallback model.

**Top risks:**

- **Risk 1 — `class.*` namespace may be reserved.** If user-applied `class.email_address` tags are rejected, fallback story breaks. Mitigation: verify with Context7/docs before coding; if blocked, drop to Option B (only predicate binding changes, not policy logic).
- **Risk 2 — ABAC policy API syntax unverified.** No in-repo precedent. Mitigation: validate exact DDL/API form via docs first; produce a standalone test before incorporating into setup. If ABAC cannot be automated reliably, keep per-table SET MASK driven by the tag list.
- **Risk 3 — Destroy ordering regression.** ABAC teardown is a new step; mis-ordering will reproduce UC_DEPENDENCY_DOES_NOT_EXIST. Mitigation: run full destroy → deploy → setup_job cycle as verification gate.
