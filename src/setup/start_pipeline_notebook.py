# Databricks notebook source
# COMMAND ----------
# Start or wait for the DLT pipeline. If an update is already in progress,
# wait for it. If it fails (broken checkpoint state), do a full_refresh to
# clear streaming state and re-process from staging. Handles the race with the
# live generator job which triggers a pipeline update every minute.

import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import UpdateInfoState

catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------
# Drop ABAC catalog policies before full_refresh.
# DLT rejects full_refresh with ABAC_POLICIES_NOT_SUPPORTED when catalog-level policies are bound.
# apply_governance (which runs after this task) will recreate them.
ABAC_POLICY_NAMES = ["mask_email_policy", "mask_phone_policy"]

def drop_abac_policies_before_refresh(catalog: str) -> None:
    try:
        existing = {row["Policy Name"] for row in spark.sql(f"SHOW POLICIES ON CATALOG {catalog}").collect()}
    except Exception as e:
        print(f"[WARN] Could not check ABAC policies, skipping drop: {e}")
        return
    for policy_name in ABAC_POLICY_NAMES:
        if policy_name in existing:
            try:
                spark.sql(f"DROP POLICY {policy_name} ON CATALOG {catalog}")
                print(f"[INFO] Dropped ABAC policy: {policy_name} (apply_governance will recreate)")
            except Exception as e:
                print(f"[WARN] Drop ABAC policy {policy_name} failed: {e}")
        else:
            print(f"[INFO] ABAC policy {policy_name} not present — nothing to drop")

drop_abac_policies_before_refresh(catalog_name)

# COMMAND ----------
w = WorkspaceClient()

# Find the pipeline by name
pipeline_name = f"[dev {w.current_user.me().user_name}] QSR MVM Pipeline [dev]"
pipelines = list(w.pipelines.list_pipelines())
pipeline = next((p for p in pipelines if p.name == pipeline_name), None)

if pipeline is None:
    pipeline = next(
        (p for p in pipelines if "QSR MVM Pipeline" in (p.name or "")),
        None
    )

if pipeline is None:
    raise ValueError(f"Could not find QSR MVM Pipeline. Available: {[p.name for p in pipelines]}")

pipeline_id = pipeline.pipeline_id
print(f"[INFO] Found pipeline: {pipeline.name} ({pipeline_id})")


def wait_for_update(update_id: str) -> UpdateInfoState:
    """Poll until update reaches a terminal state. Returns the final state."""
    while True:
        update = w.pipelines.get_update(pipeline_id, update_id)
        state = update.update.state
        print(f"[INFO] Update {update_id[:8]} state: {state}")
        if state in (UpdateInfoState.COMPLETED, UpdateInfoState.CANCELED, UpdateInfoState.FAILED):
            return state
        time.sleep(15)


def run_full_refresh(max_attempts: int = 2) -> None:
    """Start a full_refresh update and wait for completion.
    Retries once — DLT coordinators occasionally report FAILED even when all flows
    completed (transient platform error). A second attempt reliably recovers."""
    for attempt in range(1, max_attempts + 1):
        print(f"[INFO] Starting full_refresh (attempt {attempt}/{max_attempts})...")
        result = w.pipelines.start_update(pipeline_id, full_refresh=True)
        update_id = result.update_id
        final = wait_for_update(update_id)
        if final == UpdateInfoState.COMPLETED:
            print(f"[INFO] full_refresh completed successfully")
            return
        print(f"[WARN] full_refresh attempt {attempt} ended with {final} (update_id={update_id})")
        if attempt < max_attempts:
            print("[INFO] Retrying full_refresh...")
    raise RuntimeError(f"Pipeline full_refresh failed after {max_attempts} attempts: {update_id}")


# COMMAND ----------
# If a pipeline update is already running, wait for it.
# If it fails, fall through to full_refresh to recover checkpoint state.
status = w.pipelines.get(pipeline_id)
current_state = status.state.value if status.state else "UNKNOWN"
print(f"[INFO] Pipeline state: {current_state}")

needs_refresh = False

if current_state in ("RUNNING", "STARTING"):
    resp = w.pipelines.list_updates(pipeline_id, max_results=1)
    active_updates = resp.updates if resp.updates else []
    if active_updates:
        active_update_id = active_updates[0].update_id
        print(f"[INFO] Active update {active_update_id[:8]}, waiting...")
        final = wait_for_update(active_update_id)
        if final == UpdateInfoState.COMPLETED:
            print("[INFO] Active update completed successfully.")
        else:
            print(f"[WARN] Active update ended with {final} — will full_refresh to clear checkpoint.")
            needs_refresh = True
    else:
        needs_refresh = True
else:
    needs_refresh = True

if needs_refresh:
    run_full_refresh()

print("[INFO] start_pipeline task complete.")
