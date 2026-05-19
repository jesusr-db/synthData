# Databricks notebook source
# COMMAND ----------
# Start or wait for the DLT pipeline. If an update is already in progress,
# wait for it to finish rather than failing. Handles the race with the live
# generator job which triggers a pipeline update every minute.

import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import UpdateInfoState

catalog_name = dbutils.widgets.get("catalog_name")

w = WorkspaceClient()

# Find the pipeline by name
pipeline_name = f"[dev {w.current_user.me().user_name}] QSR MVM Pipeline [dev]"
pipelines = list(w.pipelines.list_pipelines())
pipeline = next((p for p in pipelines if p.name == pipeline_name), None)

if pipeline is None:
    # Fallback: find by catalog tag
    pipeline = next(
        (p for p in pipelines if "QSR MVM Pipeline" in (p.name or "")),
        None
    )

if pipeline is None:
    raise ValueError(f"Could not find QSR MVM Pipeline. Available: {[p.name for p in pipelines]}")

pipeline_id = pipeline.pipeline_id
print(f"[INFO] Found pipeline: {pipeline.name} ({pipeline_id})")

# COMMAND ----------
# Check current state — if RUNNING, wait for current update to complete
status = w.pipelines.get(pipeline_id)
current_state = status.state.value if status.state else "UNKNOWN"
print(f"[INFO] Pipeline state: {current_state}")

if current_state in ("RUNNING", "STARTING"):
    # Find the active update and wait for it
    updates = list(w.pipelines.list_updates(pipeline_id, max_results=1))
    if updates:
        active_update_id = updates[0].update_id
        print(f"[INFO] Update already in progress ({active_update_id}), waiting for completion...")
        while True:
            update = w.pipelines.get_update(pipeline_id, active_update_id)
            state = update.update.state
            print(f"[INFO] Update state: {state}")
            if state in (UpdateInfoState.COMPLETED, UpdateInfoState.CANCELED, UpdateInfoState.FAILED):
                if state == UpdateInfoState.FAILED:
                    raise RuntimeError(f"Existing pipeline update failed: {active_update_id}")
                print(f"[INFO] Existing update finished with state: {state}")
                break
            time.sleep(15)
else:
    # Start a new incremental update
    print("[INFO] Starting new pipeline update...")
    result = w.pipelines.start_update(pipeline_id, full_refresh=False)
    update_id = result.update_id
    print(f"[INFO] Started update: {update_id}")
    while True:
        update = w.pipelines.get_update(pipeline_id, update_id)
        state = update.update.state
        print(f"[INFO] Update state: {state}")
        if state in (UpdateInfoState.COMPLETED, UpdateInfoState.CANCELED, UpdateInfoState.FAILED):
            if state == UpdateInfoState.FAILED:
                raise RuntimeError(f"Pipeline update failed: {update_id}")
            print(f"[INFO] Update finished with state: {state}")
            break
        time.sleep(15)

print("[INFO] Pipeline start_pipeline task complete.")
