# Databricks notebook source
import sys

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_notebook_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

# COMMAND ----------

dbutils.widgets.text("generator_job_id", "")
generator_job_id = int(dbutils.widgets.get("generator_job_id"))

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import CronSchedule, PauseStatus

w = WorkspaceClient()
job = w.jobs.get(generator_job_id)
settings = job.settings
settings.schedule.pause_status = PauseStatus.UNPAUSED
w.jobs.update(job_id=generator_job_id, new_settings=settings)
print(f"[INFO] Generator job {generator_job_id} unpaused successfully")
