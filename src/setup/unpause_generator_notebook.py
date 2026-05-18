# Databricks notebook source
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.."))

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
