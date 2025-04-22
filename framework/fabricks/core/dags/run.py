from typing import Optional

from fabricks.core.dags.log import DagsLogger, DagsTableLogger
from fabricks.core.jobs import get_job


def run(step: str, job_id: str, schedule_id: str, schedule: str, notebook_id: Optional[str] = None):
    job = get_job(step=step, job_id=job_id)

    extra = {
        "partition_key": schedule_id,
        "schedule_id": schedule_id,
        "schedule": schedule,
        "step": step,
        "job": job,
        "target": "buffer",
    }
    if notebook_id is not None:
        extra["notebook_id"] = notebook_id

    DagsLogger.info("running", extra=extra)

    try:
        job.run(schedule_id=schedule_id, schedule=schedule)
        DagsLogger.info("done", extra=extra)

    except Exception as e:
        DagsLogger.exception("failed", extra=extra)
        raise e

    finally:
        DagsTableLogger.flush()
