from typing import Optional

from fabricks.core.dags.log import LOGGER, TABLE_LOG_HANDLER
from fabricks.core.jobs import get_job
from fabricks.core.jobs.base.error import CheckWarning, SkipRunCheckWarning


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

    LOGGER.info("running", extra=extra)

    try:
        job.run(schedule_id=schedule_id, schedule=schedule)
        LOGGER.info("done", extra=extra)

    except SkipRunCheckWarning:
        LOGGER.exception("skipped", extra=extra)

    except CheckWarning:
        LOGGER.exception("warned", extra=extra)

    except Exception as e:
        LOGGER.exception("failed", extra=extra)
        raise e

    finally:
        TABLE_LOG_HANDLER.flush()
