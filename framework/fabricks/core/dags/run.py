import json
from typing import Callable, overload

from databricks.sdk.runtime import dbutils

from fabricks.core.dags.log import LOGGER, TABLE_LOG_HANDLER
from fabricks.core.jobs import Bronze, Gold, Silver, get_job
from fabricks.core.jobs.base.exception import CheckWarning, SkipWarning


@overload
def run(
    step: str,
    *,
    job_id: str,
    schedule_id: str | None = None,
    schedule: str | None = None,
    notebook_id: str | None = None,
    pre_run_callable: Callable | None = None,
    post_run_callable: Callable | None = None,
    data: dict | None = None,
    **kwargs,
) -> None: ...


@overload
def run(
    step: str,
    topic: str,
    item: str,
    *,
    schedule_id: str | None = None,
    schedule: str | None = None,
    notebook_id: str | None = None,
    pre_run_callable: Callable | None = None,
    post_run_callable: Callable | None = None,
    data: dict | None = None,
    **kwargs,
) -> None: ...


@overload
def run(
    *,
    job: Bronze | Silver | Gold,
    schedule_id: str | None = None,
    schedule: str | None = None,
    notebook_id: str | None = None,
    pre_run_callable: Callable | None = None,
    post_run_callable: Callable | None = None,
    data: dict | None = None,
    **kwargs,
) -> None: ...


def run(
    step: str | None = None,
    topic: str | None = None,
    item: str | None = None,
    job_id: str | None = None,
    job: Bronze | Silver | Gold | None = None,
    schedule_id: str | None = None,
    schedule: str | None = None,
    notebook_id: str | None = None,
    pre_run_callable: Callable | None = None,
    post_run_callable: Callable | None = None,
    data: dict | None = None,
    **kwargs,
) -> None:
    if job is None:
        if step is not None and job_id is not None:
            job = get_job(step=step, job_id=job_id)
        elif step is not None and topic is not None and item is not None:
            job = get_job(step=step, topic=topic, item=item)
        else:
            raise ValueError("either job or step+job_id or step+topic+item must be provided")

    assert job is not None

    if schedule_id is None:
        try:
            schedule_id = dbutils.widgets.get("schedule_id")
        except:  # noqa: E722
            schedule_id = None

    if schedule is None:
        try:
            schedule = dbutils.widgets.get("schedule")
        except:  # noqa: E722
            schedule = None

    if notebook_id is None:
        try:
            context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())  # type: ignore
            notebook_id = context.get("tags").get("jobId")
        except:  # noqa: E722
            notebook_id = None

    extra = {
        "partition_key": schedule_id,
        "schedule_id": schedule_id,
        "schedule": schedule,
        "step": step,
        "job": str(job),
        "target": "buffer",
        "notebook_id": notebook_id,
        "json": json.dumps(data),
    }

    LOGGER.info("running", extra=extra)

    try:
        if pre_run_callable is not None:
            LOGGER.debug("invoke pre-run callable", extra=extra)
            pre_run_callable(**kwargs)

        job.run(**kwargs)
        LOGGER.info("done", extra=extra)

        if post_run_callable is not None:
            LOGGER.debug("invoke post-run callable", extra=extra)
            post_run_callable(**kwargs)

    except SkipWarning:
        LOGGER.exception("skipped", extra=extra)

    except CheckWarning:
        LOGGER.exception("warned", extra=extra)

    except Exception as e:
        LOGGER.exception("failed", extra=extra)
        raise e

    finally:
        TABLE_LOG_HANDLER.flush()
