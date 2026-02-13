import json
from typing import Callable, overload

from databricks.sdk.runtime import dbutils
from pyspark.errors.exceptions.base import IllegalArgumentException

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

    if schedule_id is None:
        try:
            schedule_id = dbutils.jobs.taskValues.get(taskKey="initialize", key="schedule_id")
        except (TypeError, IllegalArgumentException, ValueError):
            schedule_id = dbutils.widgets.get("schedule_id")

    if schedule is None:
        try:
            schedule = dbutils.jobs.taskValues.get(taskKey="initialize", key="schedule")
        except (TypeError, IllegalArgumentException, ValueError):
            schedule = dbutils.widgets.get("schedule")

    if notebook_id is None:
        try:
            context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())  # type: ignore
            notebook_id = context.get("tags").get("jobId")
        except:  # noqa: E722
            notebook_id = None

    assert job is not None
    assert schedule_id is not None
    assert schedule is not None

    extra = {
        "partition_key": schedule_id,
        "schedule_id": schedule_id,
        "schedule": schedule,
        "step": step,
        "job": str(job),
        "target": "buffer",
    }
    if data is not None:
        extra["json"] = data
    if notebook_id is not None:
        extra["notebook_id"] = notebook_id

    for k, v in extra.items():
        if k not in kwargs:
            kwargs[k] = v

    LOGGER.info("running", extra=extra)

    try:
        if pre_run_callable is not None:
            LOGGER.debug("invoke pre-run callable", extra=extra)
            pre_run_callable(**kwargs)

        # use kwargs to pass retry, invoke, reload, vacuum, optimize, compute_statistics, etc. to the job.run method
        retry = kwargs.get("retry", True)
        invoke = kwargs.get("invoke", True)
        reload = kwargs.get("reload")
        vacuum = kwargs.get("vacuum")
        optimize = kwargs.get("optimize")
        compute_statistics = kwargs.get("compute_statistics")

        job.run(
            schedule=schedule,
            schedule_id=schedule_id,
            retry=retry,
            invoke=invoke,
            reload=reload,
            vacuum=vacuum,
            optimize=optimize,
            compute_statistics=compute_statistics,
        )
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
