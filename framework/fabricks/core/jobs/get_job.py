from typing import Optional, Union, overload

from pyspark.sql.types import Row

from fabricks.context import Bronzes, Golds, Silvers
from fabricks.core.jobs.base.job import BaseJob
from fabricks.models import get_job_id


@overload
def get_job(*, step: str, job_id: str) -> BaseJob: ...


@overload
def get_job(*, step: str, topic: str, item: str) -> BaseJob: ...


@overload
def get_job(*, row: Row) -> BaseJob: ...


@overload
def get_job(job: str) -> BaseJob: ...


def get_job(
    job: Optional[str] = None,
    step: Optional[str] = None,
    topic: Optional[str] = None,
    item: Optional[str] = None,
    job_id: Optional[str] = None,
    row: Optional[Row] = None,
) -> BaseJob:
    """
    Retrieve a job based on the provided parameters.

    Args:
        step (Optional[str]): The step of the job.
        topic (Optional[str]): The topic of the job.
        item (Optional[str]): The item of the job.
        job_id (Optional[str]): The ID of the job.
        job (Optional[str]): The job string.
        row (Optional[Row]): The row object containing job information.

    Returns:
        BaseJob: The retrieved job.

    Raises:
        ValueError: If the required parameters are not provided.

    """
    if row:
        if "step" in row and "topic" in row and "item" in row:
            j = get_job_internal(step=row.step, topic=row.topic, item=row.item)
        elif "step" in row and "job_id" in row:
            j = get_job(step=row.step, job_id=row.job_id)
        elif "job" in row:
            parts = row.job.split(".")
            s = parts[0]
            job_id = get_job_id(job=row.job)
            j = get_job_internal(step=s, job_id=job_id)
        else:
            raise ValueError("step, topic, item or step, job_id or job mandatory")

    elif job:
        parts = job.split(".")
        s = parts[0]
        job_id = get_job_id(job=job)
        j = get_job_internal(step=s, job_id=job_id)

    elif job_id:
        assert step, "step mandatory"
        j = get_job_internal(step=step, job_id=job_id)

    else:
        assert step, "step mandatory"
        assert topic, "topic mandatory"
        assert item, "item mandatory"
        j = get_job_internal(step=step, topic=topic, item=item)

    return j


def get_job_internal(
    step: str,
    topic: Optional[str] = None,
    item: Optional[str] = None,
    job_id: Optional[str] = None,
    conf: Optional[Union[dict, Row]] = None,
):
    if step in Bronzes:
        from fabricks.core.jobs.bronze import Bronze

        if job_id is not None:
            job = Bronze.from_job_id(step=step, job_id=job_id, conf=conf)
        else:
            assert topic
            assert item
            job = Bronze.from_step_topic_item(step=step, topic=topic, item=item, conf=conf)

    elif step in Silvers:
        from fabricks.core.jobs.silver import Silver

        if job_id is not None:
            job = Silver.from_job_id(step=step, job_id=job_id, conf=conf)
        else:
            assert topic
            assert item
            job = Silver.from_step_topic_item(step=step, topic=topic, item=item, conf=conf)

    elif step in Golds:
        from fabricks.core.jobs.gold import Gold

        if job_id is not None:
            job = Gold.from_job_id(step=step, job_id=job_id, conf=conf)
        else:
            assert topic
            assert item
            job = Gold.from_step_topic_item(step=step, topic=topic, item=item, conf=conf)

    else:
        raise ValueError(f"{step} not found")

    return job
