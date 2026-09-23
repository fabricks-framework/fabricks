from typing import Literal, overload

from pyspark.sql.types import Row

from fabricks.context import Bronzes, Golds, Silvers
from fabricks.core.jobs.bronze import Bronze
from fabricks.core.jobs.gold import Gold
from fabricks.core.jobs.orphan import OrphanJob
from fabricks.core.jobs.silver import Silver
from fabricks.models import get_job_id


@overload
def get_job(*, step: str, job_id: str) -> Bronze | Gold | Silver: ...


@overload
def get_job(*, step: str, topic: str, item: str, orphan: Literal[False] = False) -> Bronze | Gold | Silver: ...


@overload
def get_job(*, step: str, topic: str, item: str, orphan: Literal[True]) -> OrphanJob: ...


@overload
def get_job(*, row: Row) -> Bronze | Gold | Silver: ...


@overload
def get_job(job: str) -> Bronze | Gold | Silver: ...


def get_job(
    job: str | None = None,
    step: str | None = None,
    topic: str | None = None,
    item: str | None = None,
    job_id: str | None = None,
    row: Row | None = None,
    orphan: bool = False,
) -> Bronze | Gold | Silver | OrphanJob:
    """
    Retrieve a job based on the provided parameters.

    Args:
        step (Optional[str]): The step of the job.
        topic (Optional[str]): The topic of the job.
        item (Optional[str]): The item of the job.
        job_id (Optional[str]): The ID of the job.
        job (Optional[str]): The job string.
        row (Optional[Row]): The row object containing job information.
        orphan (bool): If True, return an OrphanJob built from step/topic/item
            alone, with no config lookup -- for a job that's been removed
            from the runtime, see https://github.com/fabricks-framework/
            fabricks/issues/198. Requires step, topic and item; incompatible
            with job, job_id and row, since those all need a resolvable
            config to look the job up by.

    Returns:
        BaseJob: The retrieved job.

    Raises:
        ValueError: If the required parameters are not provided.

    """
    if orphan:
        assert step, "step mandatory"
        assert topic, "topic mandatory"
        assert item, "item mandatory"
        assert job is None, "orphan jobs are only resolved from step/topic/item, not job"
        assert job_id is None, "orphan jobs are only resolved from step/topic/item, not job_id"
        assert row is None, "orphan jobs are only resolved from step/topic/item, not row"
        return OrphanJob(step=step, topic=topic, item=item)

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
    topic: str | None = None,
    item: str | None = None,
    job_id: str | None = None,
    conf: dict | Row | None = None,
) -> Bronze | Gold | Silver:
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
