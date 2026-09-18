from typing import overload

from pyspark.sql.types import Row

from fabricks.context import IS_JOB_CONFIG_FROM_YAML, SPARK, Bronzes, Golds, Silvers
from fabricks.models import JobConf, get_job_id


def get_job_conf_internal(step: str, row: Row | dict) -> JobConf:
    if isinstance(row, Row):
        row = row.asDict(recursive=True)

    # Add step to row data (job_id will be computed automatically)
    row["step"] = step

    # Use Pydantic validation - handles nested models and validation automatically
    if step in Bronzes:
        from fabricks.models import JobConfBronze

        return JobConfBronze.model_validate(row)

    if step in Silvers:
        from fabricks.models import JobConfSilver

        return JobConfSilver.model_validate(row)

    if step in Golds:
        from fabricks.models import JobConfGold

        return JobConfGold.model_validate(row)

    raise ValueError(f"{step} not found")


@overload
def get_job_conf(step: str, *, job_id: str, row: Row | dict | None = None) -> JobConf: ...


@overload
def get_job_conf(step: str, *, topic: str, item: str, row: Row | dict | None = None) -> JobConf: ...


def get_job_conf(
    step: str,
    job_id: str | None = None,
    topic: str | None = None,
    item: str | None = None,
    row: Row | dict | None = None,
) -> JobConf:
    if row:
        return get_job_conf_internal(step=step, row=row)

    if IS_JOB_CONFIG_FROM_YAML:
        from fabricks.core.steps import get_step

        s = get_step(step=step)
        iter = s.get_jobs_iter(topic=topic) if topic else s.get_jobs_iter()

        if job_id:
            conf = next(
                (
                    i
                    for i in iter
                    if i.get("job_id", get_job_id(step=i["step"], topic=i["topic"], item=i["item"])) == job_id
                ),
                None,
            )
            if not conf:
                raise ValueError(f"job not found ({step}, {job_id})")

            return get_job_conf_internal(step=step, row=conf)

        if topic and item:
            conf = next((i for i in iter if i.get("topic") == topic and i.get("item") == item), None)
            if not conf:
                raise ValueError(f"job not found ({step}, {topic}, {item})")

            return get_job_conf_internal(step=step, row=conf)

    else:
        df = SPARK.sql(f"select * from fabricks.{step}_jobs")

    assert df, f"{step} not found"

    if job_id:
        try:
            row = df.where(f"job_id == '{job_id}'").collect()[0]
        except IndexError as err:
            raise ValueError(f"job not found ({step}, {job_id})") from err
    else:
        try:
            row = df.where(f"topic == '{topic}' and item == '{item}'").collect()[0]
        except IndexError as err:
            raise ValueError(f"job not found ({step}, {topic}, {item})") from err

    return get_job_conf_internal(step=step, row=row)
