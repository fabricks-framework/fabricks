from typing import Optional, Union, overload

from pyspark.sql.types import Row

from fabricks.context import IS_JOB_CONFIG_FROM_YAML, SPARK, Bronzes, Golds, Silvers
from fabricks.models import JobConf, get_job_id


def get_job_conf_internal(step: str, row: Union[Row, dict]) -> JobConf:
    if isinstance(row, Row):
        row = row.asDict(recursive=True)

    # Add step to row data (job_id will be computed automatically)
    row["step"] = step

    # Use Pydantic validation - handles nested models and validation automatically
    if step in Bronzes:
        from fabricks.models import JobConfBronze

        return JobConfBronze.model_validate(row)

    elif step in Silvers:
        from fabricks.models import JobConfSilver

        return JobConfSilver.model_validate(row)

    elif step in Golds:
        from fabricks.models import JobConfGold

        return JobConfGold.model_validate(row)

    else:
        raise ValueError(f"{step} not found")


@overload
def get_job_conf(step: str, *, job_id: str, row: Optional[Union[Row, dict]] = None) -> JobConf: ...


@overload
def get_job_conf(step: str, *, topic: str, item: str, row: Optional[Union[Row, dict]] = None) -> JobConf: ...


def get_job_conf(
    step: str,
    job_id: Optional[str] = None,
    topic: Optional[str] = None,
    item: Optional[str] = None,
    row: Optional[Union[Row, dict]] = None,
) -> JobConf:
    if row:
        return get_job_conf_internal(step=step, row=row)

    if IS_JOB_CONFIG_FROM_YAML:
        from fabricks.core.steps import get_step

        s = get_step(step=step)
        if topic:
            iter = s.get_jobs_iter(topic=topic)
        else:
            iter = s.get_jobs_iter()

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

        elif topic and item:
            conf = next(
                (i for i in iter if i.get("topic") == topic and i.get("item") == item),
                None,
            )
            if not conf:
                raise ValueError(f"job not found ({step}, {topic}, {item})")

            return get_job_conf_internal(step=step, row=conf)

    else:
        df = SPARK.sql(f"select * from fabricks.{step}_jobs")

    assert df, f"{step} not found"

    if job_id:
        try:
            row = df.where(f"job_id == '{job_id}'").collect()[0]
        except IndexError:
            raise ValueError(f"job not found ({step}, {job_id})")
    else:
        try:
            row = df.where(f"topic == '{topic}' and item == '{item}'").collect()[0]
        except IndexError:
            raise ValueError(f"job not found ({step}, {topic}, {item})")

    return get_job_conf_internal(step=step, row=row)
