from typing import Optional, Union, cast, overload

from pyspark.sql.types import Row

from fabricks.context import IS_JOB_CONFIG_FROM_YAML, SPARK
from fabricks.core.jobs.base._types import Bronzes, Golds, JobConf, Silvers, TBronze, TGold, TSilver, TStep
from fabricks.core.jobs.get_job_id import get_job_id


def get_job_conf_internal(step: TStep, row: Union[Row, dict]) -> JobConf:
    if isinstance(row, Row):
        row = row.asDict(recursive=True)

    options = row.get("options")
    table_options = row.get("table_options")
    check_options = row.get("check_options")
    spark_options = row.get("spark_options")
    invoker_options = row.get("invoker_options")
    extender_options = row.get("extender_options")

    job_id = row.get("job_id", get_job_id(step=step, topic=row["topic"], item=row["item"]))

    if step in Bronzes:
        from fabricks.core.jobs.base._types import JobConfBronze

        assert options is not None, "no option"
        step = cast(TBronze, step)
        return JobConfBronze(
            job_id=job_id,
            topic=row["topic"],
            item=row["item"],
            step=step,
            options=options,
            parser_options=row.get("parser_options"),
            table_options=table_options,
            check_options=check_options,
            invoker_options=invoker_options,
            extender_options=extender_options,
            spark_options=spark_options,
            tags=row.get("tags"),
        )

    elif step in Silvers:
        from fabricks.core.jobs.base._types import JobConfSilver

        assert options is not None, "no option"
        step = cast(TSilver, step)
        return JobConfSilver(
            job_id=job_id,
            topic=row["topic"],
            item=row["item"],
            step=step,
            options=options,
            table_options=table_options,
            check_options=check_options,
            invoker_options=invoker_options,
            extender_options=extender_options,
            spark_options=spark_options,
            tags=row.get("tags"),
        )

    elif step in Golds:
        from fabricks.core.jobs.base._types import JobConfGold

        assert options is not None, "no option"
        step = cast(TGold, step)
        return JobConfGold(
            job_id=job_id,
            topic=row["topic"],
            item=row["item"],
            step=step,
            options=options,
            table_options=table_options,
            check_options=check_options,
            invoker_options=invoker_options,
            extender_options=extender_options,
            spark_options=spark_options,
            tags=row.get("tags"),
        )

    else:
        raise ValueError(f"{step} not found")


@overload
def get_job_conf(step: TStep, *, job_id: str, row: Optional[Union[Row, dict]] = None) -> JobConf: ...


@overload
def get_job_conf(step: TStep, *, topic: str, item: str, row: Optional[Union[Row, dict]] = None) -> JobConf: ...


def get_job_conf(
    step: TStep,
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
