from typing import Optional, cast, overload

from pyspark.sql import Row

from fabricks.context import IS_LIVE, SPARK
from fabricks.core.jobs.base._types import Bronzes, Golds, JobConf, Silvers, TBronze, TGold, TSilver, TStep


@overload
def get_job_conf(step: TStep, *, job_id: str) -> JobConf: ...


@overload
def get_job_conf(step: TStep, *, topic: str, item: str) -> JobConf: ...


def _get_job_conf(step: TStep, row: Row) -> JobConf:
    options = row["options"].asDict() if row["options"] else None
    table_options = row["table_options"].asDict() if row["table_options"] else None
    check_options = row["check_options"].asDict() if row["check_options"] else None
    spark_options = row["spark_options"].asDict() if row["spark_options"] else None
    invoker_options = row["invoker_options"].asDict() if row["invoker_options"] else None
    extender_options = row["extender_options"] if row["extender_options"] else None

    if step in Bronzes:
        from fabricks.core.jobs.base._types import JobConfBronze

        assert options is not None, "no option"
        parser_options = row["parser_options"].asDict() if row["parser_options"] else None
        step = cast(TBronze, step)
        return JobConfBronze(
            job_id=row["job_id"],
            topic=row["topic"],
            item=row["item"],
            step=step,
            options=options,
            parser_options=parser_options,
            table_options=table_options,
            check_options=check_options,
            invoker_options=invoker_options,
            extender_options=extender_options,
            spark_options=spark_options,
            tags=row["tags"],
        )

    elif step in Silvers:
        from fabricks.core.jobs.base._types import JobConfSilver

        assert options is not None, "no option"
        step = cast(TSilver, step)
        return JobConfSilver(
            job_id=row["job_id"],
            topic=row["topic"],
            item=row["item"],
            step=step,
            options=options,
            table_options=table_options,
            check_options=check_options,
            invoker_options=invoker_options,
            extender_options=extender_options,
            spark_options=spark_options,
            tags=row["tags"],
        )

    elif step in Golds:
        from fabricks.core.jobs.base._types import JobConfGold

        assert options is not None, "no option"
        step = cast(TGold, step)
        return JobConfGold(
            job_id=row["job_id"],
            topic=row["topic"],
            item=row["item"],
            step=step,
            options=options,
            table_options=table_options,
            check_options=check_options,
            invoker_options=invoker_options,
            extender_options=extender_options,
            spark_options=spark_options,
            tags=row["tags"],
        )

    else:
        raise ValueError(f"{step} not found")


def get_job_conf(
    step: TStep,
    job_id: Optional[str] = None,
    topic: Optional[str] = None,
    item: Optional[str] = None,
) -> JobConf:
    if IS_LIVE:
        from fabricks.core.steps import get_step

        s = get_step(step=step)
        if topic:
            df = s.get_jobs(topic=topic)
        else:
            df = s.get_jobs()
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

    return _get_job_conf(step=step, row=row)
