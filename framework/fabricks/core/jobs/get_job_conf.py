import hashlib
from typing import Optional, Union, cast, overload

from pyspark.sql import Row

from fabricks.context import IS_LIVE, SPARK
from fabricks.core.jobs.base._types import Bronzes, Golds, JobConf, Silvers, TBronze, TGold, TSilver, TStep


@overload
def get_job_conf(step: TStep, *, job_id: str, row: Optional[Union[Row, dict]] = None) -> JobConf: ...


@overload
def get_job_conf(step: TStep, *, topic: str, item: str, row: Optional[Union[Row, dict]] = None) -> JobConf: ...


def _get_job_conf(step: TStep, row: Union[Row, dict]) -> JobConf:
    if isinstance(row, Row):
        row = row.asDict(recursive=True)
    options = row.get("options")
    table_options = row.get("table_options")
    check_options = row.get("check_options")
    spark_options = row.get("spark_options")
    invoker_options = row.get("invoker_options")
    extender_options = row.get("extender_options")

    job_id = row.get("job_id", _get_job_id(step, row["topic"], row["item"]))

    if step in Bronzes:
        from fabricks.core.jobs.base._types import JobConfBronze

        assert options is not None, "no option"
        parser_options = row["parser_options"] if row["parser_options"] else None
        step = cast(TBronze, step)
        return JobConfBronze(
            job_id=job_id,
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
            tags=row["tags"],
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
            tags=row["tags"],
        )

    else:
        raise ValueError(f"{step} not found")


def _get_job_id(step: str, topic: str, item: str):
    return hashlib.md5(f"{step}.{topic}_{item}".encode()).hexdigest()


def get_job_conf(
    step: TStep,
    job_id: Optional[str] = None,
    topic: Optional[str] = None,
    item: Optional[str] = None,
    row: Optional[Union[Row, dict]] = None,
) -> JobConf:
    if row:
        return _get_job_conf(step=step, row=row)
    if IS_LIVE:
        from fabricks.core.steps import get_step

        s = get_step(step=step)
        if topic:
            ls_iter = s.get_jobs_iter(topic=topic)
        else:
            ls_iter = s.get_jobs_iter()

        if job_id:
            job_conf = next(
                (x for x in ls_iter if x.get("job_id", _get_job_id(x["step"], x["topic"], x["item"])) == job_id), None
            )
            if not job_conf:
                raise ValueError(f"job not found ({step}, {job_id})")
            return _get_job_conf(step=step, row=job_conf)

        elif topic and item:
            job_conf = next((x for x in ls_iter if x.get("topic") == topic and x.get("item") == item), None)
            if not job_conf:
                raise ValueError(f"job not found ({step}, {topic}, {item})")
            return _get_job_conf(step=step, row=job_conf)

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
