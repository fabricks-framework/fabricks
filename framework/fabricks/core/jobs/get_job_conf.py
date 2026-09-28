from functools import cache
from typing import overload

from pyspark.sql.types import Row

from fabricks.context import IS_JOB_CONFIG_FROM_YAML, SPARK, Bronzes, Golds, Silvers
from fabricks.models import JobConf, get_job_id


def _row_dict(row: Row | dict) -> dict:
    return row.asDict(recursive=True) if isinstance(row, Row) else row


@cache
def _get_step_rows(step: str) -> tuple[dict, ...]:
    """All job rows for a step, normalized to dicts and cached for the process. See clear_job_conf_cache()."""
    if IS_JOB_CONFIG_FROM_YAML:
        from fabricks.core.steps import get_step

        rows = get_step(step=step).get_jobs_iter()
    else:
        rows = SPARK.sql(f"select * from fabricks.{step}_jobs").collect()

    return tuple(_row_dict(r) for r in rows)


def clear_job_conf_cache() -> None:
    """Drop the cached per-step job rows (e.g. after YAML/metastore changes within a long-lived process)."""
    _get_step_rows.cache_clear()


def get_job_conf_internal(step: str, row: Row | dict) -> JobConf:
    row = _row_dict(row)

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

    rows = _get_step_rows(step)
    assert rows, f"{step} not found"

    if job_id:
        conf = next(
            (
                d
                for d in rows
                if (d.get("job_id") or get_job_id(step=step, topic=d["topic"], item=d["item"])) == job_id
            ),
            None,
        )
        if conf is None:
            raise ValueError(f"job not found ({step}, {job_id})")

        return get_job_conf_internal(step=step, row=conf)

    if topic and item:
        conf = next((d for d in rows if d.get("topic") == topic and d.get("item") == item), None)
        if conf is None:
            raise ValueError(f"job not found ({step}, {topic}, {item})")

        return get_job_conf_internal(step=step, row=conf)

    raise ValueError("job_id or topic+item mandatory")
