from __future__ import annotations

from functools import lru_cache
from typing import Iterable, Optional, Protocol, Union, overload

from pyspark.sql import DataFrame
from pyspark.sql.types import Row

from fabricks.context import IS_JOB_CONFIG_FROM_YAML, PATHS_RUNTIME, SPARK, Bronzes, Golds, Silvers
from fabricks.core.read import read_yaml
from fabricks.models import JobConf, get_job_id
from fabricks.models.job import JobConfBronze, JobConfGold, JobConfSilver


def _validate_conf(step: str, row: dict | Row) -> JobConf:
    if isinstance(row, Row):
        row = row.asDict(recursive=True)
    row["step"] = step
    if step in Bronzes:
        return JobConfBronze.model_validate(row)
    elif step in Silvers:
        return JobConfSilver.model_validate(row)
    elif step in Golds:
        return JobConfGold.model_validate(row)
    else:
        raise ValueError(f"{step} not found")


class JobConfigLoader(Protocol):
    def get_conf(
        self,
        step: str,
        *,
        job_id: Optional[str] = None,
        topic: Optional[str] = None,
        item: Optional[str] = None,
    ) -> JobConf: ...

    def list_config(self, step: str, topic: Optional[str] = None) -> Iterable[dict]: ...

    def get_jobs_df(self) -> DataFrame: ...


class YamlJobConfigLoader(JobConfigLoader):
    def _iter(self, step: str, topic: Optional[str] = None) -> Iterable[dict]:
        from fabricks.core.steps import get_step

        s = get_step(step=step)
        if topic:
            yield from s.get_jobs_iter(topic=topic)
        else:
            yield from s.get_jobs_iter()

    def get_conf(
        self,
        step: str,
        *,
        job_id: Optional[str] = None,
        topic: Optional[str] = None,
        item: Optional[str] = None,
    ) -> JobConf:
        iter_ = self._iter(step, topic=topic)

        if job_id:
            conf = next(
                (
                    i
                    for i in iter_
                    if i.get("job_id", get_job_id(step=i["step"], topic=i["topic"], item=i["item"])) == job_id
                ),
                None,
            )
            if not conf:
                raise ValueError(f"job not found ({step}, {job_id})")
        else:
            conf = next(
                (i for i in iter_ if i.get("topic") == topic and i.get("item") == item),
                None,
            )
            if not conf:
                raise ValueError(f"job not found ({step}, {topic}, {item})")

        return _validate_conf(step=step, row=conf)

    def list_config(self, step: str, topic: Optional[str] = None) -> Iterable[dict]:
        yield from self._iter(step, topic=topic)

    def get_jobs_df(self) -> DataFrame:
        from pyspark.sql.functions import expr
        from sparkdantic import create_spark_schema

        from fabricks.core.jobs.get_jobs import JobConfGeneric
        from fabricks.utils.helpers import concat_dfs, run_in_parallel
        from fabricks.utils.path import GitPath

        schema = create_spark_schema(JobConfGeneric)

        def _read_yaml(path: GitPath):
            df = SPARK.createDataFrame(read_yaml(path, root="job"), schema=schema)
            if df:
                df = df.withColumn("job_id", expr("md5(concat(step,'.',topic,'_',item))"))
                return df

        dfs = run_in_parallel(_read_yaml, list(PATHS_RUNTIME.values()))
        df = concat_dfs(dfs)
        assert df is not None
        return df


class DeltaJobConfigLoader(JobConfigLoader):
    def get_conf(
        self,
        step: str,
        *,
        job_id: Optional[str] = None,
        topic: Optional[str] = None,
        item: Optional[str] = None,
    ) -> JobConf:
        df = SPARK.sql(f"select * from fabricks.{step}_jobs")

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

        return _validate_conf(step=step, row=row)

    def list_config(self, step: str, topic: Optional[str] = None) -> Iterable[dict]:
        df = SPARK.sql(f"select * from fabricks.{step}_jobs")
        if topic:
            df = df.where(f"topic == '{topic}'")
        for row in df.collect():
            yield row.asDict(recursive=True)

    def get_jobs_df(self) -> DataFrame:
        return SPARK.sql("select * from fabricks.jobs")


@lru_cache(maxsize=1)
def get_config_loader() -> JobConfigLoader:
    if IS_JOB_CONFIG_FROM_YAML:
        return YamlJobConfigLoader()
    return DeltaJobConfigLoader()


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
        return _validate_conf(step=step, row=row)

    return get_config_loader().get_conf(step, job_id=job_id, topic=topic, item=item)
