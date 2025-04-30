from dataclasses import dataclass
from typing import List, Literal, Optional, TypedDict, Union, overload

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.types import Row

from fabricks.context import IS_JOB_CONFIG_FROM_YAML, PATHS_RUNTIME, SPARK
from fabricks.core.jobs.base._types import Modes, TStep
from fabricks.core.jobs.base.job import BaseJob
from fabricks.core.jobs.get_job import get_job, get_job_internal
from fabricks.utils.helpers import concat_dfs, run_in_parallel
from fabricks.utils.path import Path
from fabricks.utils.read import read_yaml
from fabricks.utils.schema import get_schema_for_type


class GenericOptions(TypedDict):
    mode: Modes


@dataclass
class JobConfGeneric:
    step: TStep
    job_id: str
    topic: str
    item: str
    options: GenericOptions


def _get_job(row: Row):
    return get_job(row=row)


def get_jobs_internal():
    for p in PATHS_RUNTIME.values():
        yield from read_yaml(p, root="job")


def get_jobs_internal_df() -> DataFrame:
    if IS_JOB_CONFIG_FROM_YAML:
        schema = get_schema_for_type(JobConfGeneric)

        def _read_yaml(path: Path):
            df = SPARK.createDataFrame(read_yaml(path, root="job"), schema=schema)  # type: ignore
            if df:
                df = df.withColumn("job_id", expr("md5(concat(step,'.',topic,'_',item))"))
                return df

        dfs = run_in_parallel(_read_yaml, list(PATHS_RUNTIME.values()))
        df = concat_dfs(dfs)
        assert df is not None

    else:
        df = SPARK.sql("select * from fabricks.jobs")

    return df


@overload
def get_jobs(df: Optional[DataFrame] = None, *, convert: Literal[True]) -> List[BaseJob]: ...


@overload
def get_jobs(df: Optional[DataFrame] = None, *, convert: Literal[False]) -> DataFrame: ...


def get_jobs(df: Optional[DataFrame] = None, convert: Optional[bool] = False) -> Union[List[BaseJob], DataFrame]:
    """
    Retrieves a list of jobs or a DataFrame containing job information.

    Args:
        df (Optional[DataFrame]): Optional DataFrame containing job information.
        convert (Optional[bool]): Flag indicating whether to convert the DataFrame to a list of jobs.

    Returns:
        Union[List[BaseJob], DataFrame]: If `convert` is False, returns a list of BaseJob objects.
                                         If `convert` is True, returns a DataFrame with selected columns.

    Raises:
        ValueError: If the DataFrame does not contain the required columns.

    """
    if not convert:
        return get_jobs_internal_df()

    else:
        if df is None:
            return list(
                get_job_internal(j["step"], j["topic"], j["item"], j.get("job_id"), conf=j)
                for j in get_jobs_internal()
            )

        else:
            if "step" in df.columns and "topic" in df.columns and "item" in df.columns:
                df = df.select("step", "topic", "item")
            elif "step" in df.columns and "job_id" in df.columns:
                df = df.select("step", "job_id")
            elif "job" in df.columns:
                df = df.select("job")
            else:
                raise ValueError("step, topic, item or step, job_id or job mandatory")

        assert df

        jobs = run_in_parallel(_get_job, df)
        return jobs
