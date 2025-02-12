from dataclasses import dataclass
from typing import List, Optional, TypedDict, Union

from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import expr

from fabricks.context import IS_LIVE, PATHS_RUNTIME, SPARK
from fabricks.core.jobs.base._types import Modes, TStep
from fabricks.core.jobs.base.job import BaseJob
from fabricks.core.jobs.get_job import get_job
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


def _get_jobs() -> DataFrame:
    if IS_LIVE:
        schema = get_schema_for_type(JobConfGeneric)

        def _read_yaml(path: Path):
            df = read_yaml(path, root="job", schema=schema)
            if df:
                df = df.withColumn("job_id", expr("md5(concat(step,'.',topic,'_',item))"))
                return df

        dfs = run_in_parallel(_read_yaml, list(PATHS_RUNTIME.values()))
        df = concat_dfs(dfs)

    else:
        df = SPARK.sql("select * from fabricks.jobs")

    return df


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
        return _get_jobs()

    else:
        if df is None:
            df = _get_jobs()
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
