from typing import List, Literal, Optional, Union, overload

from pydantic import BaseModel
from pyspark.sql import DataFrame
from pyspark.sql.types import Row

from fabricks.context import PATHS_RUNTIME
from fabricks.core.jobs.base import BaseJob
from fabricks.core.jobs.get_job import get_job, get_job_internal
from fabricks.core.jobs.get_job_conf import get_config_loader
from fabricks.core.read import read_yaml
from fabricks.models import AllowedModes
from fabricks.utils.helpers import run_in_parallel
from fabricks.utils.sort import topological_with_data as topological_sort_with_data


class GenericOptions(BaseModel):
    mode: AllowedModes


class JobConfGeneric(BaseModel):
    step: str
    job_id: str
    topic: str
    item: str
    options: GenericOptions


def _get_job(row: Row):
    return get_job(row=row)


def get_jobs_internal():
    """Yield job configurations from YAML files with variable substitution."""
    for p in PATHS_RUNTIME.values():
        yield from read_yaml(p, root="job")


def get_jobs_internal_df() -> DataFrame:
    """Get jobs as a DataFrame with variable substitution."""
    return get_config_loader().get_jobs_df()


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
                get_job_internal(
                    j["step"],
                    j["topic"],
                    j["item"],
                    j.get("job_id"),
                    conf=j,
                )
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


def get_jobs_sorted(
    jobs_df: DataFrame,
    dependencies_df: Optional[DataFrame] = None,
) -> DataFrame:
    job_rows: List[Row] = jobs_df.collect()

    if not job_rows:
        return jobs_df.limit(0)

    if dependencies_df is None:
        return jobs_df

    dep_edges = dependencies_df.select("job_id", "parent_id").collect()
    if not dep_edges:
        return jobs_df

    items = [(row.job_id, row) for row in job_rows]
    dependencies = [(edge.job_id, edge.parent_id) for edge in dep_edges]
    sorted_items = topological_sort_with_data(items, dependencies)

    spark = jobs_df.sparkSession
    return spark.createDataFrame([row for _, row in sorted_items], schema=jobs_df.schema)
