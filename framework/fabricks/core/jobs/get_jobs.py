from typing import List, Literal, Optional, Union, overload

from pydantic import BaseModel
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.types import Row
from sparkdantic import create_spark_schema

from fabricks.context import IS_JOB_CONFIG_FROM_YAML, PATHS_RUNTIME, SPARK
from fabricks.core.jobs.base.job import BaseJob
from fabricks.core.jobs.get_job import get_job, get_job_internal
from fabricks.core.read import read_yaml
from fabricks.models import AllowedModes
from fabricks.utils.helpers import concat_dfs, run_in_parallel
from fabricks.utils.path import GitPath
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
    if IS_JOB_CONFIG_FROM_YAML:
        schema = create_spark_schema(JobConfGeneric)

        def _read_yaml(path: GitPath):
            df = SPARK.createDataFrame(
                read_yaml(path, root="job"),
                schema=schema,
            )
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
    """
    Sort a job DataFrame by explicit dependency edges and return as a new DataFrame.

    This function uses the pure Python topological_sort algorithm from fabricks.utils
    and wraps it for Spark DataFrame usage.

    Args:
        jobs_df: DataFrame with 'job_id' column and any other job metadata
        dependencies_df: Optional DataFrame with 'job_id' and 'parent_id' columns.
                        If None or empty, jobs are returned in original order.

    Returns:
        DataFrame with same schema as jobs_df, rows sorted in topological order
        (dependencies first)

    Raises:
        CyclicDependencyError: If circular dependencies are detected

    Example:
        >>> jobs = spark.createDataFrame([
        ...     ("job_a", "topic1"),
        ...     ("job_b", "topic1"),
        ...     ("job_c", "topic2")
        ... ], ["job_id", "topic"])
        >>> deps = spark.createDataFrame([
        ...     ("dep1", "sql", "job_b", "job_a", "job_a"),
        ...     ("dep2", "sql", "job_c", "job_b", "job_b")
        ... ], ["dependency_id", "origin", "job_id", "parent", "parent_id"])
        >>> sorted_jobs = get_sorted_job_dataframe_from_dependencies(jobs, deps)
        >>> [row.job_id for row in sorted_jobs.collect()]
        ['job_a', 'job_b', 'job_c']
    """
    # Collect all job rows
    job_rows: List[Row] = jobs_df.collect()

    # If no jobs, return empty DataFrame
    if not job_rows:
        return jobs_df.limit(0)

    # If no dependencies, return jobs as-is
    if dependencies_df is None or dependencies_df.isEmpty():
        return jobs_df

    # Collect dependency edges
    dep_edges = dependencies_df.select("job_id", "parent_id").collect()

    # Build items list (job_id, row) and dependencies list (child_id, parent_id)
    items = [(row.job_id, row) for row in job_rows]
    dependencies = [(edge.job_id, edge.parent_id) for edge in dep_edges]

    # Sort using pure Python topological sort
    sorted_items = topological_sort_with_data(items, dependencies)

    # Extract sorted rows
    sorted_rows = [row for _, row in sorted_items]

    # Create new DataFrame from sorted rows, preserving schema
    spark = jobs_df.sparkSession
    return spark.createDataFrame(sorted_rows, schema=jobs_df.schema)
