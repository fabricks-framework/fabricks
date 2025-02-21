from typing import List, Optional, TypedDict

from pyspark.sql import DataFrame

from fabricks.context import PATH_SCHEDULES, SPARK
from fabricks.context.log import Logger
from fabricks.core.jobs.base._types import TStep
from fabricks.utils.read.read_yaml import read_yaml
from fabricks.utils.schema import get_schema_for_type
from fabricks.utils.sqlglot import fix as fix_sql


class Options(TypedDict):
    steps: Optional[List[TStep]]
    tag: Optional[str]
    view: Optional[str]
    variables: Optional[dict[str, str]]


class Schedule(TypedDict):
    name: str
    options: Options


def get_schedules() -> DataFrame:
    schema = get_schema_for_type(Schedule)
    df = read_yaml(PATH_SCHEDULES, root="schedule", schema=schema)
    assert df, "no schedules found"
    return df


def get_schedule(name: str) -> DataFrame:
    df = get_schedules()
    df = df.where(f"name == '{name}'")
    assert not df.isEmpty(), "schedule not found"
    assert df.count() == 1, "schedule duplicated"
    return df


def _create_or_replace_view(name: str, options: DataFrame):
    step = "-- no step provided"
    tag = "-- no tag provided"
    view = "-- no view provided"

    if options.steps is not None:
        steps = [f"'{s}'" for s in options.steps]  # type: ignore
        step = f"and j.step in ({', '.join(steps)})"
    if options.tag is not None:
        tag = f"and array_contains(j.tags, '{options.tag}')"
    if options.view is not None:
        view = f"inner join fabricks.{options.view} v on j.job_id = v.job_id"

    sql = f"""
    create or replace view fabricks.{name}_schedule
    as
    select
        j.*
    from
        fabricks.jobs j
        {view}
    where
        true 
        {step}
        {tag}
        and j.type not in ('manual')
    """
    sql = fix_sql(sql)
    Logger.debug(f"schedule - %sql\n---\n{sql}\n---")

    SPARK.sql(sql)


def create_or_replace_view(name: str):
    df = get_schedule(name=name)
    for row in df.collect():
        try:
            _create_or_replace_view(row.name, row.options)
        except Exception:
            Logger.exception(f"schedule - {row.name} not created nor replaced")


def create_or_replace_views():
    df = get_schedules()
    for row in df.collect():
        try:
            _create_or_replace_view(row.name, row.options)
        except Exception:
            Logger.exception(f"schedule - {row.name} not created nor replaced")


def get_dependencies(name: str) -> DataFrame:
    from fabricks.core.dags import DagGenerator

    g = DagGenerator(schedule=name)
    return g.get_dependencies()


def get_mermaid_diagram(name: str) -> str:
    df = get_dependencies(name)

    df = df.withColumnRenamed("ParentId", "parent_id")
    df = df.withColumnRenamed("Parent", "parent")
    df = df.withColumnRenamed("JobId", "job_id")
    df = df.withColumnRenamed("Job", "job")

    dependencies = df.select("parent_id", "parent", "job_id", "job").collect()

    out = "flowchart TD\n"

    unique_nodes = set()

    for row in dependencies:
        parent_id = str(row["parent_id"])
        parent_name = str(row["parent"])
        child_id = str(row["job_id"])
        child_name = str(row["job"])

        if parent_id != "0" and parent_id is not None:
            if parent_id not in unique_nodes:
                out += f"    {parent_id}[{parent_name}]\n"
                unique_nodes.add(parent_id)

            if child_id not in unique_nodes:
                out += f"    {child_id}[{child_name}]\n"
                unique_nodes.add(child_id)

            out += f"    {parent_id} --> {child_id}\n"
        else:
            if child_id not in unique_nodes:
                out += f"    {child_id}[{child_name}]\n"
                unique_nodes.add(child_id)

    return out
