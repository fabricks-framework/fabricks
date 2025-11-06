from pyspark.sql import DataFrame


def get_dependencies(name: str) -> DataFrame:
    from fabricks.core.dags import DagGenerator

    g = DagGenerator(schedule=name)
    return g.get_dependencies()


def get_mermaid_diagram(name: str) -> str:
    from fabricks.utils.mermaid import get_mermaid_diagram as get_diagram

    df = get_dependencies(name)

    df = df.withColumnRenamed("ParentId", "parent_id")
    df = df.withColumnRenamed("Parent", "parent")
    df = df.withColumnRenamed("JobId", "job_id")
    df = df.withColumnRenamed("Job", "job")

    return get_diagram(df)
