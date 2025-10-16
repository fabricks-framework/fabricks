from pyspark.sql import DataFrame


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
