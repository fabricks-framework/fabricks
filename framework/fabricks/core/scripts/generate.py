from typing import Tuple

from pyspark.sql import DataFrame

from fabricks.core.dags.generator import DagGenerator


def generate(schedule: str) -> Tuple[str, DataFrame, DataFrame]:
    """
    Generate a schedule, job dataframe, and dependency dataframe based on the given schedule.

    Args:
        schedule (str): The schedule to generate from.

    Returns:
        Tuple[str, DataFrame, DataFrame]: A tuple containing the schedule ID, job dataframe, and dependency dataframe.
    """
    with DagGenerator(schedule) as g:
        schedule_id, job_df, dep_df = g.generate()
        return schedule_id, job_df, dep_df
