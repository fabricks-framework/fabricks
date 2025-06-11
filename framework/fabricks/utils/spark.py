import os
from typing import Optional, Tuple

from databricks.sdk.dbutils import RemoteDbUtils
from pyspark.sql import DataFrame, SparkSession


def get_spark_session() -> Tuple[SparkSession, RemoteDbUtils]:
    localmode = os.getenv("DATABRICKS_LOCALMODE", "false").lower() in ("true", "1", "yes")

    if localmode:
        from databricks.connect.session import DatabricksSession
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.core import Config

        cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
        assert cluster_id, "DATABRICKS_CLUSTER_ID environment variable is not set"

        profile = os.getenv("DATABRICKS_PROFILE", "DEFAULT")

        w = WorkspaceClient()
        c = Config(profile=profile, cluster_id=cluster_id)

        dbutils = w.dbutils
        spark = DatabricksSession.builder.sdkConfig(c).getOrCreate()

    else:
        from databricks.sdk.runtime import dbutils

        spark = SparkSession.builder.getOrCreate()  # type: ignore
        dbutils = dbutils

    assert spark is not None

    return spark, dbutils  # type: ignore


def display(df: DataFrame, limit: Optional[int] = None) -> None:
    """
    Display a Spark DataFrame in Databricks notebook or local environment.
    If running in local mode, it converts the DataFrame to a Pandas DataFrame for display.
    """
    localmode = os.getenv("DATABRICKS_LOCALMODE", "false").lower() in ("true", "1", "yes")

    if localmode:
        from IPython.display import display

        if limit is not None:
            df = df.limit(limit)
        display(df.toPandas())

    else:
        from databricks.sdk.runtime import display

        if limit is not None:
            df = df.limit(limit)
        display(df)


spark, dbutils = get_spark_session()
