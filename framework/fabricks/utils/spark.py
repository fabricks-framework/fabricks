import os
from typing import Final, Optional

from databricks.sdk.dbutils import RemoteDbUtils
from pyspark.sql import DataFrame, SparkSession

DATABRICKS_LOCALMODE: Final[bool] = os.getenv("DATABRICKS_LOCALMODE", "false").lower() in ("true", "1", "yes")


def get_spark() -> SparkSession:
    if DATABRICKS_LOCALMODE:
        from databricks.connect.session import DatabricksSession
        from databricks.sdk.core import Config

        profile = os.getenv("DATABRICKS_PROFILE", "DEFAULT")

        cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
        assert cluster_id, "DATABRICKS_CLUSTER_ID environment variable is not set"

        c = Config(profile=profile, cluster_id=cluster_id)

        spark = DatabricksSession.builder.sdkConfig(c).getOrCreate()

    else:
        pass

        spark = SparkSession.builder.getOrCreate()  # type: ignore

    assert spark is not None
    return spark  # type: ignore


def display(df: DataFrame, limit: Optional[int] = None) -> None:
    """
    Display a Spark DataFrame in Databricks notebook or local environment.
    If running in local mode, it converts the DataFrame to a Pandas DataFrame for display.
    """
    if DATABRICKS_LOCALMODE:
        from IPython.display import display

        if limit is not None:
            df = df.limit(limit)

        display(df.toPandas())

    else:
        from databricks.sdk.runtime import display

        if limit is not None:
            df = df.limit(limit)

        display(df)


def get_dbutils(spark: Optional[SparkSession] = None) -> Optional[RemoteDbUtils]:
    try:
        if DATABRICKS_LOCALMODE:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            dbutils = w.dbutils

        else:
            from pyspark.dbutils import DBUtils

            dbutils = DBUtils(spark)

        assert dbutils is not None
        return dbutils  # type: ignore

    except Exception:
        return None


spark = get_spark()
dbutils = get_dbutils(spark=spark)
