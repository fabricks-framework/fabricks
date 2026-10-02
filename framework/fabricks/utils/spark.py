import os
from typing import Any

from databricks.sdk.dbutils import RemoteDbUtils
from pyspark.sql import DataFrame, SparkSession

from fabricks.utils.environment import FABRICKS_ENVIRONMENT


def get_spark() -> SparkSession:
    if FABRICKS_ENVIRONMENT == "remote":
        from databricks.connect.session import DatabricksSession  # ty: ignore[unresolved-import]
        from databricks.sdk.core import Config

        profile = os.getenv("DATABRICKS_PROFILE", "DEFAULT")

        cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
        assert cluster_id, "DATABRICKS_CLUSTER_ID environment variable is not set"

        c = Config(profile=profile, cluster_id=cluster_id)

        spark = DatabricksSession.builder.sdkConfig(c).getOrCreate()

    elif FABRICKS_ENVIRONMENT == "docker":
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder.appName("fabricks-docker")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.allowMultipleContexts", "true")
            .enableHiveSupport()
        )
        # Static config: only settable on the builder, not after getOrCreate().
        # Set per xdist worker by tests/spark/apache/conftest.py.
        _warehouse_dir = os.environ.get("FABRICKS_TEST_WAREHOUSE_DIR")
        if _warehouse_dir:
            builder = builder.config("spark.sql.warehouse.dir", _warehouse_dir)
        _default_parallelism = os.environ.get("FABRICKS_TEST_SPARK_DEFAULT_PARALLELISM")
        if _default_parallelism:
            builder = builder.config("spark.default.parallelism", _default_parallelism)
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        # Iteration 2+ of the Silver scenarios adds columns to an existing table and
        # bypasses update_schema(), so MERGE needs autoMerge (same as production).
        spark.sql("set spark.databricks.delta.schema.autoMerge.enabled = true")

    else:
        spark = SparkSession.builder.getOrCreate()

    assert spark is not None
    return spark


def display(df: DataFrame, limit: int | None = None) -> None:
    """
    Display a Spark DataFrame. Uses IPython/pandas display outside a native
    Databricks runtime (FABRICKS_ENVIRONMENT != "databricks"); the
    Databricks-injected display otherwise.
    """
    if FABRICKS_ENVIRONMENT != "databricks":
        from IPython.display import display

        if limit is not None:
            df = df.limit(limit)

        display(df.toPandas())

    else:
        from databricks.sdk.runtime import display

        if limit is not None:
            df = df.limit(limit)

        display(df)


def get_dbutils(spark: SparkSession | None = None) -> RemoteDbUtils | None:
    try:
        dbutils: Any  # RemoteDbUtils (remote) or the runtime's DBUtils (databricks/docker)
        if FABRICKS_ENVIRONMENT == "remote":
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            dbutils = w.dbutils

        else:
            from pyspark.dbutils import DBUtils  # ty: ignore[unresolved-import]

            dbutils = DBUtils(spark)

        assert dbutils is not None
        return dbutils

    except Exception:
        return None


spark = get_spark()
dbutils = get_dbutils(spark=spark)
