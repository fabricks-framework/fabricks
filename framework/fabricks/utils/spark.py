import os

from databricks.sdk.dbutils import RemoteDbUtils
from pyspark.sql import DataFrame, SparkSession

from fabricks.utils.environment import FABRICKS_ENVIRONMENT


def get_spark() -> SparkSession:
    if FABRICKS_ENVIRONMENT == "remote":
        from databricks.connect.session import DatabricksSession
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
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        # Job-sequential Silver scenarios (Task 9) merge job2+'s data — which
        # introduces columns job1's schema doesn't have (verified: job2 adds
        # `newField`, still present through job9) — into a table whose schema
        # was created from an earlier job. This plan bypasses job
        # orchestration entirely (no Bronze/Silver/Gold classes, no
        # update_schema() between jobs), so without autoMerge a `MERGE INTO`
        # referencing a new source column would fail with a real schema
        # mismatch. Matches production's own fix for this (`add_spark_options_to_spark()`
        # in fabricks/context/spark_session.py already sets this for every
        # real Databricks session) rather than inventing local-only schema-
        # reconciliation logic. Confirmed via Delta Lake's own OSS docs this
        # is a core open-source feature (available since Delta 0.6.0), not
        # Databricks-Runtime-only, despite the `spark.databricks.*` config
        # namespace. Does not need resolveMergeUpdateStructsByName alongside
        # it: this plan's fixture data has no `__metadata`/struct columns
        # (verified — `has_metadata = "__metadata" in columns`,
        # `fabricks/cdc/base/processor.py`), so the struct-field merge clause
        # that setting affects is never emitted here; add it only if a future
        # job's data actually introduces a struct column.
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
        if FABRICKS_ENVIRONMENT == "remote":
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
