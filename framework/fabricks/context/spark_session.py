from typing import Optional

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from typing_extensions import deprecated

from fabricks.context.runtime import CATALOG, CONF_RUNTIME, IS_UNITY_CATALOG, SECRET_SCOPE
from fabricks.context.secret import add_secret_to_spark, get_secret_from_secret_scope
from fabricks.utils.spark import spark as _spark


def add_catalog_to_spark(spark: Optional[SparkSession] = None):
    if spark is None:
        spark = _spark  # type: ignore

    if CATALOG is not None:
        spark.sql(f"use catalog {CATALOG};")


def add_credentials_to_spark(spark: Optional[SparkSession] = None):
    if spark is None:
        spark = _spark  # type: ignore

    credentials = CONF_RUNTIME.get("credentials", {})
    for uri, secret in credentials.items():
        s = get_secret_from_secret_scope(secret_scope=SECRET_SCOPE, name=secret)
        add_secret_to_spark(secret=s, uri=uri, spark=spark)


def add_spark_options_to_spark(spark: Optional[SparkSession] = None):
    if spark is None:
        spark = _spark  # type: ignore

    # delta default options
    spark.sql("set spark.databricks.delta.schema.autoMerge.enabled = True;")
    spark.sql("set spark.databricks.delta.resolveMergeUpdateStructsByName.enabled = True;")

    # runtime options
    spark_options = CONF_RUNTIME.get("spark_options", {})
    if spark_options:
        sql_options = spark_options.get("sql", {})
        for key, value in sql_options.items():
            spark.sql(f"set {key} = {value};")

        conf_options = spark_options.get("conf", {})
        for key, value in conf_options.items():
            spark.conf.set(key, value)


def build_spark_session(spark: Optional[SparkSession] = None, app_name: Optional[str] = "default") -> SparkSession:
    if app_name is None:
        app_name = "default"

    if spark is not None:
        _spark = spark
        _spark.builder.appName(app_name)

    else:
        _spark = (
            SparkSession.builder.appName(app_name)  # type: ignore
            .config("spark.driver.allowMultipleContexts", "true")
            .enableHiveSupport()
            .getOrCreate()
        )

    add_catalog_to_spark(spark=_spark)
    if not IS_UNITY_CATALOG:
        add_credentials_to_spark(spark=_spark)
    add_spark_options_to_spark(spark=_spark)

    return _spark


@deprecated("use build_spark_session instead")
def init_spark_session(spark: Optional[SparkSession] = None):
    if spark is None:
        spark = _spark  # type: ignore

    return build_spark_session(spark=spark)


SPARK = build_spark_session(app_name="default")
try:
    DBUTILS = DBUtils(SPARK)
except Exception:
    DBUTILS = None
