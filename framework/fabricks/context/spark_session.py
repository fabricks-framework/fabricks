from typing import Optional, Tuple

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from databricks.sdk.runtime import spark as _spark

from fabricks.context.runtime import CATALOG, CONF_RUNTIME, SECRET_SCOPE
from fabricks.utils.secret import add_secret_to_spark, get_secret_from_secret_scope


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


def build_spark_session(spark: Optional[SparkSession] = None) -> Tuple[SparkSession, DBUtils]:
    if spark is None:
        spark = _spark  # type: ignore

    add_catalog_to_spark(spark=spark)
    add_credentials_to_spark(spark=spark)
    add_spark_options_to_spark(spark=spark)

    return spark, DBUtils(spark)


def init_spark_session(spark: Optional[SparkSession] = None):
    if spark is None:
        spark = _spark  # type: ignore

    build_spark_session(spark=spark)


SPARK, DBUTILS = build_spark_session()
