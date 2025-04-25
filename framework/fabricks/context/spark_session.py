from typing import Optional, Tuple

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from fabricks.context.runtime import CATALOG, CONF_RUNTIME, DEFAULT_SPARK_CONF, SECRET_SCOPE
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


def build_spark_session(
    spark: Optional[SparkSession] = None,
    reset: Optional[bool] = False,
) -> Tuple[SparkSession, DBUtils]:
    if spark is None:
        spark = _spark  # type: ignore

    add_catalog_to_spark(spark=spark)
    add_credentials_to_spark(spark=spark)
    add_spark_options_to_spark(spark=spark)

    if reset:
        spark_conf = spark.conf.getAll  # type: ignore

        keys_to_unset = [key for key in spark_conf if key not in DEFAULT_SPARK_CONF]
        for key in keys_to_unset:
            spark.conf.unset(key)
            spark.sql(f"reset `{key}`")

        keys_to_reset = [
            key for key in spark_conf if key in DEFAULT_SPARK_CONF and spark_conf[key] != DEFAULT_SPARK_CONF[key]
        ]
        for key in keys_to_reset:
            spark.conf.set(key, DEFAULT_SPARK_CONF[key])
            spark.sql(f"set {key} = {DEFAULT_SPARK_CONF[key]}")

    try:
        dbutils = DBUtils(spark)
    except Exception:
        dbutils = None

    return spark, dbutils


def init_spark_session(spark: Optional[SparkSession] = None):
    if spark is None:
        spark = _spark  # type: ignore

    build_spark_session(spark=spark)


SPARK, DBUTILS = build_spark_session()
