from typing import Optional, Tuple

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from fabricks.context.runtime import CONF_RUNTIME, SECRET_SCOPE
from fabricks.utils.secret import add_secret_to_spark, get_secret_from_secret_scope


def build_spark_session(new: Optional[bool] = False, log: Optional[bool] = False) -> Tuple[SparkSession, DBUtils]:
    if new:
        spark = SparkSession.builder.getOrCreate().newSession()  # type: ignore

        catalog = CONF_RUNTIME.get("options", {}).get("catalog")
        if catalog:
            spark.sql(f"use catalog {catalog};")

        # delta
        spark.sql("set spark.databricks.delta.schema.autoMerge.enabled = True;")
        spark.sql("set spark.databricks.delta.resolveMergeUpdateStructsByName.enabled = True;")

        spark_options = CONF_RUNTIME.get("spark_options", {})
        if spark_options:
            sql_options = spark_options.get("sql", {})
            for key, value in sql_options.items():
                spark.sql(f"set {key} = {value};")

            conf_options = spark_options.get("conf", {})
            for key, value in conf_options.items():
                spark.conf.set(key, value)

        credentials = CONF_RUNTIME.get("credentials", {})
        for uri, secret in credentials.items():
            s = get_secret_from_secret_scope(secret_scope=SECRET_SCOPE, name=secret)
            add_secret_to_spark(secret=s, uri=uri)

    else:
        spark = SparkSession.builder.getOrCreate()  # type: ignore

    return spark, DBUtils(spark)


build_spark_session(new=True, log=True)
