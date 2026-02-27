from datetime import datetime

from pyspark.sql import SparkSession

from fabricks.core.udfs import udf


@udf(name="add_now")
def add_now(spark: SparkSession):
    def _add_now(a: str) -> str:
        return a + "_" + str(datetime.now())

    spark.udf.register("udf_add_now", _add_now)
