from pyspark.sql import SparkSession

from fabricks.core.udfs import udf


@udf(name="addition")
def addition(spark: SparkSession):
    def _addition(a: int, b: int) -> int:
        return a + b

    spark.udf.register("udf_addition", _addition)
