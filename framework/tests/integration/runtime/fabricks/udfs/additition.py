from fabricks.core.udfs import udf
from pyspark.sql import SparkSession


@udf(name="additition")
def additition(spark: SparkSession):
    def _additition(a: int, b: int) -> int:
        return a + b

    spark.udf.register("udf_additition", _additition)
