from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    spark = SparkSession.builder.getOrCreate()  # type: ignore
    assert spark is not None
    return spark


spark = get_spark_session()
