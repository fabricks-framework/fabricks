from pyspark.sql import SparkSession

from fabricks.core.udfs import udf


@udf(name="identity")
def identity(spark: SparkSession):
    sql = """
    create or replace function udf_identity(bk string, sk bigint)
    returns bigint
    return
        cast(
            case 
                when bk is null then -1
                when sk is null then -2
                else xxhash64(sk)
            end
        as bigint)
    """

    spark.sql(sql)
