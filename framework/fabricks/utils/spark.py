from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from fabricks.utils.spark import spark

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
