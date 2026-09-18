from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

DataFrameLike = DataFrame | ConnectDataFrame
SparkSessionLike = SparkSession | ConnectSparkSession
