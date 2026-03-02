from typing import Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

DataFrameLike = Union[DataFrame, ConnectDataFrame]
SparkSessionLike = Union[SparkSession, ConnectSparkSession]
