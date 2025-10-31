from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

DataFrameLike = Union[DataFrame, ConnectDataFrame]
