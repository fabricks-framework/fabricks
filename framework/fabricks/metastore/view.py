from typing import Optional, Union
from uuid import uuid4

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.connect.dataframe import DataFrame as CDataFrame

from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.relational import Relational


class View(Relational):
    @staticmethod
    def create_or_replace(
        df: Union[DataFrame, pd.DataFrame],
        *dependencies,
        spark: Optional[SparkSession] = None,
    ) -> str:
        if spark is None:
            if isinstance(df, (DataFrame, CDataFrame)):
                spark = df.sparkSession
            else:
                spark = SPARK

        assert spark is not None

        uuid = str(uuid4().hex)
        df = spark.createDataFrame(df) if isinstance(df, pd.DataFrame) else df
        if dependencies:
            for d in dependencies:
                df = df.join(d.where("1 == 2"), how="leftanti")

        df.createOrReplaceGlobalTempView(uuid)
        return uuid


def create_or_replace_global_temp_view(name: str, df: DataFrame, uuid: Optional[bool] = False) -> str:
    if uuid:
        name = f"{name}__{str(uuid4().hex)}"

    job = name.split("__")[0]
    DEFAULT_LOGGER.debug(f"create global temp view {name}", extra={"job": job})
    df.createOrReplaceGlobalTempView(name)

    return f"global_temp.{name}"
