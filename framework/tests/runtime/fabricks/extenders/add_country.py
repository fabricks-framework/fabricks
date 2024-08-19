from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from framework.fabricks.core.extenders import extender


@extender(name="add_country")
def add_country(df: DataFrame) -> DataFrame:
    df = df.withColumn("country", lit("Belgium"))
    return df
