from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from fabricks.core.extenders import extender


@extender(name="add_country")
def add_country(df: DataFrame, **kwargs) -> DataFrame:
    return df.withColumn("country", lit(kwargs.get("country")))
