from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from fabricks.core.extenders import extender


@extender(name="dummy")
def dummy_extender(df: DataFrame, **kwargs) -> DataFrame:
    return df.withColumn("extended_by", lit("dummy"))
