from fabricks.context import IS_UNITY_CATALOG

if IS_UNITY_CATALOG:
    from pyspark.sql.connect.dataframe import DataFrame
else:
    from pyspark.sql import DataFrame

from pyspark.sql.functions import lit

from fabricks.core.extenders import extender


@extender(name="force_reload")
def force_reload(df: DataFrame) -> DataFrame:
    df = df.withColumn("__operation", lit("reload"))
    return df
