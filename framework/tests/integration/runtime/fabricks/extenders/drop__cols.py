from fabricks.context import IS_UNITY_CATALOG

if IS_UNITY_CATALOG:
    from pyspark.sql.connect.dataframe import DataFrame
else:
    from pyspark.sql import DataFrame

from fabricks.core.extenders import extender


@extender(name="drop__cols")
def drop__cols(df: DataFrame) -> DataFrame:
    cols = [c for c in df.columns if c.startswith("__")]
    df = df.drop(*cols)
    return df
