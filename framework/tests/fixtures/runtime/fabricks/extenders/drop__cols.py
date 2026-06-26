from pyspark.sql import DataFrame

from fabricks.core.extenders import extender


@extender(name="drop__cols")
def drop__cols(df: DataFrame) -> DataFrame:
    cols = [c for c in df.columns if c.startswith("__")]
    cols = [c for c in cols if c not in ["__partition_by_1", "__cluster_by_1"]]
    df = df.drop(*cols)
    return df
