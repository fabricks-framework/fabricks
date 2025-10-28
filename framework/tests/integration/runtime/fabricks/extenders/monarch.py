from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from fabricks.core.extenders import extender


@extender(name="monarch")
def monarch(df: DataFrame, **kwargs) -> DataFrame:
    df = df.withColumn(
        "__operation",
        expr("if(BEL_DeleteDateUtc is not null, 'delete', if(BEL_IsFullLoad=='true', 'reload', 'upsert'))"),
    )
    cols = [c for c in df.columns if c.startswith("BEL_")]
    df = df.drop(*cols)
    return df
