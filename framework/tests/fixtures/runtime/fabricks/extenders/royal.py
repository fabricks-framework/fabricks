from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, lit

from fabricks.core.extenders import extender


@extender(name="royal")
def royal(df: DataFrame, **kwargs) -> DataFrame:
    df = df.withColumn(
        "__operation",
        expr("if(BEL_DeleteDateUtc is not null, 'delete', if(BEL_IsFullLoad=='true', 'reload', 'upsert'))"),
    )
    df = df.where("__operation != 'delete'")
    df = df.withColumn("__operation", lit("reload"))
    cols = [c for c in df.columns if c.startswith("BEL_")]
    df = df.drop(*cols)

    return df
