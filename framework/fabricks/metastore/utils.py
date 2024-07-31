from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame


def get_tables(schema: str) -> DataFrame:
    table_df = spark.sql(f"show tables in {schema}")
    view_df = spark.sql(f"show views in {schema}")
    df = spark.sql(
        """
        select 
            database,
            concat_ws('.', database, tableName) as table
        from 
            {tables} 
            left anti join {views} on tableName = viewName
        """,
        tables=table_df,
        views=view_df,
    )
    return df


def get_views(schema: str) -> DataFrame:
    view_df = spark.sql(f"show views in {schema}")
    df = spark.sql(
        """
        select 
            namespace as database,
            concat_ws('.', namespace, viewName) as view
        from 
            {views}
        """,
        views=view_df,
    )
    return df
