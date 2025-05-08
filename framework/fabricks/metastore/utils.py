from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql import DataFrame

from fabricks.context import SPARK


def get_tables(schema: str) -> DataFrame:
    table_df = SPARK.sql(f"show tables in {schema}")
    view_df = SPARK.sql(f"show views in {schema}")

    try:
        df = SPARK.sql(
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

    except AnalysisException:
        return SPARK.sql("select null::string as database, null::string as table")


def get_views(schema: str) -> DataFrame:
    view_df = SPARK.sql(f"show views in {schema}")

    try:
        df = SPARK.sql(
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

    except AnalysisException:
        return SPARK.sql("select null::string as database, null::string as view")
