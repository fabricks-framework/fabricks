from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql import DataFrame

from fabricks.context import IS_UNITY_CATALOG, SPARK


def get_tables(schema: str) -> DataFrame:
    try:
        if IS_UNITY_CATALOG:
            df = SPARK.sql(
                f"""
                select
                  table_schema as database,
                  concat_ws('.', table_schema, table_name) as table,
                  md5(concat_ws('.', table_schema, table_name)) as job_id
                from
                  system.information_schema.tables
                where
                  table_schema = '{schema}'
                  and table_type = 'MANAGED'
                """
            )

        else:
            table_df = SPARK.sql(f"show tables in {schema}")
            view_df = SPARK.sql(f"show views in {schema}")

            df = SPARK.sql(
                """
                select
                  database,
                  concat_ws('.', database, tableName) as table,
                  md5(table) as job_id
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
    try:
        if IS_UNITY_CATALOG:
            df = SPARK.sql(
                f"""
                select
                  table_schema as database,
                  concat_ws('.', table_schema, table_name) as view,
                  md5(concat_ws('.', table_schema, table_name)) as job_id
                from
                  system.information_schema.views
                where
                  table_schema = '{schema}'
                """
            )

        else:
            view_df = SPARK.sql(f"show views in {schema}")

            df = SPARK.sql(
                """
                select
                  namespace as database,
                  concat_ws('.', namespace, viewName) as view,
                  md5(view) as job_id
                from
                    {views}
                where
                  not isTemporary
                """,
                views=view_df,
            )

        return df

    except AnalysisException:
        return SPARK.sql("select null::string as database, null::string as view")
