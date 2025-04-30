from pyspark.sql.types import Row

from fabricks.cdc import NoCDC
from fabricks.context import SPARK
from fabricks.core.jobs.base._types import Steps
from fabricks.utils.helpers import concat_dfs, run_in_parallel


def collect_stats():
    def _collect_tables(s: str):
        df_table = SPARK.sql(f"show tables in {s}")
        df_view = SPARK.sql(f"show views in {s}")

        cond = [df_table.tableName == df_view.viewName]
        df_table = df_table.join(df_view, cond, how="left_anti")

        return df_table

    dfs = run_in_parallel(_collect_tables, Steps, workers=8)
    df_table = concat_dfs(dfs)
    assert df_table is not None

    def _collect_stats(row: Row):
        table = row["tableName"]
        database = row["database"]
        job = f"{database}.{table}"

        desc = SPARK.sql(f"describe detail {job}").collect()[0]
        bytes = desc["sizeInBytes"]
        files = desc["numFiles"]

        df = SPARK.sql(
            f"""
            select
              '{database}' as step,
              md5('{job}') as job_id,
              cast({bytes} as long) as bytes,
              cast({files} as long) as `files`,
              cast(count(*) as long) as `rows`
            from
              {job}
            """
        )

        return df

    dfs = run_in_parallel(_collect_stats, df_table, workers=64)
    df = concat_dfs(dfs)
    assert df is not None

    NoCDC("fabricks", "statistics").overwrite(df)
