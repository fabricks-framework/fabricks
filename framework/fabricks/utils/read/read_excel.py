from fabricks.context import IS_UNITY_CATALOG

if IS_UNITY_CATALOG:
    from pyspark.sql.connect.dataframe import DataFrame
else:
    from pyspark.sql import DataFrame


def read_excel(path: str) -> DataFrame:
    raise NotImplementedError()
