from pyspark.sql import SparkSession

try:
    from fabricks.core.udfs import udf

except ModuleNotFoundError:  # Needed for the tests (https://docs.databricks.com/aws/en/files/workspace-modules)
    import os
    import sys
    from pathlib import Path

    p = Path(os.getcwd())
    while not (p / "pyproject.toml").exists():
        p = p.parent

    root = p.absolute()

    if str(root) not in sys.path:
        print(f"adding {root} to sys.path")
        sys.path.insert(0, str(root))

    from fabricks.core.udfs import udf


@udf(name="identity")
def identity(spark: SparkSession):
    sql = """
    create or replace function udf_identity(bk string, sk bigint)
    returns bigint
    return
        cast(
            case 
                when bk is null then -1
                when sk is null then -2
                else xxhash64(sk)
            end
        as bigint)
    """

    spark.sql(sql)
