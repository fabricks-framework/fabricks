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


@udf(name="addition")
def addition(spark: SparkSession):
    def _addition(a: int, b: int) -> int:
        return a + b

    spark.udf.register("udf_addition", _addition)
