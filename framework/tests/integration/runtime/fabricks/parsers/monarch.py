from pyspark.sql import DataFrame, SparkSession

try:
    from fabricks.core.parsers import parser
    from fabricks.utils.path import Path
    from tests.integration.runtime.fabricks.parsers.delete_log import DeleteLogBaseParser

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

    from fabricks.core.parsers import parser
    from fabricks.utils.path import Path
    from tests.integration.runtime.fabricks.parsers.delete_log import DeleteLogBaseParser


@parser(name="monarch")
class MonarchParser(DeleteLogBaseParser):
    def parse(
        self,
        data_path: Path,
        schema_path: Path,
        spark: SparkSession,
        stream: bool,
    ) -> DataFrame:
        df = super().parse(stream=stream, data_path=data_path, schema_path=schema_path, spark=spark)
        if df:
            cols = [c for c in df.columns if c.startswith("BEL_")]
            if cols:
                df = df.drop(*cols)
        return df
