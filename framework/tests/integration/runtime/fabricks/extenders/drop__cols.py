from pyspark.sql import DataFrame

try:
    from fabricks.core.extenders import extender

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

    from fabricks.core.extenders import extender


@extender(name="drop__cols")
def drop__cols(df: DataFrame) -> DataFrame:
    cols = [c for c in df.columns if c.startswith("__")]
    df = df.drop(*cols)
    return df
