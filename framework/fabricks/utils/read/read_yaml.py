from typing import Optional

import yaml
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from fabricks.context import SPARK
from fabricks.utils.helpers import concat_dfs
from fabricks.utils.path import Path


def read_yaml(
    path: Path,
    root: Optional[str] = None,
    schema: Optional[StructType] = None,
    file_name: Optional[str] = None,
) -> Optional[DataFrame]:
    files = [f for f in path.walk() if f.endswith(".yml")]
    if file_name:
        files = [f for f in files if file_name in f]

    dfs = [SPARK.createDataFrame([], schema=schema)] if schema else []

    for file in files:
        with open(file) as f:
            data = yaml.safe_load(f)

        if schema:
            dt = [d[root] for d in data] if root else data
            df = SPARK.createDataFrame(dt, schema=schema)
        else:
            json = SPARK.sparkContext.parallelize(data)
            df = SPARK.read.json(json)
            if root:
                df = df.select(f"{root}.*")

        dfs.append(df)

    if dfs:
        df = concat_dfs(dfs)
        return df

    return SPARK.createDataFrame([], schema=schema) if schema else None
