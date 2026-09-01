from typing import Any
from uuid import uuid4

import pandas as pd

from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.metastore.dbobject import DbObject
from fabricks.utils._types import DataFrameLike, SparkSessionLike


class View(DbObject):
    @staticmethod
    def create_or_replace(
        df: DataFrameLike | pd.DataFrame,
        *dependencies: Any,  # noqa: ANN401 - classic/connect DataFrame union join overloads can't be typed precisely; see .limit()/.join() usage below
        spark: SparkSessionLike | None = None,
    ) -> str:
        if spark is None:
            spark = df.sparkSession if isinstance(df, DataFrameLike) else SPARK

        assert spark is not None

        uuid = str(uuid4().hex)
        df = spark.createDataFrame(df) if isinstance(df, pd.DataFrame) else df
        if dependencies:
            for d in dependencies:
                df = df.join(d.limit(0), how="leftanti")

        df.createOrReplaceGlobalTempView(uuid)
        return uuid


def create_or_replace_global_temp_view(
    name: str,
    df: DataFrameLike,
    uuid: bool | None = False,
    job: Any | None = None,  # noqa: ANN401 - opaque logging label; callers pass unrelated job/processor classes
) -> str:
    if uuid:
        name = f"{name}__{uuid4().hex!s}"

    if job is None:
        job = name.split("__")[0]

    DEFAULT_LOGGER.debug(f"create global temp view {name}", extra={"label": job})
    df.createOrReplaceGlobalTempView(name)

    return f"global_temp.{name}"
