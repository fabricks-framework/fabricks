from typing import Callable, Optional

from pyspark.sql import DataFrame
from pyspark.sql.streaming.query import StreamingQuery

from fabricks.utils.path import Path


def write_stream(
    df: DataFrame,
    checkpoints_path: Path,
    func: Callable,
    timeout: Optional[int] = 18000,
) -> StreamingQuery:
    if timeout is None:
        timeout = 18000

    assert timeout is not None

    query = (
        df.writeStream.foreachBatch(func)
        .option("checkpointLocation", checkpoints_path.string)
        .trigger(once=True)
        .start()
    )
    query.awaitTermination(timeout=timeout)
    return query
