from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from fabricks.utils.path import FileSharePath


def run_once_via_stream(
    df: DataFrame,
    checkpoints_path: FileSharePath,
    func: Callable,
    add_dummy: bool = False,
) -> None:
    """Drive `func` exactly once over `df` through a trigger-once writeStream.

    Used to create a table / update its schema on the streaming load path. `add_dummy`
    unions a row from `fabricks.dummy` so the stream is guaranteed to start.
    """
    if add_dummy:
        dummy_df = df.sparkSession.readStream.table("fabricks.dummy")
        dummy_df = dummy_df.withColumn("__metadata", lit(None))  # __metadata is always present
        dummy_df = dummy_df.select("__metadata")
        df = df.unionByName(dummy_df, allowMissingColumns=True)
    if checkpoints_path.exists():
        checkpoints_path.rm()
    query = (
        df.writeStream.foreachBatch(func)
        .option("checkpointLocation", checkpoints_path.string)
        .trigger(once=True)
        .start()
    )
    query.awaitTermination()
    checkpoints_path.rm()
