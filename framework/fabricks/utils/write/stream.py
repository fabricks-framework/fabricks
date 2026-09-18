from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql.streaming.query import StreamingQuery
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from fabricks.utils.path import FileSharePath


def _is_isolation_startup_failure(exception: BaseException) -> bool:
    # DBR 18.0 changed how foreachBatch sets up its execution session on
    # Shared/USER_ISOLATION clusters -- when the schedule fans out several
    # streaming jobs in parallel, their isolated-worker sandboxes compete for
    # startup capacity on the same cluster and one can miss the platform's
    # 60s SandboxClientTimeoutTracker deadline (before our callback ever
    # runs). No client-side setting raises that deadline, so retry.
    message = str(exception)
    return "ISOLATION_STARTUP_FAILURE" in message or "SandboxClientTimeoutTracker" in message


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=5, max=30),
    retry=retry_if_exception(_is_isolation_startup_failure),
    reraise=True,
)
def write_stream(
    df: DataFrame, checkpoints_path: FileSharePath, func: Callable, timeout: int | None = 18000
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
