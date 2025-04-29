
import contextlib
from io import StringIO
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from functools import reduce
from io import StringIO
from threading import local
from typing import Any, Callable, Iterable, List, Optional, Union

from pyspark.sql import DataFrame
from pyspark.sql.connect.dataframe import DataFrame as CDataFrame
from typing_extensions import deprecated

from fabricks.utils.path import Path
from fabricks.utils.spark import spark


def concat_ws(fields: Union[str, List[str]], alias: Optional[str] = None) -> str:
    if isinstance(fields, str):
        fields = [fields]

    if alias:
        coalesce = [f"coalesce(cast({alias}.{f} as string), '-1')" for f in fields]
    else:
        coalesce = [f"coalesce(cast({f} as string), '-1')" for f in fields]

    return "concat_ws('*', " + ",".join(coalesce) + ")"


def concat_dfs(dfs: Iterable[DataFrame]) -> DataFrame:
    return reduce(lambda x, y: x.unionByName(y, allowMissingColumns=True), dfs)


@deprecated("use run_threads instead")
def run_threads(func: Callable, iter: Union[List, DataFrame, range, set], workers: int = 8) -> List[Any]:
    return run_in_parallel(func, iter, workers)


# Thread-local storage for context isolation
thread_local = local()


@contextmanager
def isolated_context():
    """Context manager that provides isolated stdout and other context variables."""
    # Save original stdout
    original_stdout = sys.stdout

    # Create thread-local stdout
    thread_local.stdout = StringIO()

    # Redirect stdout to our thread-local version
    sys.stdout = thread_local.stdout

    try:
        yield
    finally:
        # Always restore original stdout
        sys.stdout = original_stdout


def run_in_parallel(func: Callable, iterable: Union[List, DataFrame, range, set], workers: int = 8) -> List[Any]:
    """
    Runs the given function in parallel on the elements of the iterable using multiple threads
    with isolated contexts.

    Args:
        func (Callable): The function to be executed in parallel.
        iterable (Union[List, DataFrame, range, set]): The iterable containing the elements.
        workers (int, optional): The number of worker threads to use. Defaults to 8.

    Returns:
        List[Any]: A list containing the results of the function calls.
    """
    results = []

    # Wrapper to run the function in an isolated context
    def run_with_isolation(item):
        with isolated_context():
            return func(item)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Handle DataFrame conversion if needed
        if isinstance(iterable, (DataFrame, CDataFrame)):
            iterable = iterable.collect()

        # Submit all tasks to the executor
        futures = {executor.submit(run_with_isolation, item): item for item in iterable}

        # Collect results as they complete
        for future in as_completed(futures):
            result = future.result()  # This will raise exceptions naturally
            if result is not None:
                results.append(result)

    return results


def run_notebook(path: Path, timeout: Optional[int] = None, **kwargs):
    """
    Runs a notebook located at the given path.

    Args:
        path (Path): The path to the notebook file.
        timeout (Optional[int]): The maximum execution time for the notebook in seconds. Defaults to None.
        **kwargs: Additional keyword arguments to be passed to the notebook.

    Returns:
        None
    """
    from databricks.sdk.runtime import dbutils

    if timeout is None:
        timeout = 3600

    dbutils.notebook.run(path.get_notebook_path(), timeout, {**kwargs})  # type: ignore


def xxhash64(s: Any):
    df = spark.sql(f"select xxhash64(cast('{s}' as string)) as xxhash64")
    return df.collect()[0][0]


def md5(s: Any):
    from hashlib import md5

    md5 = md5(str(s).encode())
    return md5.hexdigest()


def explain(df: DataFrame, extended: bool = True):
    buffer = StringIO()
    
    with contextlib.redirect_stdout(buffer):
        df.explain(extended)
    
    return buffer.getvalue()
