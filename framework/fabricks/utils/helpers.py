from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import reduce
from typing import Any, Callable, Iterable, List, Optional, Union

from databricks.sdk.runtime import dbutils, spark
from pyspark.sql import DataFrame
from typing_extensions import deprecated

from fabricks.utils.path import Path


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


def run_in_parallel(func: Callable, iterable: Union[List, DataFrame, range, set], workers: int = 8) -> List[Any]:
    """
    Runs the given function in parallel on the elements of the iterable using multiple threads.

    Args:
        func (Callable): The function to be executed in parallel.
        iterable (Union[List, DataFrame, range, set]): The iterable containing the elements on which the function will be executed.
        workers (int, optional): The number of worker threads to use. Defaults to 8.

    Returns:
        List[Any]: A list containing the results of the function calls.

    """
    out = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        iterable = iterable.collect() if isinstance(iterable, DataFrame) else iterable
        futures = {executor.submit(func, i): i for i in iterable}
        for future in as_completed(futures):
            try:
                r = future.result()
                if r:
                    out.append(r)
            except Exception:
                pass

    return out


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
