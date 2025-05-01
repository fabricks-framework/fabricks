from concurrent.futures import ThreadPoolExecutor
from functools import reduce
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


def concat_dfs(dfs: Iterable[DataFrame]) -> Optional[DataFrame]:
    dfs = [df for df in dfs if df is not None]
    if len(dfs) == 0:
        return None
    return reduce(lambda x, y: x.unionByName(y, allowMissingColumns=True), dfs)


@deprecated("use run_in_parallel instead")
def run_threads(func: Callable, iter: Union[List, DataFrame, range, set], workers: int = 8) -> List[Any]:
    return run_in_parallel(func, iter, workers)


def run_in_parallel(
    func: Callable,
    iterable: Union[List, DataFrame, range, set],
    workers: int = 8,
    progress_bar: Optional[bool] = False,
    position: Optional[int] = None,
) -> List[Any]:
    """
    Runs the given function in parallel on the elements of the iterable using multiple threads.

    Args:
        func (Callable): The function to be executed in parallel.
        iterable (Union[List, DataFrame, range, set]): The iterable containing the elements on which the function will be executed.
        workers (int, optional): The number of worker threads to use. Defaults to 8.

    Returns:
        List[Any]: A list containing the results of the function calls.

    """
    iterable = iterable.collect() if isinstance(iterable, (DataFrame, CDataFrame)) else iterable  # type: ignore

    with ThreadPoolExecutor(max_workers=workers) as executor:
        if progress_bar:
            from tqdm import tqdm

            results = list(tqdm(executor.map(func, iterable), total=len(iterable), position=position))
        else:
            results = list(executor.map(func, iterable))

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
