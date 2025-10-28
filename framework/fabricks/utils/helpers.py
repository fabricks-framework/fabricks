import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import reduce
from multiprocessing import Pool
from typing import Any, Callable, Iterable, List, Literal, Optional, Union

from pyspark.sql import DataFrame
from typing_extensions import deprecated

from fabricks.utils._types import DataFrameLike
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
    loglevel: int = logging.CRITICAL,
    logger: Optional[logging.Logger] = None,
    executor: Optional[Literal["ThreadPoolExecutor", "ProcessPoolExecutor", "Pool"]] = "Pool",
) -> List[Any]:
    """
    Runs the given function in parallel on the elements of the iterable using multiple threads or processes.

    Args:
        func (Callable): The function to be executed in parallel.
        iterable (Union[List, DataFrame, range, set]): The iterable containing the elements on which the function will be executed.
        workers (int, optional): The number of worker threads/processes to use. Defaults to 8.
        progress_bar (Optional[bool], optional): Whether to display a progress bar. Defaults to False.
        position (Optional[int], optional): Position for the progress bar. Defaults to None.
        loglevel (int, optional): Log level to set during execution. Defaults to logging.CRITICAL.
        logger (Optional[logging.Logger], optional): Logger instance to use. Defaults to None.
        executor (Optional[Literal["ThreadPoolExecutor", "ProcessPoolExecutor", "Pool"]], optional):
            Type of executor to use. Defaults to "Pool".

    Returns:
        List[Any]: A list containing the results of the function calls.

    """
    if logger is None:
        logger = logging.getLogger()

    current_loglevel = logger.getEffectiveLevel()
    logger.setLevel(loglevel)

    iterable = iterable.collect() if isinstance(iterable, DataFrameLike) else iterable  # type: ignore

    results = []

    if executor == "Pool":
        with Pool(processes=workers) as p:
            if progress_bar:
                from tqdm import tqdm

                with tqdm(total=len(iterable), position=position) as pbar:
                    for result in p.imap(func, iterable):
                        pbar.update()
                        pbar.refresh()
                        results.append(result)
            else:
                results = list(p.imap(func, iterable))

    else:
        Executor = ProcessPoolExecutor if executor == "ProcessPoolExecutor" else ThreadPoolExecutor
        with Executor(max_workers=workers) as exe:
            if progress_bar:
                from tqdm import tqdm

                with tqdm(total=len(iterable), position=position) as pbar:
                    for result in exe.map(func, iterable):
                        pbar.update()
                        pbar.refresh()
                        results.append(result)
            else:
                results = list(exe.map(func, iterable))

    logger.setLevel(current_loglevel)

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
