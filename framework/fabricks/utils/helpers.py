import logging
import sys
from functools import reduce
from queue import Queue
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


def _init_worker_process(paths: Optional[List[str]] = None):
    """Initialize worker process by adding paths to sys.path."""
    import sys

    if paths:
        for path in paths:
            if path not in sys.path:
                sys.path.insert(0, path)


def _create_wrapper_with_sys_paths(func: Callable, paths: Optional[List[str]]):
    """Create a wrapper function that initializes sys.path before calling func."""

    def wrapper(item):
        import sys

        if paths:
            for path in paths:
                if path not in sys.path:
                    sys.path.insert(0, path)
        return func(item)

    return wrapper


def _process_queue_item(func: Callable, task_queue: Queue, result_queue: Queue, stop_signal: Any):
    """Worker function that processes items from a queue."""
    while True:
        try:
            item = task_queue.get(timeout=1)

            if item is stop_signal:
                task_queue.put(stop_signal)  # Put it back for other workers
                break

            result = func(item)
            result_queue.put(result)
        except Exception:
            continue


def _run_in_parallel_legacy(
    func: Callable,
    iterable: Union[List, DataFrame, range, set],
    workers: int = 8,
    progress_bar: Optional[bool] = False,
    position: Optional[int] = None,
    sys_paths: Optional[List[str]] = None,
) -> List[Any]:
    from concurrent.futures import ThreadPoolExecutor

    iterable = iterable.collect() if isinstance(iterable, DataFrameLike) else iterable  # type: ignore

    # Wrap the function to inject sys.path if needed
    wrapped_func = _create_wrapper_with_sys_paths(func, sys_paths) if sys_paths else func

    with ThreadPoolExecutor(max_workers=workers) as executor:
        if progress_bar:
            from tqdm import tqdm

            results = list(tqdm(executor.map(wrapped_func, iterable), total=len(iterable), position=position))
        else:
            results = list(executor.map(wrapped_func, iterable))

    return results


def run_in_parallel(
    func: Callable,
    iterable: Union[List, DataFrame, range, set],
    workers: int = 8,
    progress_bar: Optional[bool] = False,
    position: Optional[int] = None,
    loglevel: int = logging.CRITICAL,
    logger: Optional[logging.Logger] = None,
    run_as: Optional[Literal["ThreadPool", "ProcessPool", "Pool", "Queue", "Legacy"]] = "Legacy",
    sys_paths: Optional[List[str]] = None,
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
        run_as (Optional[Literal["ThreadPool", "ProcessPool", "Pool", "Queue"]], optional): Type of run as to use.
        sys_paths (Optional[List[str]], optional): List of paths to add to sys.path before execution. Defaults to None.

    Returns:
        List[Any]: A list containing the results of the function calls.

    """
    if logger is None:
        logger = logging.getLogger()

    current_loglevel = logger.getEffectiveLevel()
    logger.setLevel(loglevel)

    # Add custom paths to sys.path if provided
    if sys_paths:
        for path in sys_paths:
            if path not in sys.path:
                sys.path.insert(0, path)

    if run_as == "Legacy":
        results = _run_in_parallel_legacy(
            func=func,
            iterable=iterable,
            workers=workers,
            progress_bar=progress_bar,
            position=position,
            sys_paths=sys_paths,
        )

    else:
        iterables = iterable.collect() if isinstance(iterable, DataFrameLike) else iterable  # type: ignore
        results = []

        if run_as == "Queue":
            import threading

            # Wrap the function to inject sys.path if needed
            wrapped_func = _create_wrapper_with_sys_paths(func, sys_paths) if sys_paths else func

            task_queue = Queue()
            result_queue = Queue()
            stop_signal = object()

            for item in iterables:
                task_queue.put(item)

            task_queue.put(stop_signal)

            threads = []
            for _ in range(workers):
                t = threading.Thread(
                    target=_process_queue_item, args=(wrapped_func, task_queue, result_queue, stop_signal)
                )
                t.start()

                threads.append(t)

            if progress_bar:
                from tqdm import tqdm

                with tqdm(total=len(iterables), position=position) as t:
                    for _ in range(len(iterables)):
                        result = result_queue.get()
                        results.append(result)

                        t.update()
                        t.refresh()

            else:
                for _ in range(len(iterables)):
                    results.append(result_queue.get())

            for t in threads:
                t.join()

        elif run_as == "Pool":
            from multiprocessing import Pool

            # Use initializer to set up sys.path in worker processes
            init_args = (sys_paths,) if sys_paths else None
            initializer = _init_worker_process if sys_paths else None

            with Pool(processes=workers, initializer=initializer, initargs=init_args) as p:  # type: ignore
                if progress_bar:
                    from tqdm import tqdm

                    with tqdm(total=len(iterables), position=position) as t:
                        for result in p.map(func, iterables):
                            results.append(result)

                            t.update()
                            t.refresh()

                else:
                    results = list(p.map(func, iterables))

        else:
            from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

            Executor = ProcessPoolExecutor if run_as == "ProcessPool" else ThreadPoolExecutor

            # Wrap the function to inject sys.path if needed
            wrapped_func = _create_wrapper_with_sys_paths(func, sys_paths) if sys_paths else func

            with Executor(max_workers=workers) as exe:
                if progress_bar:
                    from tqdm import tqdm

                    with tqdm(total=len(iterables), position=position) as t:
                        for result in exe.map(wrapped_func, iterables):
                            results.append(result)

                            t.update()
                            t.refresh()

                else:
                    results = list(exe.map(wrapped_func, iterables))

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


def xxhash64(s: Any) -> int:
    df = spark.sql(f"select xxhash64(cast('{s}' as string)) as xxhash64")
    return df.collect()[0][0]


def md5(s: Any) -> str:
    from hashlib import md5

    md5 = md5(str(s).encode())
    return md5.hexdigest()


def load_module_from_path(name: str, path: Path):
    from importlib.util import module_from_spec, spec_from_file_location

    sys.path.append(str(path.parent))

    spec = spec_from_file_location(name, path.string)
    assert spec, f"no valid module found in {path.string}"
    assert spec.loader is not None

    textwrap_module = module_from_spec(spec)
    spec.loader.exec_module(textwrap_module)

    return textwrap_module
