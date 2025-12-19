import logging
from typing import Any, Callable, List, Literal, Optional, Union

from pyspark.sql import DataFrame

from fabricks.context import PATH_RUNTIME
from fabricks.utils.helpers import run_in_parallel as _run_in_parallel


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
    Wrapper around run_in_parallel that automatically includes the runtime path in sys.path.

    This ensures that custom parsers, extenders, UDFs, and other runtime modules are available
    in worker threads/processes, which is especially important for Databricks Runtime 17.3+.

    Args:
        func (Callable): The function to be executed in parallel.
        iterable (Union[List, DataFrame, range, set]): The iterable containing the elements on which the function will be executed.
        workers (int, optional): The number of worker threads/processes to use. Defaults to 8.
        progress_bar (Optional[bool], optional): Whether to display a progress bar. Defaults to False.
        position (Optional[int], optional): Position for the progress bar. Defaults to None.
        loglevel (int, optional): Log level to set during execution. Defaults to logging.CRITICAL.
        logger (Optional[logging.Logger], optional): Logger instance to use. Defaults to None.
        run_as (Optional[Literal["ThreadPool", "ProcessPool", "Pool", "Queue"]], optional): Type of run as to use.
        sys_paths (Optional[List[str]], optional): Additional paths to add to sys.path. The runtime path is always included.

    Returns:
        List[Any]: A list containing the results of the function calls.
    """
    # Always include the runtime path
    if sys_paths:
        sys_paths.extend([str(PATH_RUNTIME)])
    else:
        sys_paths = [str(PATH_RUNTIME)]

    return _run_in_parallel(
        func=func,
        iterable=iterable,
        workers=workers,
        progress_bar=progress_bar,
        position=position,
        loglevel=loglevel,
        logger=logger,
        run_as=run_as,
        sys_paths=sys_paths,
    )
