import logging
import sys
from functools import reduce
from hashlib import md5 as hashlib_md5
from pathlib import Path
from typing import Any, Callable, Iterable, List, Literal, Optional, Union

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from typing_extensions import deprecated

from fabricks.utils._types import DataFrameLike
from fabricks.utils.path import GitPath


def concat_ws(fields: Union[str, List[str]], alias: Optional[str] = None) -> str:
    if isinstance(fields, str):
        fields = [fields]

    if alias:
        coalesce = [f"coalesce(cast({alias}.{f} as string), '-1')" for f in fields]
    else:
        coalesce = [f"coalesce(cast({f} as string), '-1')" for f in fields]

    return "concat_ws('*', " + ",".join(coalesce) + ")"


def md5(s: Any) -> str:
    hash_obj = hashlib_md5(str(s).encode())
    return hash_obj.hexdigest()


def add_hash(column: str, df: DataFrame, fields: Union[str, List[str]]):

    return df.withColumn(f"{column}", F.md5(F.expr(concat_ws(fields))))


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
    run_as: Literal["Thread", "Pool"] = "Thread",
) -> List[Any]:
    if logger is None:
        logger = logging.getLogger()

    current_loglevel = logger.getEffectiveLevel()
    logger.setLevel(loglevel)

    items = list(iterable.collect() if isinstance(iterable, DataFrameLike) else iterable)

    def _collect(mapped):
        if progress_bar:
            from tqdm import tqdm

            return list(tqdm(mapped, total=len(items), position=position))

        return list(mapped)

    try:
        if run_as == "Pool":
            from multiprocessing import Pool

            with Pool(processes=workers) as p:
                return _collect(p.imap(func, items))

        else:
            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor(max_workers=workers) as exe:
                return _collect(exe.map(func, items))
    finally:
        logger.setLevel(current_loglevel)


def run_notebook(path: GitPath, timeout: Optional[int] = None, **kwargs):
    """
    Runs a notebook located at the given path.

    Args:
        path (GitPath): The path to the notebook file.
        timeout (Optional[int]): The maximum execution time for the notebook in seconds. Defaults to None.
        **kwargs: Additional keyword arguments to be passed to the notebook.

    Returns:
        None
    """
    from databricks.sdk.runtime import dbutils

    if timeout is None:
        timeout = 3600

    dbutils.notebook.run(path.get_notebook_path(), timeout, {**kwargs})  # type: ignore


def load_module_from_path(name: str, path: GitPath):
    from importlib.util import module_from_spec, spec_from_file_location

    if path.parent not in sys.path:
        sys.path.insert(0, str(path.parent))

    spec = spec_from_file_location(name, path.string)
    assert spec, f"no valid module found in {path.string}"
    assert spec.loader is not None

    textwrap_module = module_from_spec(spec)
    spec.loader.exec_module(textwrap_module)

    return textwrap_module


def find_upward(
    filename: str,
    root: Optional[Union[str, Path]] = None,
) -> Optional[GitPath]:
    """
    Find a file by searching upward through the directory hierarchy.

    Args:
        filename: Name of the file to search for (e.g., "pyproject.toml", ".git")
        root: Directory to start searching from. Defaults to current working directory.

    Returns:
        Path to the file if found, None otherwise.

    Example:
        >>> pyproject = find_upward("pyproject.toml")
        >>> if pyproject:
        ...     print(f"Found: {pyproject}")
        >>> # Search from a specific location
        >>> config = find_upward(".env", root="/path/to/start")
    """
    if root is None:
        current = Path.cwd()
    else:
        current = Path(root).resolve()

    if current.is_file():
        current = current.parent

    while True:
        candidate = current / filename
        if candidate.exists():
            return GitPath(candidate)

        parent = current.parent
        if parent == current:  # Reached filesystem root
            return None

        current = parent


def backticks(columns: Union[str, List[str]]) -> List[str]:
    if isinstance(columns, str):
        columns = [columns]

    return [f"`{c}`" for c in columns]


def backtick(column: str) -> str:
    return f"`{column}`"
