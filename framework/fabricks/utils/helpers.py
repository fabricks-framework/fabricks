from collections.abc import Callable, Iterable
from functools import reduce
from hashlib import md5 as hashlib_md5
import logging
from pathlib import Path
import sys
from types import ModuleType
from typing import Any, Literal

from pyspark.sql import DataFrame
import pyspark.sql.functions as F  # noqa: N812 - idiomatic PySpark alias
from typing_extensions import deprecated

from fabricks.utils._types import DataFrameLike
from fabricks.utils.path import GitPath


def concat_ws(fields: str | list[str], alias: str | None = None) -> str:
    if isinstance(fields, str):
        fields = [fields]

    if alias:
        coalesce = [f"coalesce(cast({alias}.{f} as string), '-1')" for f in fields]
    else:
        coalesce = [f"coalesce(cast({f} as string), '-1')" for f in fields]

    return "concat_ws('*', " + ",".join(coalesce) + ")"


def md5(s: Any) -> str:  # noqa: ANN401 - generic hash of any stringifiable value, not restricted to str
    hash_obj = hashlib_md5(str(s).encode())
    return hash_obj.hexdigest()


def add_hash(column: str, df: DataFrame, fields: str | list[str]) -> DataFrame:
    return df.withColumn(f"{column}", F.md5(F.expr(concat_ws(fields))))


def concat_dfs(dfs: Iterable[DataFrame]) -> DataFrame | None:
    dfs = [df for df in dfs if df is not None]
    if len(dfs) == 0:
        return None
    return reduce(lambda x, y: x.unionByName(y, allowMissingColumns=True), dfs)


@deprecated("use run_in_parallel instead")
def run_threads(func: Callable, iter: list | DataFrame | range | set, workers: int = 8) -> list[Any]:
    return run_in_parallel(func, iter, workers)


def run_in_parallel(
    func: Callable,
    iterable: list | DataFrame | range | set,
    workers: int = 8,
    progress_bar: bool | None = False,
    position: int | None = None,
    loglevel: int = logging.CRITICAL,
    logger: logging.Logger | None = None,
    run_as: Literal["Thread", "Pool"] = "Thread",
) -> list[Any]:
    if logger is None:
        logger = logging.getLogger()

    current_loglevel = logger.getEffectiveLevel()
    logger.setLevel(loglevel)

    items = list(iterable.collect() if isinstance(iterable, DataFrameLike) else iterable)

    def _collect(mapped: Iterable[Any]) -> list[Any]:
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


def run_notebook(path: GitPath, timeout: int | None = None, **kwargs: Any) -> None:  # noqa: ANN401 - forwarded straight to dbutils.notebook.run
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


def load_module_from_path(name: str, path: GitPath) -> ModuleType:
    from importlib.util import module_from_spec, spec_from_file_location

    if path.parent not in sys.path:
        sys.path.insert(0, str(path.parent))

    spec = spec_from_file_location(name, path.string)
    assert spec, f"no valid module found in {path.string}"
    assert spec.loader is not None

    textwrap_module = module_from_spec(spec)
    spec.loader.exec_module(textwrap_module)

    return textwrap_module


def find_upward(filename: str, root: str | Path | None = None) -> GitPath | None:
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
    current = Path.cwd() if root is None else Path(root).resolve()

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


def backticks(columns: str | list[str]) -> list[str]:
    if isinstance(columns, str):
        columns = [columns]

    return [f"`{c}`" for c in columns]


def backtick(column: str) -> str:
    return f"`{column}`"
