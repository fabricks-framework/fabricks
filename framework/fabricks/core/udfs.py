import importlib.util
import os
import re
from typing import Callable, List, Optional

from pyspark.sql import SparkSession

from fabricks.context import CATALOG, IS_UNITY_CATALOG, PATH_UDFS, SPARK
from fabricks.context.log import DEFAULT_LOGGER

UDFS: dict[str, Callable] = {}


def register_all_udfs():
    """
    Register all user-defined functions (UDFs).

    This function iterates over all UDFs returned by the `get_udfs` function,
    splits the UDF name into the function name and extension, and attempts to
    register the UDF using the `register_udf` function. If an exception occurs
    during registration, an error message is logged.

    Returns:
        None
    """
    for udf in get_udfs():
        split = udf.split(".")
        try:
            register_udf(udf=split[0], extension=split[1])
        except Exception:
            DEFAULT_LOGGER.exception(f"udf {udf} not registered")


def get_udfs() -> List[str]:
    files = [os.path.basename(f) for f in PATH_UDFS.walk()]
    udfs = [f for f in files if not str(f).endswith("__init__.py") and not str(f).endswith(".requirements.txt")]
    return udfs


def get_extension(udf: str) -> str:
    for u in get_udfs():
        r = re.compile(rf"{udf}(\.py|\.sql)")
        if re.match(r, u):
            return u.split(".")[1]

    raise ValueError(f"{udf} not found")


def is_registered(udf: str, spark: Optional[SparkSession] = None) -> bool:
    if spark is None:
        spark = SPARK
    assert spark is not None

    df = spark.sql("show user functions in default")

    if CATALOG:
        df = df.where(f"function == '{CATALOG}.default.udf_{udf}'")
    else:
        df = df.where(f"function == 'spark_catalog.default.udf_{udf}'")

    return not df.isEmpty()


def register_udf(udf: str, extension: Optional[str] = None, spark: Optional[SparkSession] = None):
    """
    Register a user-defined function (UDF) in Spark.

    Args:
        udf (str): The name of the UDF to register.
        extension (Optional[str]): The file extension of the UDF implementation file. If not provided, it will be inferred from the UDF name.
        spark (Optional[SparkSession]): The SparkSession object. If not provided, a new SparkSession will be created.

    Raises:
        ValueError: If the UDF implementation file is not found or if the UDF name is not found.

    """
    if spark is None:
        spark = SPARK
    assert spark is not None

    if not is_registered(udf, spark):
        if extension is None:
            extension = get_extension(udf)

        assert extension

        path = PATH_UDFS.join(f"{udf}.{extension}")

        if extension == "sql":
            spark.sql(path.get_sql())

        elif extension == "py":
            if not IS_UNITY_CATALOG:
                assert path.exists(), f"udf not found ({path.string})"
            else:
                DEFAULT_LOGGER.debug(f"could not check if udf exists ({path.string})")

            spec = importlib.util.spec_from_file_location(udf, path.string)
            assert spec, f"no valid udf found ({path.string})"
            assert spec.loader is not None

            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)

            u = UDFS[udf]
            u(spark)

        else:
            raise ValueError(f"{udf} not found")


def udf(name: str):
    def decorator(fn: Callable):
        UDFS[name] = fn
        return fn

    return decorator
