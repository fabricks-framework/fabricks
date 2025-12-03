import os
from typing import List, Optional

from pyspark.sql import SparkSession

from fabricks.context import CATALOG, PATH_MASKS, SPARK
from fabricks.context.log import DEFAULT_LOGGER


def register_all_masks(override: bool = False):
    """
    Register all masks.
    """

    DEFAULT_LOGGER.info("register masks")
    for mask in get_masks():
        split = mask.split(".")
        try:
            register_mask(mask=split[0], override=override)
        except Exception as e:
            DEFAULT_LOGGER.exception(f"could not register mask {mask}", exc_info=e)


def get_masks() -> List[str]:
    return [os.path.basename(f) for f in PATH_MASKS.walk()]


def is_registered(mask: str, spark: Optional[SparkSession] = None) -> bool:
    if spark is None:
        spark = SPARK
    assert spark is not None

    df = spark.sql("show user functions in default")

    if CATALOG:
        df = df.where(f"function == '{CATALOG}.default.mask_{mask}'")
    else:
        df = df.where(f"function == 'spark_catalog.default.mask_{mask}'")

    return not df.isEmpty()


def register_mask(mask: str, override: Optional[bool] = False, spark: Optional[SparkSession] = None):
    if spark is None:
        spark = SPARK
    assert spark is not None

    if not is_registered(mask, spark) or override:
        if override:
            DEFAULT_LOGGER.debug(f"override mask {mask}")
        else:
            DEFAULT_LOGGER.debug(f"register mask {mask}")

        path = PATH_MASKS.joinpath(f"{mask}.sql")
        spark.sql(path.get_sql())
