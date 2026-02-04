import os
from typing import List, Optional

from pyspark.sql import SparkSession

from fabricks.context import CATALOG, CONF_RUNTIME, PATH_MASKS, SPARK
from fabricks.context.log import DEFAULT_LOGGER

MASK_SCHEMA = CONF_RUNTIME.mask_options.schema_name or "default" if CONF_RUNTIME.mask_options else "default"
MASK_PREFIX = CONF_RUNTIME.mask_options.prefix or "mask_" if CONF_RUNTIME.mask_options else "mask_"


def register_all_masks(override: bool = False):
    """
    Register all masks.
    """

    DEFAULT_LOGGER.info("register masks", extra={"label": "fabricks"})
    for mask in get_masks():
        split = mask.split(".")
        try:
            register_mask(mask=split[0], override=override)
        except Exception as e:
            DEFAULT_LOGGER.exception(f"could not register mask {mask}", exc_info=e, extra={"label": "fabricks"})


def get_masks() -> List[str]:
    return [os.path.basename(f) for f in PATH_MASKS.walk()]


def is_registered(mask: str, spark: Optional[SparkSession] = None) -> bool:
    if spark is None:
        spark = SPARK
    assert spark is not None

    df = spark.sql(f"show user functions in {MASK_SCHEMA}")

    if CATALOG:
        df = df.where(f"function == '{CATALOG}.{MASK_SCHEMA}.{MASK_PREFIX}{mask}'")
    else:
        df = df.where(f"function == 'spark_catalog.{MASK_SCHEMA}.{MASK_PREFIX}{mask}'")

    return not df.isEmpty()


def register_mask(mask: str, override: Optional[bool] = False, spark: Optional[SparkSession] = None):
    if spark is None:
        spark = SPARK
    assert spark is not None

    if not is_registered(mask, spark) or override:
        if override:
            DEFAULT_LOGGER.debug(f"drop mask {mask}", extra={"label": "fabricks"})
        else:
            DEFAULT_LOGGER.debug(f"register mask {mask}", extra={"label": "fabricks"})

        path = PATH_MASKS.joinpath(f"{mask}.sql")
        spark.sql(path.get_sql())
