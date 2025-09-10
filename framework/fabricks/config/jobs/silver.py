from typing import Literal, Optional

from fabricks.config.jobs.base import BaseJobConfig, DefaultOptions

SilverModes = Literal["memory", "append", "latest", "update", "combine"]


class SilverOptions(DefaultOptions):
    # default
    mode: SilverModes

    # optional
    deduplicate: Optional[bool] = None
    stream: Optional[bool] = None
    order_duplicate_by: Optional[dict[str, str]] = None


class SilverJobConfig(BaseJobConfig):
    extend: Literal["silver"] = "silver"
    step: Literal["silver"]

    options: Optional[SilverOptions] = None
