from typing import Literal, Optional

from fabricks.config.jobs.base import BaseJobConfig, DefaultOptions

GoldModes = Literal["memory", "append", "complete", "update", "invoke"]


class GoldOptions(DefaultOptions):
    # default
    mode: GoldModes

    # optional
    update_where: Optional[str] = None
    deduplicate: Optional[bool] = None  # remove duplicates on the keys and on the hash
    rectify_as_upserts: Optional[bool] = None  # convert reloads into upserts and deletes
    correct_valid_from: Optional[bool] = None
    persist_last_timestamp: Optional[bool] = None
    table: Optional[str] = None
    notebook: Optional[bool] = None
    requirements: Optional[bool] = None


class GoldJobConfig(BaseJobConfig):
    extend: Literal["gold"] = "gold"
    step: Literal["gold"]

    options: Optional[GoldOptions] = None
