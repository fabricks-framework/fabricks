from typing import List, Literal, Optional

from fabricks.config.base import ModelBase
from fabricks.config.jobs.base import BaseJobConfig, ChangeDataCaptures, DefaultOptions, Operations

BronzeModes = Literal["memory", "append", "register"]


class ParserOptions(ModelBase):
    file_format: Optional[str]
    read_options: Optional[dict[str, str]]


class BronzeOptions(DefaultOptions):
    # default
    mode: BronzeModes
    change_data_capture: ChangeDataCaptures = "none"

    # mandatory
    uri: str
    parser: str
    source: str

    # preferred
    keys: Optional[List[str]] = None

    # optional
    encrypted_columns: Optional[List[str]] = None
    calculated_columns: Optional[dict[str, str]] = None
    operation: Optional[Operations] = None


class BronzeJobConfig(BaseJobConfig):
    extend: Literal["bronze"] = "bronze"
    step: Literal["bronze"]

    options: Optional[BronzeOptions] = None
    parser_options: Optional[ParserOptions] = None
