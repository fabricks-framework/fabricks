from typing import List, Literal, Optional

from pydantic import model_validator
from typing_extensions import Self

from fabricks.models.config.base import ModelBase
from fabricks.models.config.jobs.base import BaseJobConfig, DefaultOptions, Operations

BronzeModes = Literal["memory", "append", "register"]


class ParserOptions(ModelBase):
    file_format: Optional[str] = None
    read_options: Optional[dict[str, str]] = None


class BronzeOptions(DefaultOptions):
    # default
    mode: BronzeModes

    # mandatory
    uri: str

    # preferred
    parser: Optional[str] = None
    keys: Optional[List[str]] = None

    # optional
    source: Optional[str] = None
    encrypted_columns: Optional[List[str]] = None
    calculated_columns: Optional[dict[str, str]] = None
    operation: Optional[Operations] = None

    @model_validator(mode="after")
    def check_parser(self) -> Self:
        if self.parser is None and self.mode not in ("register"):
            raise ValueError("parser required when mode not in ('register')")
        return self


class BronzeJobConfig(BaseJobConfig):
    extend: Literal["bronze"] = "bronze"

    options: BronzeOptions
    parser_options: Optional[ParserOptions] = None
