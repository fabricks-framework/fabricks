from typing import Callable, Optional

from fabricks.core.parsers._types import ParserOptions
from fabricks.core.parsers.base import PARSERS, BaseParser


def parser(name: str):
    def decorator(parser: Callable[[Optional[ParserOptions]], BaseParser]):
        PARSERS[name] = parser

    return decorator
