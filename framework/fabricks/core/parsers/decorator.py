from typing import Callable, Optional

from fabricks.core.parsers.base import PARSERS, BaseParser
from fabricks.core.parsers.types import ParserOptions


def parser(name: str):
    def decorator(parser: Callable[[Optional[ParserOptions]], BaseParser]):
        PARSERS[name] = parser

    return decorator
