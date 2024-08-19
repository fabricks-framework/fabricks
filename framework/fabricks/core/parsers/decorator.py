from typing import Callable, Optional

from framework.fabricks.core.parsers.base import PARSERS, BaseParser
from framework.fabricks.core.parsers.types import ParserOptions


def parser(name: str):
    def decorator(parser: Callable[[Optional[ParserOptions]], BaseParser]):
        PARSERS[name] = parser

    return decorator
