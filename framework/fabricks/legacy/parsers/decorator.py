from typing import Callable, Optional

from typing_extensions import deprecated

from fabricks.legacy.parsers.base import PARSERS, BaseParser
from fabricks.models import ParserOptions


@deprecated("use pre-run invoker notebook instead")
def parser(name: str):
    def decorator(parser: Callable[[Optional[ParserOptions]], BaseParser]):
        PARSERS[name] = parser

    return decorator
