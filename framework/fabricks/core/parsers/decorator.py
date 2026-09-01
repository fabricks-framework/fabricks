from collections.abc import Callable

from fabricks.core.parsers.base import PARSERS, BaseParser
from fabricks.models import ParserOptions


def parser(name: str) -> Callable[[Callable[[ParserOptions | None], BaseParser]], None]:
    def decorator(parser: Callable[[ParserOptions | None], BaseParser]) -> None:
        PARSERS[name] = parser

    return decorator
