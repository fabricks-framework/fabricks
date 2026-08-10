# keep legacy imports for backwards compatibility

from fabricks.core.legacy.parsers.base import PARSERS, BaseParser
from fabricks.core.legacy.parsers.decorator import parser
from fabricks.core.legacy.parsers.get_parser import get_parser

__all__ = [
    "BaseParser",
    "get_parser",
    "parser",
    "PARSERS",
]
