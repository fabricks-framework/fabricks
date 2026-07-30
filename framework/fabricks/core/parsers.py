# keep legacy imports for backwards compatibility

from fabricks.legacy.parsers.base import PARSERS, BaseParser
from fabricks.legacy.parsers.decorator import parser
from fabricks.legacy.parsers.get_parser import get_parser

__all__ = [
    "BaseParser",
    "get_parser",
    "parser",
    "PARSERS",
]
