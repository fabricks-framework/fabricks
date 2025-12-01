from typing import Optional

from fabricks.context import PATH_PARSERS
from fabricks.core.parsers._types import ParserOptions
from fabricks.core.parsers.base import PARSERS, BaseParser
from fabricks.utils.helpers import load_module_from_path


def get_parser(name: str, parser_options: Optional[ParserOptions] = None) -> BaseParser:
    if name not in ["json", "parquet", "avro", "csv", "tsv", "delta", "table"]:
        path = PATH_PARSERS.joinpath(name).append(".py")
        assert path.exists(), f"parser not found ({path})"

        load_module_from_path(name, path)
        parser = PARSERS[name](parser_options)

    else:
        parser = BaseParser(parser_options, name)

    assert parser
    return parser
