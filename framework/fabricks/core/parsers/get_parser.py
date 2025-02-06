import sys
from importlib.util import spec_from_file_location
from typing import Optional

from fabricks.context import PATH_PARSERS
from fabricks.core.parsers._types import ParserOptions
from fabricks.core.parsers.base import PARSERS, BaseParser


def get_parser(name: str, parser_options: Optional[ParserOptions] = None) -> BaseParser:
    if name not in ["json", "parquet", "avro", "csv", "tsv", "delta", "table"]:
        sys.path.append(PATH_PARSERS.string)

        path = PATH_PARSERS.join(name).append(".py")
        assert path.exists(), f"parser not found ({path})"
        spec = spec_from_file_location(name, path.string)
        assert spec, f"parser not found ({path})"
        spec.loader.load_module()  # type: ignore

        parser = PARSERS[name](parser_options)
    else:
        parser = BaseParser(parser_options, name)

    assert parser
    return parser
