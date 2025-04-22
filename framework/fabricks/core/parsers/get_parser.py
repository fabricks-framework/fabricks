from importlib.util import module_from_spec, spec_from_file_location
from typing import Optional

from fabricks.context import PATH_PARSERS
from fabricks.core.parsers._types import ParserOptions
from fabricks.core.parsers.base import PARSERS, BaseParser


def get_parser(name: str, parser_options: Optional[ParserOptions] = None) -> BaseParser:
    if name not in ["json", "parquet", "avro", "csv", "tsv", "delta", "table"]:
        path = PATH_PARSERS.join(name).append(".py")
        assert path.exists(), f"parser not found ({path})"

        spec = spec_from_file_location(name, path.string)
        assert spec, f"parser not found ({path})"
        assert spec.loader is not None

        mod = module_from_spec(spec)
        spec.loader.exec_module(mod)
        parser = PARSERS[name](parser_options)

    else:
        parser = BaseParser(parser_options, name)

    assert parser
    return parser
