from typing import Optional, TypedDict


class ParserOptions(TypedDict):
    file_format: Optional[str]
    read_options: Optional[dict[str, str]]
