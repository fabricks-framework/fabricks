from __future__ import annotations

from pathlib import Path
from typing import Generator, List, Optional, TypeVar, cast

import yaml
from pydantic import BaseModel, ConfigDict

T = TypeVar("T", bound="ModelBase")


class ModelBase(BaseModel):
    # Ignore extra/unknown fields (TypedDict-like strictness)
    model_config = ConfigDict(extra="ignore")
    file: Optional[Path] = None

    @classmethod
    def from_file(cls: type[T], path: Path, root: Optional[str] = None) -> Generator[T, None, None]:
        with open(path, "r", encoding="utf-8") as f:
            content = yaml.safe_load(f)

            if not isinstance(content, List):
                if root:
                    yield cls(**cast(dict, content[root]), file=path)
                else:
                    yield cls(**cast(dict, content), file=path)
            else:
                if root:
                    yield from (cls(**cast(dict, c[root]), file=path) for c in content)
                else:
                    yield from (cls(**cast(dict, c), file=path) for c in content)

    @classmethod
    def from_files(
        cls: type[T], path: Path, root: Optional[str] = None, preferred_file_name: Optional[str] = None
    ) -> Generator[T, None, None]:
        found = False

        for child in path.iterdir():
            if child.suffix not in [".yaml", ".yml"]:
                continue
            elif preferred_file_name is not None and preferred_file_name not in child.name:
                continue
            else:
                found = True

            yield from cls.from_file(child, root=root)

        if preferred_file_name is not None and not found:
            yield from cls.from_files(child, root=root, preferred_file_name=None)
