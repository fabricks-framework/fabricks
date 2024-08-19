from dataclasses import dataclass
from typing import Final

from fabricks.context import FABRICKS_STORAGE, PATH_RUNTIME
from fabricks.utils.path import Path


@dataclass(frozen=True)
class Paths:
    tests: Path
    landing: Path
    raw: Path
    out: Path


paths: Final[Paths] = Paths(
    tests=Path(PATH_RUNTIME.pathlib.parent.resolve(), assume_git=True),
    landing=FABRICKS_STORAGE.join("landing"),
    raw=FABRICKS_STORAGE.join("raw"),
    out=FABRICKS_STORAGE.join("out"),
)

steps = ["bronze", "silver", "transf", "gold", "semantic"]
