from dataclasses import dataclass
from typing import Final

from fabricks.context import FABRICKS_STORAGE, PATH_RUNTIME
from fabricks.utils.path import FileSharePath, GitPath


@dataclass(frozen=True)
class Paths:
    tests: GitPath
    landing: FileSharePath
    raw: FileSharePath
    out: FileSharePath


paths: Final[Paths] = Paths(
    tests=GitPath(PATH_RUNTIME.pathlibpath.parent.resolve()),
    landing=FABRICKS_STORAGE.joinpath("landing"),
    raw=FABRICKS_STORAGE.joinpath("raw"),
    out=FABRICKS_STORAGE.joinpath("out"),
)

steps = ["bronze", "silver", "transf", "gold", "semantic"]
