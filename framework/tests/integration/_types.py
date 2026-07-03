from dataclasses import dataclass
from typing import Final

from fabricks.context import FABRICKS_STORAGE, PATH_RUNTIME
from fabricks.utils.path import FileSharePath, GitPath


@dataclass(frozen=True)
class Paths:
    root: GitPath
    landing: FileSharePath
    raw: FileSharePath
    out: FileSharePath

    def __str__(self) -> str:
        return f"{self.root} {self.landing} {self.raw} {self.out}"


PATHS: Final[Paths] = Paths(
    root=PATH_RUNTIME.parent().parent().joinpath("integration"),
    landing=FABRICKS_STORAGE.joinpath("landing"),
    raw=FABRICKS_STORAGE.joinpath("raw"),
    out=FABRICKS_STORAGE.joinpath("out"),
)
STEPS: Final[list[str]] = ["bronze", "silver", "transf", "gold", "semantic"]
