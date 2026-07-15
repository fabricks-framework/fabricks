from typing import Final

from fabricks.context import FABRICKS_STORAGE, PATH_RUNTIME
from fabricks.utils.path import FileSharePath, GitPath

ROOT: Final[GitPath] = PATH_RUNTIME.parent().parent().joinpath("integration")
PHASES: Final[GitPath] = ROOT.joinpath("phases")
LANDING: Final[FileSharePath] = FABRICKS_STORAGE.joinpath("landing")
RAW: Final[FileSharePath] = FABRICKS_STORAGE.joinpath("raw")
OUT: Final[FileSharePath] = FABRICKS_STORAGE.joinpath("out")
STEPS: Final[list[str]] = ["bronze", "silver", "transf", "gold", "semantic"]
