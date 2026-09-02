from collections.abc import Iterator
from pathlib import Path as PathlibPath
import shutil

from fabricks.utils.path.base import BasePath


class LocalFileSharePath(BasePath):
    """A BasePath backed by the plain local filesystem (pathlib), no dbutils.

    Used as the storage root for local (non-Databricks) tests, where
    FileSharePath's dbutils.fs.* calls have no equivalent.
    """

    def __init__(self, path: str | PathlibPath) -> None:
        super().__init__(path=path)

    def exists(self) -> bool:
        return self.pathlibpath.exists()

    def walk(self, depth: int | None = None, convert: bool | None = False, file_format: str | None = None) -> list:  # noqa: ARG002 - `depth` kept to match BasePath.walk
        if not self.exists():
            return []

        out = [self.string] if self.pathlibpath.is_file() else list(self._yield(self.string))

        if file_format:
            out = [o for o in out if o.endswith(file_format)]

        if convert:
            out = [self.__class__(o) for o in out]

        return out

    def _yield(self, path: str | PathlibPath) -> Iterator[str]:
        if isinstance(path, str):
            path = PathlibPath(path)

        for child in path.glob("*"):
            if child.is_dir():
                yield from self._yield(child)
            else:
                yield str(child)

    def rm(self) -> None:
        if self.exists():
            if self.pathlibpath.is_dir():
                shutil.rmtree(self.pathlibpath)
            else:
                self.pathlibpath.unlink()
