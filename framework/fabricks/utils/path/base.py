import posixpath
from abc import ABC, abstractmethod
from pathlib import Path as PathlibPath


class BasePath(ABC):
    """Abstract base class for all path types."""

    def __init__(self, path: str | PathlibPath):
        """Initialize the path."""
        if isinstance(path, PathlibPath):
            path = path.as_posix()

        new_path = str(path)
        if new_path.startswith("abfss:/") and not new_path.startswith("abfss://"):
            new_path = new_path.replace("abfss:/", "abfss://")

        self.path: str = new_path

    def __json__(self):
        """Return the JSON representation of the path."""
        return self.string

    @classmethod
    def from_uri(
        cls,
        uri: str,
        regex: dict[str, str] | None = None,
    ):
        """Create a path from a URI with optional regex substitution."""
        uri = uri.strip()
        if regex:
            import re

            for key, value in regex.items():
                uri = re.sub(rf"{key}", value, uri)

        return cls(uri)

    @property
    def string(self) -> str:
        """Get the string representation of the path."""
        return self.path

    @property
    def pathlibpath(self) -> PathlibPath:
        """Get the pathlib representation of the path."""
        return PathlibPath(self.string)

    def get_file_name(self) -> str:
        """Get the file name from the path."""
        return self.pathlibpath.name

    def get_sql(self) -> str:
        """Read and return SQL content from a .sql file."""
        p = self.string
        if not p.endswith(".sql"):
            p += ".sql"

        with open(p, "r") as f:
            sql = f.read()

        return sql

    def is_sql(self) -> bool:
        """Check if the path points to a SQL file."""
        return self.string.endswith(".sql")

    def joinpath(self, *other):
        """Join this path with other path segments."""
        parts = [str(o) for o in other]
        base = self.string

        joined = posixpath.join(base, *parts)
        new = posixpath.normpath(joined)

        return self.__class__(path=new)

    def append(self, other: str):
        """Append a string to the path."""
        new_path = self.string + other
        return self.__class__(path=new_path)

    def parent(self):
        """Get the parent directory of the path."""
        new_path = self.pathlibpath.parent
        return self.__class__(path=new_path)

    @abstractmethod
    def exists(self) -> bool:
        """Check if the path exists."""

    @abstractmethod
    def walk(
        self,
        depth: int | None = None,
        convert: bool | None = False,
        file_format: str | None = None,
    ) -> list:
        """Walk the path and return all files."""

    @abstractmethod
    def _yield(self, path: str | PathlibPath):
        """Recursively yield all file paths under the given path."""

    def __str__(self) -> str:
        return self.string
