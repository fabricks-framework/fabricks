import posixpath
from abc import ABC, abstractmethod
from pathlib import Path as PathlibPath
from typing import List, Optional, Union

from pyspark.sql.dataframe import DataFrame
from typing_extensions import deprecated

from fabricks.utils.spark import spark


class BasePath(ABC):
    """Abstract base class for all path types."""

    def __init__(self, path: Union[str, PathlibPath]):
        """Initialize the path."""
        if isinstance(path, PathlibPath):
            path = path.as_posix()

        new_path = str(path)
        if new_path.startswith("abfss:/") and not new_path.startswith("abfss://"):
            new_path = new_path.replace("abfss:/", "abfss://")

        self.path: str = new_path

    @classmethod
    def from_uri(
        cls,
        uri: str,
        regex: Optional[dict[str, str]] = None,
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
        depth: Optional[int] = None,
        convert: Optional[bool] = False,
        file_format: Optional[str] = None,
    ) -> List:
        """Walk the path and return all files."""

    @abstractmethod
    def _yield(self, path: Union[str, PathlibPath]):
        """Recursively yield all file paths under the given path."""

    def __str__(self) -> str:
        return self.string


class GitPath(BasePath):
    def __init__(self, path: Union[str, PathlibPath]):
        super().__init__(path=path)

    def exists(self) -> bool:
        """Check if the path exists in the local/git file system."""
        try:
            return self.pathlibpath.exists()
        except Exception:
            return False

    def get_notebook_path(self) -> str:
        """Get the notebook path for Databricks workspace."""
        path = self.path.replace("Workspace/", "")
        if path.endswith(".ipynb"):
            path = path.replace(".ipynb", "")
        if path.endswith(".py"):
            path = path.replace(".py", "")
        return path

    def walk(
        self,
        depth: Optional[int] = None,
        convert: Optional[bool] = False,
        file_format: Optional[str] = None,
    ) -> List:
        out = []
        if self.exists():
            if self.pathlibpath.is_file():
                out = [self.string]
            else:
                out = list(self._yield(self.string))

        if file_format:
            out = [o for o in out if o.endswith(".sql")]
        if convert:
            out = [self.__class__(o) for o in out]
        return out

    def _yield(self, path: Union[str, PathlibPath]):
        """Recursively yield all file paths in the git/local file system."""
        if isinstance(path, str):
            path = PathlibPath(path)

        for child in path.glob(r"*"):
            if child.is_dir():
                yield from self._yield(child)
            else:
                yield str(child)


class FileSharePath(BasePath):
    def __init__(self, path: Union[str, PathlibPath]):
        super().__init__(path=path)

    def exists(self) -> bool:
        """Check if the path exists in the distributed file system."""
        try:
            from fabricks.utils.spark import dbutils

            assert dbutils is not None, "dbutils not found"
            dbutils.fs.ls(self.string)
            return True
        except Exception:
            return False

    def get_container(self) -> str:
        """Get the container name from an ABFSS path."""
        import re

        assert self.string.startswith("abfss://")

        r = re.compile(r"(?<=abfss:\/\/)(.+?)(?=@)")
        m = re.findall(r, self.string)[0]
        return m

    def get_storage_account(self) -> str:
        """Get the storage account name from an ABFSS path."""
        import re

        assert self.string.startswith("abfss://")

        r = re.compile(r"(?<=@)(.+?)(?=\.)")
        m = re.findall(r, self.string)[0]
        return m

    def get_file_system(self) -> str:
        """Get the file system from an ABFSS path."""
        import re

        assert self.string.startswith("abfss://")

        r = re.compile(r"(?<=\.)(.+)(?=\/)")
        m = re.findall(r, self.string)[0]
        return m

    def get_dbfs_mnt_path(self) -> str:
        """Get the DBFS mount path."""
        import os

        mount_point = self.pathlibpath.parts[1].split(".")[0].split("@")[0]
        rest = self.pathlibpath.parts[2:]

        return str(os.path.join("/dbfs/mnt", mount_point, "/".join(rest)))

    def walk(
        self,
        depth: Optional[int] = None,
        convert: Optional[bool] = False,
        file_format: Optional[str] = None,
    ) -> List:
        out = []
        if self.exists():
            if self.pathlibpath.is_file():
                out = [self.string]
            elif depth:
                out = self._list_fs(depth)
            else:
                out = list(self._yield(self.string))

        if file_format:
            out = [o for o in out if o.endswith(".sql")]
        if convert:
            out = [self.__class__(o) for o in out]
        return out

    def get_file_info(self) -> DataFrame:
        rows = self._yield_file_info(self.string)
        df = spark.createDataFrame(
            rows,
            schema=["path", "name", "size", "modification_time"],
        )
        return df

    def rm(self):
        from databricks.sdk.runtime import dbutils

        if self.exists():
            list(self._rm(self.string))
            dbutils.fs.rm(self.string, recurse=True)

    def _list_fs(self, depth: int) -> List:
        from databricks.sdk.runtime import dbutils

        paths = dbutils.fs.ls(self.string)

        if depth == 1:
            children = paths
        else:
            i = 1
            children = []
            while True:
                if i == depth:
                    break
                else:
                    children = []

                for path in paths:
                    children += dbutils.fs.ls(path.path)

                paths = children
                i += 1

        return [c.path for c in children]

    def _yield_file_info(self, path: str):
        from databricks.sdk.runtime import dbutils

        for child in dbutils.fs.ls(path):
            if child.isDir():  # type: ignore
                yield from self._yield_file_info(child.path)
            else:
                yield dbutils.fs.ls(child.path)[0]

    def _yield(self, path: Union[str, PathlibPath]):
        """Recursively yield all file paths in the distributed file system."""
        from databricks.sdk.runtime import dbutils

        path_str = str(path)
        for child in dbutils.fs.ls(path_str):
            if child.isDir():  # type: ignore
                yield from self._yield(child.path)
            else:
                yield str(child.path)

    def _rm(self, path: str):
        from databricks.sdk.runtime import dbutils

        try:
            for child in dbutils.fs.ls(path):
                if child.isDir():  # type: ignore
                    yield from self._rm(child.path)
                else:
                    yield dbutils.fs.rm(child.path, recurse=True)

        except Exception:
            return False


def resolve_git_path(
    path: str | None,
    default: str | None = None,
    base: GitPath | str | None = None,
    variables: dict[str, str] | None = None,
) -> GitPath:
    """
    Resolve a path as a GitPath with optional variable substitution and base path joining.

    Args:
        path: The path string from configuration
        default: Default value if path is None
        base: Base path to join with (must be GitPath or str)
        apply_variables: Whether to apply variable substitution using VARIABLES
        variables: Dictionary of variable substitutions

    Returns:
        Resolved GitPath object
    """
    if isinstance(base, str):
        base = GitPath(base)

    resolved_value = path or default
    if resolved_value is None:
        raise ValueError("path and default cannot both be None")

    if variables:
        return GitPath.from_uri(resolved_value, regex=variables)

    if base:
        return base.joinpath(resolved_value)

    return GitPath(resolved_value)


def resolve_fileshare_path(
    path: str | None,
    default: str | None = None,
    base: FileSharePath | str | None = None,
    variables: dict[str, str] | None = None,
) -> FileSharePath:
    """
    Resolve a path as a FileSharePath with optional variable substitution and base path joining.

    Args:
        path: The path string from configuration
        default: Default value if path is None
        base: Base path to join with (must be FileSharePath or str)
        apply_variables: Whether to apply variable substitution using VARIABLES
        variables: Dictionary of variable substitutions

    Returns:
        Resolved FileSharePath object
    """
    if isinstance(base, str):
        base = FileSharePath(base)

    resolved_value = path or default
    if resolved_value is None:
        raise ValueError("path and default cannot both be None")

    if variables:
        return FileSharePath.from_uri(resolved_value, regex=variables)

    if base:
        return base.joinpath(resolved_value)

    return FileSharePath(resolved_value)


@deprecated("Use GitPath or FileSharePath directly instead.")
class Path:
    """
    Legacy Path class with assume_git flag for backward compatibility.
    """

    def __new__(cls, path: Union[str, PathlibPath], assume_git: bool = False):
        if assume_git:
            return GitPath(path)
        else:
            return FileSharePath(path)

    @classmethod
    def from_uri(
        cls,
        uri: str,
        regex: Optional[dict[str, str]] = None,
        assume_git: Optional[bool] = False,
    ):
        """
        Create a path from a URI with optional regex substitution.

        Args:
            uri: The URI string
            regex: Dictionary of regex patterns to substitute
            assume_git: If True, return GitPath; otherwise FileSharePath

        Returns:
            GitPath if assume_git is True, FileSharePath otherwise
        """
        if assume_git is None:
            assume_git = False

        path_class = GitPath if assume_git else FileSharePath
        return path_class.from_uri(uri, regex=regex)
