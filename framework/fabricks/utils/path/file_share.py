from pathlib import Path as PathlibPath

from fabricks.utils.path.base import BasePath


class FileSharePath(BasePath):
    def __init__(self, path: str | PathlibPath):
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
        depth: int | None = None,
        convert: bool | None = False,
        file_format: str | None = None,
    ) -> list:
        out = []
        if self.exists():
            if self.pathlibpath.is_file():
                out = [self.string]
            elif depth:
                out = self._list_fs(depth)
            else:
                out = list(self._yield(self.string))

        if file_format:
            out = [o for o in out if o.endswith(file_format)]

        if convert:
            out = [self.__class__(o) for o in out]

        return out

    def get_file_info(self) -> list[dict[str, str | int]]:
        rows = []

        for file_info in self._yield_file_info(self.string):
            rows.append(
                {
                    "path": file_info.path,
                    "name": file_info.name,
                    "size": file_info.size,
                    "modification_time": file_info.modificationTime,
                }
            )

        return rows

    def rm(self):
        from databricks.sdk.runtime import dbutils

        if self.exists():
            list(self._rm(self.string))
            dbutils.fs.rm(self.string, recurse=True)

    def _list_fs(self, depth: int) -> list:
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

    def _yield(self, path: str | PathlibPath):
        """Recursively yield all file paths in the distributed file system."""
        from databricks.sdk.runtime import dbutils

        if isinstance(path, PathlibPath):
            path = str(path)

        for child in dbutils.fs.ls(path):
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
