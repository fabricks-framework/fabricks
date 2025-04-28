import os
from pathlib import Path as PathlibPath
from typing import List, Optional, Union

from pyspark.sql.dataframe import DataFrame

from fabricks.utils.spark import spark


class Path:
    def __init__(self, path: Union[str, PathlibPath], assume_git: bool = False):
        self.assume_git = assume_git

        # // is replaced by / by pathlib
        if isinstance(path, PathlibPath):
            self.path: str = str(path).replace("abfss:/", "abfss://")
        else:
            self.path: str = str(path)

    @classmethod
    def from_uri(
        cls,
        uri: str,
        regex: Optional[dict[str, str]] = None,
        assume_git: Optional[bool] = False,
    ):
        uri = uri.strip()
        if assume_git is None:
            assume_git = False
        if regex:
            import re

            for key, value in regex.items():
                uri = re.sub(rf"{key}", value, uri)

        return cls(uri, assume_git=assume_git)

    @property
    def pathlib(self) -> PathlibPath:
        return PathlibPath(self.string)

    @property
    def string(self) -> str:
        return self.path

    def get_container(self) -> str:
        import re

        assert self.string.startswith("abfss://")
        r = re.compile(r"(?<=abfss:\/\/)(.+?)(?=@)")
        m = re.findall(r, self.string)[0]
        return m

    def get_storage_account(self) -> str:
        import re

        assert self.string.startswith("abfss://")
        r = re.compile(r"(?<=@)(.+?)(?=\.)")
        m = re.findall(r, self.string)[0]
        return m

    def get_file_name(self) -> str:
        return self.pathlib.name

    def get_file_system(self) -> str:
        import re

        assert self.string.startswith("abfss://")
        r = re.compile(r"(?<=\.)(.+)(?=\/)")
        m = re.findall(r, self.string)[0]
        return m

    def get_dbfs_mnt_path(self) -> str:
        mount_point = self.pathlib.parts[1].split(".")[0].split("@")[0]
        rest = self.pathlib.parts[2:]
        return str(os.path.join("/dbfs/mnt", mount_point, "/".join(rest)))

    def get_notebook_path(self) -> str:
        return self.path.replace("Workspace/", "")

    def get_sql(self) -> str:
        p = self.string
        if not p.endswith(".sql"):
            p += ".sql"
        with open(p, "r") as f:
            sql = f.read()
        return sql

    def is_sql(self) -> bool:
        return self.string.endswith(".sql")

    def exists(self) -> bool:
        from databricks.sdk.runtime import dbutils

        try:
            if self.assume_git:
                return self.pathlib.exists()
            else:
                dbutils.fs.ls(self.string)
                return True
        except Exception:
            return False

    def join(self, *other):
        new_path = self.pathlib.joinpath(*other)
        return Path(path=new_path, assume_git=self.assume_git)

    def append(self, other: str):
        new_path = self.string + other
        return Path(path=new_path, assume_git=self.assume_git)

    def parent(self, *other):
        new_path = self.pathlib.parent
        return Path(path=new_path, assume_git=self.assume_git)

    def get_file_info(self) -> DataFrame:
        assert not self.assume_git
        rows = self._yield_file_info(self.string)
        df = spark.createDataFrame(
            rows,
            schema=["path", "name", "size", "modification_time"],
        )
        return df

    def walk(
        self, depth: Optional[int] = None, convert: Optional[bool] = False, file_format: Optional[str] = None
    ) -> List:
        out = []
        if self.exists():
            if self.pathlib.is_file():
                out = [self.string]
            elif depth:
                assert not self.assume_git
                out = self._list_fs(depth)
            else:
                if self.assume_git:
                    out = list(self._yield_git(self.string))
                else:
                    out = list(self._yield_fs(self.string))

        if file_format:
            out = [o for o in out if o.endswith(".sql")]
        if convert:
            out = [Path(o) for o in out]
        return out

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

    def _yield_fs(self, path: str):
        from databricks.sdk.runtime import dbutils

        for child in dbutils.fs.ls(path):
            if child.isDir():  # type: ignore
                yield from self._yield_fs(child.path)
            else:
                yield str(child.path)

    def _yield_git(self, path: Union[str, PathlibPath]):
        if isinstance(path, str):
            path = PathlibPath(path)

        for child in path.glob(r"*"):
            if child.is_dir():
                yield from self._yield_git(child)
            else:
                yield str(child)

    def rm(self):
        from databricks.sdk.runtime import dbutils

        if self.exists():
            list(self._rm(self.string))
            dbutils.fs.rm(self.string, recurse=True)

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

    def __str__(self) -> str:
        return self.string
