"""Path utilities without Spark dependencies."""

from pathlib import Path as PathlibPath

from typing_extensions import deprecated

from fabricks.utils.path.base import BasePath
from fabricks.utils.path.file_share import FileSharePath, resolve_fileshare_path
from fabricks.utils.path.git import GitPath, resolve_git_path

__all__ = [
    "BasePath",
    "GitPath",
    "Path",
    "resolve_git_path",
    "FileSharePath",
    "resolve_fileshare_path",
]


@deprecated("Use GitPath or FileSharePath directly instead.")
class Path:
    """
    Legacy Path class with assume_git flag for backward compatibility.
    """

    def __new__(cls, path: str | PathlibPath, assume_git: bool = False):
        if assume_git:
            return GitPath(path)
        else:
            return FileSharePath(path)

    @classmethod
    def from_uri(
        cls,
        uri: str,
        regex: dict[str, str] | None = None,
        assume_git: bool | None = False,
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

        path_class = GitPath if assume_git else None
        if path_class is None:
            path_class = FileSharePath

        return path_class.from_uri(uri, regex=regex)
