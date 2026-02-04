"""Path configuration models."""

from dataclasses import dataclass

from fabricks.utils.path import FileSharePath, GitPath


@dataclass(frozen=True)
class Paths:
    """Runtime path references."""

    to_storage: "FileSharePath"
    to_tmp: "FileSharePath"
    to_checkpoints: "FileSharePath"
    to_commits: "FileSharePath"
    to_schema: "FileSharePath"
    to_runtime: "GitPath"
