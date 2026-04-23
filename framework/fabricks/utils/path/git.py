from pathlib import Path as PathlibPath

from fabricks.utils.path.base import BasePath


class GitPath(BasePath):
    def __init__(self, path: str | PathlibPath):
        super().__init__(path=path)

    def exists(self) -> bool:
        """Check if the path exists in the local/git file system."""
        try:
            return self.pathlibpath.exists()
        except Exception:
            return False

    def get_notebook_path(self) -> str:
        """Get the notebook path for Databricks workspace."""
        path = self.path
        if path.endswith(".ipynb"):
            path = path[: -len(".ipynb")]
        elif path.endswith(".py"):
            path = path[: -len(".py")]

        return path

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
            else:
                out = list(self._yield(self.string))

        if file_format:
            out = [o for o in out if o.endswith(file_format)]

        if convert:
            out = [self.__class__(o) for o in out]

        return out

    def _yield(self, path: str | PathlibPath):
        """Recursively yield all file paths in the git/local file system."""
        if isinstance(path, str):
            path = PathlibPath(path)

        for child in path.glob(r"*"):
            if child.is_dir():
                yield from self._yield(child)

            else:
                yield str(child)


def resolve_git_path(
    path: str | None,
    default: str | None = None,
    base: "GitPath | str | None" = None,
    variables: dict[str, str] | None = None,
) -> "GitPath":
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
