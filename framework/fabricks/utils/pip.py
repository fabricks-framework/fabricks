from pathlib import Path
import re
import subprocess
import tomllib
from typing import Literal

from fabricks.utils.helpers import find_upward
from fabricks.utils.path import FileSharePath, GitPath


def pip_package(
    package: str | list[str], whl_path: FileSharePath | None = None, tgt_path: FileSharePath | None = None
) -> None:
    if isinstance(package, str):
        package = [package]

    args = ["pip", "install"]

    if whl_path:
        w = whl_path.get_dbfs_mnt_path()
        args += ["--no-index", f"--find-links={w}"]

    if tgt_path:
        t = tgt_path.get_dbfs_mnt_path()
        args += ["--target", t]

    for p in package:
        out = subprocess.run([*args, p], capture_output=True)
        if out.returncode == 1:
            raise ValueError(p, out.stderr)


def pip_requirements(
    requirements_path: FileSharePath, whl_path: FileSharePath | None = None, tgt_path: FileSharePath | None = None
) -> None:
    r = requirements_path.string

    args = ["pip", "install"]

    if whl_path:
        w = whl_path.get_dbfs_mnt_path()
        args += ["--no-index", f"--find-links={w}"]

    if tgt_path:
        t = tgt_path.get_dbfs_mnt_path()
        args += ["--target", t]

    out = subprocess.run([*args, "-r", r], capture_output=True)
    if out.returncode == 1:
        raise ValueError(r, out.stderr)


def pip_wheel(requirement_path: FileSharePath, whl_path: FileSharePath) -> None:
    r = requirement_path.string
    w = whl_path.get_dbfs_mnt_path()

    out = subprocess.run(["pip", "wheel", "--wheel-dir", w, "-r", r], capture_output=True)
    if out.returncode == 1:
        raise ValueError(r, out.stderr)


def pip_list(
    format: Literal["freeze", "pretty", "dict", "pyproject"] = "freeze",
    pyproject: bool = True,
    path: str | Path | GitPath | None = None,
) -> str | dict[str, str]:
    """
    List installed packages and their versions.

    Args:
        format: Output format - "freeze" (default), "pretty", "dict", or "pyproject"
                - "freeze": Returns pip freeze format (package==version)
                - "pretty": Returns human-readable list format
                - "dict": Returns dictionary {package: version}
                - "pyproject": Returns packages listed in pyproject.toml dependencies
        pyproject: If True, only show packages listed in pyproject.toml dependencies (default: True)
        path: Path to pyproject.toml file (default: searches up from current file)

    Returns:
        String output or dictionary depending on format parameter

    Raises:
        ValueError: If pip command fails or invalid format specified
        FileNotFoundError: If pyproject.toml not found when pyproject=True
    """
    # Get all installed packages
    out = subprocess.run(["pip", "freeze"], capture_output=True, text=True)
    if out.returncode != 0:
        raise ValueError("pip freeze failed", out.stderr)

    # Parse installed packages into dict
    installed = {}
    for line in out.stdout.strip().split("\n"):
        if line and "==" in line:
            package, version = line.split("==", 1)
            installed[package.lower()] = (package, version)

    if pyproject:
        if path is None:
            path = find_upward("pyproject.toml")

        if path is None:
            raise FileNotFoundError("pyproject.toml not found nor provided")

        with Path(str(path)).open("rb") as f:
            content = tomllib.load(f)

        dependencies = content.get("project", {}).get("dependencies", [])
        parsed = set()
        for d in dependencies:
            # Extract package name from dependency specification (e.g., "pandas>=2.0.0" -> "pandas")
            match = re.match(r"^([a-zA-Z0-9_-]+)", d)
            if match:
                parsed.add(match.group(1).lower())

        installed = {k: v for k, v in installed.items() if k in parsed}

    if format == "freeze":
        return "\n".join(f"{pkg}=={ver}" for pkg, ver in installed.values())

    if format == "pretty":
        lines = ["Package            Version", "------------------ -------"]
        for pkg, ver in sorted(installed.values()):
            lines.append(f"{pkg:<18} {ver}")
        return "\n".join(lines)

    if format == "dict":
        return dict(installed.values())

    if format == "pyproject":
        lines = ["dependencies = ["]
        for pkg, ver in sorted(installed.values()):
            lines.append(f'    "{pkg}=={ver}",')
        lines.append("]")
        return "\n".join(lines)

    raise ValueError(f'Invalid format: {format}. Supported formats are: "freeze", "pretty", "dict", "pyproject"')
