import subprocess
from typing import List, Optional, Union

from fabricks.utils.path import Path


def pip_package(
    package: Union[str, List[str]],
    whl_path: Optional[Path] = None,
    tgt_path: Optional[Path] = None,
):
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
        out = subprocess.run(args + [p], capture_output=True)
        if out.returncode == 1:
            raise ValueError(p, out.stderr)


def pip_requirements(
    requirements_path: Path,
    whl_path: Optional[Path] = None,
    tgt_path: Optional[Path] = None,
):
    r = requirements_path.string

    args = ["pip", "install"]

    if whl_path:
        w = whl_path.get_dbfs_mnt_path()
        args += ["--no-index", f"--find-links={w}"]

    if tgt_path:
        t = tgt_path.get_dbfs_mnt_path()
        args += ["--target", t]

    out = subprocess.run(args + ["-r", r], capture_output=True)
    if out.returncode == 1:
        raise ValueError(r, out.stderr)


def pip_wheel(requirement_path: Path, whl_path: Path):
    import subprocess

    r = requirement_path.string
    w = whl_path.get_dbfs_mnt_path()

    out = subprocess.run(["pip", "wheel", "--wheel-dir", w, "-r", r], capture_output=True)
    if out.returncode == 1:
        raise ValueError(r, out.stderr)
