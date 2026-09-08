import importlib
from pathlib import Path

import pytest

from fabricks.utils.path.local import LocalFileSharePath


def test_exists_false_for_missing_path(tmp_path: Path):
    p = LocalFileSharePath(str(tmp_path / "missing"))
    assert p.exists() is False


def test_exists_true_and_walk_lists_files(tmp_path: Path):
    (tmp_path / "a.txt").write_text("a")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.txt").write_text("b")

    p = LocalFileSharePath(str(tmp_path))
    assert p.exists() is True

    found = {Path(f).name for f in p.walk()}
    assert found == {"a.txt", "b.txt"}


def test_walk_file_format_filter(tmp_path: Path):
    (tmp_path / "a.txt").write_text("a")
    (tmp_path / "b.parquet").write_text("b")

    p = LocalFileSharePath(str(tmp_path))
    found = p.walk(file_format="parquet")
    assert len(found) == 1
    assert found[0].endswith("b.parquet")


def test_rm_removes_directory(tmp_path: Path):
    target = tmp_path / "victim"
    target.mkdir()
    (target / "f.txt").write_text("x")

    p = LocalFileSharePath(str(target))
    assert p.exists() is True
    p.rm()
    assert p.exists() is False


def test_joinpath_preserves_class(tmp_path: Path):
    p = LocalFileSharePath(str(tmp_path))
    child = p.joinpath("sub", "dir")
    assert isinstance(child, LocalFileSharePath)


@pytest.fixture
def set_environment(monkeypatch):
    from fabricks.utils import environment as environment_module
    from fabricks.utils.path import file_share as file_share_module

    def _set(value: str):
        monkeypatch.setenv("FABRICKS_ENVIRONMENT", value)
        importlib.reload(environment_module)
        importlib.reload(file_share_module)
        return file_share_module

    yield _set

    monkeypatch.delenv("FABRICKS_ENVIRONMENT", raising=False)
    importlib.reload(environment_module)
    importlib.reload(file_share_module)


def test_resolve_fileshare_path_docker_returns_local(tmp_path, set_environment):
    fs = set_environment("docker")
    p = fs.resolve_fileshare_path(str(tmp_path / "gold"))
    assert isinstance(p, LocalFileSharePath)


def test_resolve_fileshare_path_databricks_returns_fileshare(set_environment):
    fs = set_environment("databricks")
    p = fs.resolve_fileshare_path("abfss://gold@storage.dfs.core.windows.net/gold")
    # fs.FileSharePath, not a module-level import: `set_environment` reloads
    # fabricks.utils.path.file_share, which rebinds FileSharePath to a new
    # class object each time -- isinstance against a pre-reload import would
    # spuriously fail.
    assert isinstance(p, fs.FileSharePath)
    assert not isinstance(p, LocalFileSharePath)


def test_resolve_fileshare_path_databricks_rejects_non_abfss(set_environment):
    fs = set_environment("databricks")
    with pytest.raises(AssertionError):
        fs.resolve_fileshare_path("/not/an/abfss/path")
