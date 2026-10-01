import pytest

from fabricks.utils.path import GitPath

NOTEBOOKS = "/Workspace/Users/alice@example.com/my_repo/notebooks"


@pytest.mark.parametrize(
    ("file_name", "expected"),
    [
        ("extract_table.py", "extract_table"),
        ("extract_table.ipynb", "extract_table"),
        ("extract_table", "extract_table"),
        ("extract_table.sql", "extract_table.sql"),
    ],
    ids=["py", "ipynb", "no-extension", "other-extension-kept"],
)
def test_get_notebook_path_strips_only_notebook_extensions(file_name, expected):
    assert GitPath(f"{NOTEBOOKS}/{file_name}").get_notebook_path() == f"{NOTEBOOKS}/{expected}"


def test_get_notebook_path_only_strips_the_trailing_extension():
    assert GitPath("/Workspace/repo/a.py/b").get_notebook_path() == "/Workspace/repo/a.py/b"
