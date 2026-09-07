from fabricks.utils.path import GitPath


def test_git_path_keeps_workspace_prefix():
    path = GitPath("/Workspace/Users/alice@example.com/my_repo/notebooks/extract_table.py")

    assert path.get_notebook_path() == "/Workspace/Users/alice@example.com/my_repo/notebooks/extract_table"


def test_git_path_strips_ipynb_extension():
    path = GitPath("/Workspace/Users/alice@example.com/my_repo/notebooks/extract_table.ipynb")

    assert path.get_notebook_path() == "/Workspace/Users/alice@example.com/my_repo/notebooks/extract_table"


def test_git_path_without_extension_unchanged():
    path = GitPath("/Workspace/Users/alice@example.com/my_repo/notebooks/extract_table")

    assert path.get_notebook_path() == "/Workspace/Users/alice@example.com/my_repo/notebooks/extract_table"
