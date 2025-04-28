import os
import sys
from logging import ERROR
from pathlib import Path

import pytest

p = Path(os.getcwd())
while not (p / "pyproject.toml").exists():
    p = p.parent

root = p.absolute()

if str(root) not in sys.path:
    sys.path.append(str(root))


from fabricks.context import PATH_RUNTIME  # noqa: E402
from fabricks.context.log import DEFAULT_LOGGER  # noqa: E402
from fabricks.utils.helpers import run_notebook  # noqa: E402

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(301)
def test_run():
    for i in range(3, 12):
        run_notebook(PATH_RUNTIME.parent().join("run"), i=i)
    assert True
