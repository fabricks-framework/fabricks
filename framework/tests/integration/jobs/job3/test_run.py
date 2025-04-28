from logging import ERROR

import pytest

from fabricks.context import PATH_RUNTIME
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.utils.helpers import run_notebook

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(301)
def test_run():
    for i in range(3, 12):
        run_notebook(PATH_RUNTIME.parent().join("run"), i=i)
    assert True
