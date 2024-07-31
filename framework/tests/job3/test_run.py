from logging import ERROR

import pytest

from fabricks.context import PATH_RUNTIME
from fabricks.context.log import Logger
from fabricks.utils.helpers import run_notebook

Logger.setLevel(ERROR)


@pytest.mark.order(301)
def test_job3_run():
    for i in range(3, 12):
        run_notebook(PATH_RUNTIME.parent().join("run"), i=i)
    assert True
