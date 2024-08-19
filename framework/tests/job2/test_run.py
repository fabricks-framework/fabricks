from logging import ERROR

import pytest

from framework.fabricks.context import PATH_RUNTIME
from framework.fabricks.context.log import Logger
from framework.fabricks.utils.helpers import run_notebook

Logger.setLevel(ERROR)


@pytest.mark.order(201)
def test_job2_run():
    run_notebook(PATH_RUNTIME.parent().join("run"), i=2)
    assert True
