from logging import ERROR

import pytest

from fabricks.context import PATH_RUNTIME
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.utils.helpers import run_notebook

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(201)
def test_run():
    run_notebook(PATH_RUNTIME.parent().join("run"), i=2)
    assert True
