from logging import ERROR

import pytest

from fabricks.context import PATH_NOTEBOOKS
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.utils.helpers import run_notebook

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(201)
def test_run_2():
    try:
        run_notebook(PATH_NOTEBOOKS.joinpath("standalone"), schedule="run_2")
        assert True  # schedule should not fail
    except Exception:
        assert False  # schedule should not fail
