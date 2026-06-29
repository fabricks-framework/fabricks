from logging import ERROR

import pytest

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.utils.helpers import run_notebook
from tests.integration._types import paths

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(201)
def test_run():
    try:
        run_notebook(paths.tests.joinpath("run"), i=2)
        assert False
    except Exception:
        assert True  # notebook should fail
