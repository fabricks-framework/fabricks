from logging import ERROR

import pytest

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.utils.helpers import run_notebook
from tests.integration._types import paths

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(301)
def test_run():
    for i in range(3, 12):
        run_notebook(paths.tests.joinpath("run"), i=i)

    assert True
