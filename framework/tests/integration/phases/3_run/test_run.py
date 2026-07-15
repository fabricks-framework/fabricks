from logging import ERROR

import pytest

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.utils.helpers import run_notebook
from tests.integration.helpers.const import ROOT

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(301)
def test_run():
    for i in range(3, 12):
        run_notebook(ROOT.joinpath("run"), i=i)

    assert True
