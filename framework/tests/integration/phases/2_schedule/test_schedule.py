from logging import ERROR

import pytest

from fabricks.context import PATH_NOTEBOOKS
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.utils.helpers import run_notebook
from tests.integration.helpers.seed import landing_to_raw

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(201)
def test_run_2():
    landing_to_raw(iter=[2])
    run_notebook(PATH_NOTEBOOKS.joinpath("standalone"), schedule="run_2")
