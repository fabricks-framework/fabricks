from logging import ERROR

import pytest

from fabricks.context.log import Logger
from fabricks.core import get_job
from tests.integration.compare import compare_gold_to_expected

Logger.setLevel(ERROR)


@pytest.mark.order(221)
def test_job2_gold_scd1_complete():
    j = get_job(step="gold", topic="scd1", item="complete")
    compare_gold_to_expected(j, "scd1", 2, where="__is_current")


@pytest.mark.order(222)
def test_job2_gold_scd1_update():
    j = get_job(step="gold", topic="scd1", item="update")
    compare_gold_to_expected(j, "scd1", 2)


@pytest.mark.order(223)
def test_job2_gold_scd2_complete():
    j = get_job(step="gold", topic="scd2", item="complete")
    compare_gold_to_expected(j, "scd2", 2)


@pytest.mark.order(224)
def test_job2_gold_scd2_update():
    j = get_job(step="gold", topic="scd2", item="update")
    compare_gold_to_expected(j, "scd2", 2)
