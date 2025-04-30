from logging import ERROR

import pytest

from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_job
from tests.integration.compare import compare_gold_to_expected

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(221)
def test_gold_scd1_complete():
    j = get_job(step="gold", topic="scd1", item="complete")
    compare_gold_to_expected(j, "scd1", 2, where="__is_current")


@pytest.mark.order(222)
def test_gold_scd1_update():
    j = get_job(step="gold", topic="scd1", item="update")
    compare_gold_to_expected(j, "scd1", 2)


@pytest.mark.order(222)
def test_gold_scd1_identity():
    j = get_job(step="gold", topic="scd1", item="identity")
    compare_gold_to_expected(j, "scd1", 2)


@pytest.mark.order(223)
def test_gold_scd2_complete():
    j = get_job(step="gold", topic="scd2", item="complete")
    compare_gold_to_expected(j, "scd2", 2)


@pytest.mark.order(224)
def test_gold_scd2_update():
    j = get_job(step="gold", topic="scd2", item="update")
    compare_gold_to_expected(j, "scd2", 2)


@pytest.mark.order(225)
def test_gold_scd1_last_timestamp():
    max_timestamp = SPARK.sql("select max(`__valid_from`) as __timestamp from expected.gold_scd2_job1").collect()[0][0]
    last_timestamp = SPARK.sql("select * from gold.scd1_last_timestamp__last_timestamp").collect()[0][0]
    assert max_timestamp == last_timestamp, "persisted last timestamp is not max timestamp"
