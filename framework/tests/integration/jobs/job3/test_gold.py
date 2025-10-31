from logging import ERROR

import pytest

from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_job
from tests.integration.compare import assert_dfs_equal, compare_gold_to_expected

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(321)
def test_gold_scd1_complete():
    j = get_job(step="gold", topic="scd1", item="complete")
    compare_gold_to_expected(j, "scd1", 11, where="__is_current")


@pytest.mark.order(322)
def test_gold_scd1_update():
    j = get_job(step="gold", topic="scd1", item="update")
    compare_gold_to_expected(j, "scd1", 11)


@pytest.mark.order(322)
def test_gold_scd1_identity():
    j = get_job(step="gold", topic="scd1", item="identity")
    compare_gold_to_expected(j, "scd1", 11)


@pytest.mark.order(323)
def test_gold_scd1_memory():
    j = get_job(step="gold", topic="scd1", item="memory")
    compare_gold_to_expected(j, "scd1", 11)


@pytest.mark.order(324)
def test_gold_scd2_complete():
    j = get_job(step="gold", topic="scd2", item="complete")
    compare_gold_to_expected(j, "scd2", 11)


@pytest.mark.order(325)
def test_gold_scd2_update():
    j = get_job(step="gold", topic="scd2", item="update")
    compare_gold_to_expected(j, "scd2", 11)


@pytest.mark.order(326)
def test_gold_scd2_memory():
    j = get_job(step="gold", topic="scd2", item="memory")
    compare_gold_to_expected(j, "scd2", 11)


@pytest.mark.order(327)
def test_gold_nocdc_update():
    j = get_job(step="gold", topic="nocdc", item="update")
    j.run()

    df_a = SPARK.sql(
        """
        select
          `id`,
          valid_from,
          valid_to,
          `value`,
          monarch
        from
          gold.nocdc_update
        """
    )

    df_b = SPARK.sql(
        """
        select
          `id`,
          `__valid_from` as valid_from,
          `__valid_to` as valid_to,
          `value`,
          monarch
        from
          expected.gold_scd2_job11
        """
    )

    assert_dfs_equal(df_a, df_b)
