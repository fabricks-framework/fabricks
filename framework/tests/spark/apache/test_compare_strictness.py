"""The oracle comparator must fail on what the oracle files cannot express: stray columns, type drift and a
`__key` that stops mapping one-to-one onto `id`."""

import pytest

from tests.spark.expected.compare import assert_dfs_equal, assert_keys_match_business_key


def _frame(spark, sql: str):
    return spark.sql(sql)


def test_assert_dfs_equal_passes_for_identical_rows_ignoring_system_columns(local_spark):
    actual = _frame(local_spark, "select 1 as id, 'a' as name, 'k1' as __key, 'h1' as __hash")
    expected = _frame(local_spark, "select 1 as id, 'a' as name")

    assert_dfs_equal(actual, expected)


def test_assert_dfs_equal_fails_on_a_column_the_oracle_does_not_have(local_spark):
    actual = _frame(local_spark, "select 1 as id, 'a' as name, 'leak' as stray")
    expected = _frame(local_spark, "select 1 as id, 'a' as name")

    with pytest.raises(AssertionError, match=r"columns the oracle does not: \['stray'\]"):
        assert_dfs_equal(actual, expected)


def test_assert_dfs_equal_fails_on_a_type_drift(local_spark):
    actual = _frame(local_spark, "select 1 as id, cast(2 as double) as value")
    expected = _frame(local_spark, "select 1 as id, cast(2 as int) as value")

    with pytest.raises(AssertionError, match="column types differ"):
        assert_dfs_equal(actual, expected)


def test_assert_dfs_equal_fails_on_a_value_difference(local_spark):
    actual = _frame(local_spark, "select 1 as id, 'a' as name")
    expected = _frame(local_spark, "select 1 as id, 'b' as name")

    with pytest.raises(AssertionError):
        assert_dfs_equal(actual, expected)


def test_assert_keys_match_business_key_passes_for_a_one_to_one_key(local_spark):
    assert_keys_match_business_key(_frame(local_spark, "select 1 as id, 'k1' as __key union all select 2, 'k2'"))


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        pytest.param("select 1 as id, cast(null as string) as __key", "null __key", id="null-key"),
        pytest.param("select 1 as id, 'k' as __key union all select 2, 'k'", "not one-to-one", id="shared-key"),
        pytest.param("select 1 as id, 'k1' as __key union all select 1, 'k2'", "not one-to-one", id="split-key"),
    ],
)
def test_assert_keys_match_business_key_fails(local_spark, sql, message):
    with pytest.raises(AssertionError, match=message):
        assert_keys_match_business_key(_frame(local_spark, sql))
