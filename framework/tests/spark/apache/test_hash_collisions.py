"""Reproduces https://github.com/fabricks-framework/fabricks/issues/235: `add_key` joins the key fields with '*'
and replaces nulls with '-1', so distinct business keys can share a __key. Encodes the correct behavior.
"""

import pytest

from tests.spark.apache.test_hashing import _eval_sql, _hash_macros


@pytest.mark.parametrize(
    ("left", "right"),
    [
        pytest.param(
            "select 'a*b' as id, 'c' as name", "select 'a' as id, 'b*c' as name", id="delimiter-inside-value"
        ),
        pytest.param(
            "select cast(null as string) as id, 'c' as name",
            "select '-1' as id, 'c' as name",
            id="null-vs-literal-minus-one",
        ),
    ],
)
@pytest.mark.xfail(strict=True, reason="add_key joins with '*' and maps null to '-1' (#235)")
def test_add_key_distinguishes_distinct_business_keys(local_spark, left, right):
    sql_expr = _hash_macros().add_key(["id", "name"])

    assert _eval_sql(local_spark, sql_expr, left) != _eval_sql(local_spark, sql_expr, right)


def test_add_key_distinguishes_ordinary_distinct_keys_control(local_spark):
    sql_expr = _hash_macros().add_key(["id", "name"])

    first = _eval_sql(local_spark, sql_expr, "select 'a' as id, 'c' as name")
    second = _eval_sql(local_spark, sql_expr, "select 'b' as id, 'c' as name")

    assert first != second
