"""Pins the known limitation from https://github.com/fabricks-framework/fabricks/issues/235: `add_key` joins the key
fields with '*' and replaces nulls with '-1', so distinct business keys can share a __key. Fixing it would change every
materialized __key/__hash, so these collisions are expected until a versioned key format exists.
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
def test_add_key_collides_on_delimiter_and_null_known_limitation(local_spark, left, right):
    # Flip to != only together with a key-format migration; a silent change would rewrite every stored __key.
    sql_expr = _hash_macros().add_key(["id", "name"])

    assert _eval_sql(local_spark, sql_expr, left) == _eval_sql(local_spark, sql_expr, right)


def test_add_key_distinguishes_ordinary_distinct_keys_control(local_spark):
    sql_expr = _hash_macros().add_key(["id", "name"])

    first = _eval_sql(local_spark, sql_expr, "select 'a' as id, 'c' as name")
    second = _eval_sql(local_spark, sql_expr, "select 'b' as id, 'c' as name")

    assert first != second
