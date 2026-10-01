"""The `add_key`/`add_hash` macros in cdc/templates/macros/hash.sql.jinja: the __key/__hash formula every
CDC merge uses to decide a row changed. Executed on real Spark. Guards against __key/__hash changing
for a row whose business content did not change (a mass rewrite from a wrong hash). Only the macro's
own contract is pinned here, not the field list `get_query_context()` builds at call time.
"""

from jinja2 import Environment, PackageLoader

from fabricks.utils.sqlglot import fix


def _hash_macros():
    env = Environment(loader=PackageLoader("fabricks.cdc", "templates"))
    return env.get_template("macros/hash.sql.jinja").make_module()


def _eval_sql(local_spark, sql_expr: str, row_sql: str) -> str:
    # add_key/add_hash render a trailing comma inside array(...) (see the
    # macro's own {% for %} loop) -- harmless in production because
    # get_data() always runs generated SQL through fix() (processor.py's
    # `sql = self.get_query(src, fix=True, **kwargs)`) before executing it.
    # Doing the same here, rather than skipping it, means this test exercises
    # the exact SQL shape a real merge would actually run.
    sql = fix(f"select {sql_expr} as value from ({row_sql}) t")
    return local_spark.sql(sql).collect()[0][0]


def test_add_key_is_stable_for_identical_field_values(local_spark):
    macros = _hash_macros()
    sql_expr = macros.add_key(["id", "name"])
    row_sql = "select 1 as id, 'Leopold I' as name"

    first = _eval_sql(local_spark, sql_expr, row_sql)
    second = _eval_sql(local_spark, sql_expr, row_sql)

    assert first == second, "same key fields, same values must hash identically across calls"
    assert first == "8321ca3a6e9d2da20fa8120e7dfbce25"


def test_add_key_changes_when_a_key_field_value_changes(local_spark):
    macros = _hash_macros()
    sql_expr = macros.add_key(["id", "name"])

    unchanged = _eval_sql(local_spark, sql_expr, "select 1 as id, 'Leopold I' as name")
    changed = _eval_sql(local_spark, sql_expr, "select 1 as id, 'Leopold II' as name")

    assert unchanged != changed


def test_add_hash_treats_reload_and_upsert_as_the_same_operation(local_spark):
    # hash.sql.jinja's own add_hash comment: reloads and upserts should
    # have the same hash, not deletes -- __operation is folded to a
    # delete/not-delete boolean rather than compared literally.
    macros = _hash_macros()
    sql_expr = macros.add_hash(["name", "__operation"])

    reload_hash = _eval_sql(local_spark, sql_expr, "select 'Leopold I' as name, 'reload' as __operation")
    upsert_hash = _eval_sql(local_spark, sql_expr, "select 'Leopold I' as name, 'upsert' as __operation")
    delete_hash = _eval_sql(local_spark, sql_expr, "select 'Leopold I' as name, 'delete' as __operation")

    assert reload_hash == upsert_hash
    assert delete_hash != reload_hash
