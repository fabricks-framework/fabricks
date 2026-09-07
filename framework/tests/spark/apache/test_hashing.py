"""fabricks/cdc/templates/macros/hash.sql.jinja's add_key/add_hash macros:
the __key/__hash formula every CDC merge relies on to decide whether a row
changed. Rendered and executed for real via this tier's real Spark session
(md5 evaluation isn't something worth mocking).

The actual risk this guards against: __key/__hash silently changing for a
row whose business content didn't change between two updates, which the
merge then reads as "this row changed" and rewrites/upserts unnecessarily —
a mass update triggered by an incorrect hash, not a real data change. The
macro's own md5() is deterministic by construction; what can actually break
that guarantee is a caller passing a different, differently-ordered, or
differently-cased field list across two calls for what should be the same
row shape (framework/fabricks/cdc/base/processor.py's `get_query_context()`
builds that field list from `self.get_columns(...)` at call time — not
covered here, this file only pins the macro's own contract).
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


def test_add_key_changes_when_a_key_field_value_changes(local_spark):
    macros = _hash_macros()
    sql_expr = macros.add_key(["id", "name"])

    unchanged = _eval_sql(local_spark, sql_expr, "select 1 as id, 'Leopold I' as name")
    changed = _eval_sql(local_spark, sql_expr, "select 1 as id, 'Leopold II' as name")

    assert unchanged != changed


def test_add_key_is_insensitive_to_extra_non_key_columns(local_spark):
    # get_query_context() only ever passes the business-key fields into
    # add_key, but this pins that the macro itself doesn't accidentally
    # depend on anything beyond the field list it's given (e.g. row width
    # or column position) -- a row with unrelated extra columns present
    # must hash identically to one without them, given the same key values.
    macros = _hash_macros()
    sql_expr = macros.add_key(["id", "name"])

    narrow = _eval_sql(local_spark, sql_expr, "select 1 as id, 'Leopold I' as name")
    wide = _eval_sql(
        local_spark, sql_expr, "select 1 as id, 'Leopold I' as name, 0.19 as doubleField, 'king' as __source"
    )

    assert narrow == wide


def test_add_key_field_order_is_significant():
    # Documents, deliberately, that add_key is order-sensitive (array_join
    # over the fields in the order given) -- this is why get_query_context()
    # must build that field list the same way on every call for the same
    # row shape. If this ever changed to be order-independent, every
    # already-materialized __key value in production would change with it.
    macros = _hash_macros()
    assert macros.add_key(["id", "name"]) != macros.add_key(["name", "id"])


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
