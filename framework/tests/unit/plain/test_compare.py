from tests.spark.expected.compare import _expected_scd2_schema, _make_spark_compatible


def test_make_spark_compatible_rewrites_qualify_row_number():
    sql = "select * except (a, b) from src qualify row_number() over (partition by id order by ts) = 1"

    rewritten = _make_spark_compatible(sql)

    assert " qualify " not in rewritten.lower()
    assert "select * except (a, b, __qualify_rn) from (" in rewritten
    assert "row_number() over (partition by id order by ts) as __qualify_rn" in rewritten
    assert "from src" in rewritten
    assert "where __qualify_rn = 1" in rewritten


def test_make_spark_compatible_leaves_other_sql_unchanged():
    sql = "select id, name from expected.scd2_iter1 where __is_current"

    assert _make_spark_compatible(sql) == sql


def test_expected_scd2_schema_omits_new_field_by_default():
    schema = _expected_scd2_schema([{"id": 1, "name": "Leopold I"}])

    assert [field.name for field in schema.fields] == [
        "__valid_from",
        "__valid_to",
        "id",
        "name",
        "doubleField",
        "__is_current",
        "__is_deleted",
        "__source",
    ]


def test_expected_scd2_schema_adds_new_field_when_any_row_has_it():
    rows = [{"id": 1, "name": "Leopold I"}, {"id": 2, "name": "Leopold II", "newField": True}]

    schema = _expected_scd2_schema(rows)

    assert schema.fields[-1].name == "newField"


def test_expected_scd2_schema_handles_empty_rows():
    schema = _expected_scd2_schema([])

    assert "newField" not in [field.name for field in schema.fields]
