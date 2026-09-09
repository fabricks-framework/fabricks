from typing import Literal

from tests.spark.test_data import load_entity_frames

_CACHE: dict[tuple[int, int, tuple[int, ...], str], object] = {}
_PREPARED_SPARKS: set[int] = set()


def validate_scenario(seed_from: int, iters: list[int], compare_to: int | None = None) -> None:
    if not iters:
        raise ValueError("iters must not be empty")
    if iters != list(range(seed_from + 1, iters[-1] + 1)):
        raise ValueError("iters must be the contiguous range right after seed_from")
    if compare_to is not None and iters[-1] != compare_to:
        raise ValueError("compare_to must be iters' own last element")


def run_cdc_scenario(spark, seed_from: int, iters: list[int], cdc: Literal["scd1", "scd2"]):
    validate_scenario(seed_from, iters)
    cache_key = (id(spark), seed_from, tuple(iters), cdc)
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    from fabricks.cdc.scd1 import SCD1
    from fabricks.cdc.scd2 import SCD2
    from tests.spark.expected.compare import create_expected_views

    if id(spark) not in _PREPARED_SPARKS:
        create_expected_views(spark, "scd2")
        create_expected_views(spark, "scd1")
        _PREPARED_SPARKS.add(id(spark))

    suffix = f"king_and_queen_seed{seed_from}_{iters[0]}to{iters[-1]}_{cdc}"
    scd = SCD2("cdc", suffix, "scd2", spark=spark) if cdc == "scd2" else SCD1(
        "cdc", suffix, "scd1", spark=spark
    )

    if seed_from:
        expected = _load_scd2_seed(spark, seed_from) if cdc == "scd2" else _load_scd1_seed(spark, seed_from)
        _seed_table(scd.table, expected, keys=["id", "__source"])

    for iter_num in iters:
        frames = load_entity_frames(spark, iter_num)
        combined = frames[0]
        for frame in frames[1:]:
            combined = combined.unionByName(frame, allowMissingColumns=True)

        options = {"keys": "id", "add_key": True, "soft_delete": True}
        if cdc == "scd2":
            options["correct_valid_from"] = True
        if scd.table.exists() and set(combined.columns) - set(scd.table.columns):
            scd.update_schema(combined, **options)
        scd.update(combined, **options)

    _CACHE[cache_key] = scd
    return scd


def _load_scd2_seed(spark, iteration: int):
    from tests.spark.expected.compare import load_expected

    return load_expected(spark, "scd2", iteration).selectExpr("*", "__valid_from as __timestamp")


def _load_scd1_seed(spark, iteration: int):
    scd2 = spark.read.table(f"expected.scd2_iter{iteration}")
    scd2.createOrReplaceTempView(f"__seed_scd2_src_{iteration}")
    return spark.sql(f"""
        select * except (__valid_from, __valid_to, __rn), __valid_from as __timestamp
        from (
            select *, row_number() over (partition by id order by __valid_to desc) as __rn
            from __seed_scd2_src_{iteration}
        )
        where __rn = 1
    """)


def _seed_table(table, expected, keys: list[str]) -> None:
    key_columns = ", ".join(f"cast(`{key}` as string)" for key in keys)
    seeded = expected.selectExpr(
        "*", f"md5(array_join(array({key_columns}), '*', '-1')) as __key", "'current' as __operation"
    )
    seeded.write.format("delta").mode("overwrite").save(str(table.delta_path))
    table.register()
