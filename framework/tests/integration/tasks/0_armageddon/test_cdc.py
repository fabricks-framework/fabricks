from typing import Literal

import pytest

from fabricks.cdc import SCD1, SCD2, NoCDC
from fabricks.context import SPARK
from fabricks.context.log import DEFAULT_LOGGER
from fabricks.models.cdc import CdcContext
from tests.integration.compare import assert_dfs_equal

CDC = {"scd1": SCD1, "scd2": SCD2, "nocdc": NoCDC}


@pytest.mark.integration
@pytest.mark.parametrize("topic", ["monarch", "king_and_queen", "prince", "princesses", "duke"])
@pytest.mark.parametrize("cdc", ["scd1", "scd2"])
@pytest.mark.parametrize("iter", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
def test_cdc_isolated(
    topic: Literal["monarch", "king_and_queen", "prince", "princesses", "duke"],
    cdc: Literal["scd1", "scd2", "nocdc"],
    iter: int | list[int] = 1,
    type: Literal["latest", "append"] | None = None,
):
    if type == "latest":
        assert cdc == "nocdc"  # mandatory for latest
        assert topic == "duke"  # mandatory for latest as duke force reload in __operation
    elif type == "append":
        assert cdc == "nocdc"  # mandatory for append

    if isinstance(iter, int):
        iter = [iter]

    if type == "append":
        # append relies on non-cumulative batch tables, so gaps must be filled to not lose data
        iter = list(range(min(iter), max(iter) + 1))

    context = CdcContext(keys=["id"], schema_drift=True)

    if type == "latest":
        context.slice = "latest"
    elif cdc == "scd2":
        context.correct_valid_from = True

    iter_str = [str(i) for i in iter]
    last_iter = iter[-1]
    tgt = CDC[cdc]("test", f"{topic}_{cdc}_{'_'.join(iter_str)}")

    if type == "latest":
        expected = f"select * from expected.silver_latest_job{last_iter}"
    elif type == "append":
        expected = f"select * from input.{topic}_job{last_iter}"
    else:
        expected = f"select * from expected.silver_{cdc}_job{last_iter}"

    DEFAULT_LOGGER.info(f"comparing to {expected}")
    x = 0

    for i in iter_str:
        if topic == "king_and_queen":
            view_1 = f"input.king_job{i}"
            view_2 = f"input.queen_job{i}"

            if type == "append":
                view_1 = view_1 + "_batch"
                view_2 = view_2 + "_batch"

            query = f"select *, 'king' as __source from {view_1} union all select *, 'queen' as __source from {view_2}"
        else:
            view = f"input.{topic}_job{i}"

            if type == "append":
                view = view + "_batch"

            query = f"select * from {view}"

        if x == 0:
            tgt.drop()
            tgt.create_table(query, context=context)

        if type == "append":
            tgt.append(query, context=context)
        else:
            tgt.update(query, context=context)

        x += 1

    expected_df = SPARK.sql(expected).drop("__source")
    df = tgt.table.dataframe.drop("__source")
    assert_dfs_equal(df, expected_df, soft_delete=False)
