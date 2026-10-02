"""#233: a wait_for entry that is also a parent is a config error, whatever the step or the letter case."""

from pydantic import ValidationError
import pytest

from fabricks.models.job import BronzeOptions, GoldOptions, SilverOptions

STEPS = [
    pytest.param(lambda **kw: BronzeOptions(mode="append", uri="abfss://x", **kw), id="bronze"),
    pytest.param(lambda **kw: SilverOptions(mode="append", **kw), id="silver"),
    pytest.param(lambda **kw: GoldOptions(mode="append", **kw), id="gold"),
]


@pytest.mark.parametrize("make", STEPS)
@pytest.mark.parametrize(
    ("parents", "wait_for"),
    [
        (["gold.fact_other"], ["gold.fact_other"]),
        (["Gold.Fact_Other"], ["gold.fact_other"]),
        (["gold.fact_other"], ["GOLD.FACT_OTHER"]),
        (["gold.fact_a", "gold.fact_b"], ["gold.fact_c", "Gold.Fact_B"]),
    ],
    ids=["same", "mixed-case-parent", "mixed-case-wait-for", "one-of-several"],
)
def test_a_wait_for_entry_that_is_a_parent_is_rejected(make, parents, wait_for):
    with pytest.raises(ValidationError, match="wait_for entries are already parents"):
        make(parents=parents, wait_for=wait_for)


@pytest.mark.parametrize("make", STEPS)
def test_a_wait_for_entry_that_is_not_a_parent_is_accepted(make):
    options = make(parents=["gold.fact_other"], wait_for=["gold.fact_third"])

    assert options.wait_for == ["gold.fact_third"]


@pytest.mark.parametrize("make", STEPS)
def test_wait_for_without_parents_is_accepted(make):
    assert make(wait_for=["gold.fact_third"]).wait_for == ["gold.fact_third"]
