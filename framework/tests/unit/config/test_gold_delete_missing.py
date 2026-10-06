"""Issue #251: gold `delete_missing` option reaches the cdc context, so mode update + nocdc can hard delete."""

import pytest

from tests.unit.config._helpers import _FakeDF
from tests.unit.config.test_cdc_context import _gold_job


@pytest.mark.parametrize(("option", "expected"), [(True, True), (False, None), (None, None)])
def test_gold_delete_missing_option_maps_to_context(option, expected):
    job = _gold_job(mode="update", delete_missing=option)

    context = job.build_cdc_context(_FakeDF(columns=["id"]))

    assert context.get("delete_missing") is expected
