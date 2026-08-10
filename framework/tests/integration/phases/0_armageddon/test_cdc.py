import pytest

from tests.integration.helpers.compare import compare_cdc_to_expected

ITERS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


@pytest.mark.parametrize("topic", ["monarch", "king_and_queen", "prince", "princesses", "royal"])
@pytest.mark.parametrize("cdc", ["scd1", "scd2"])
@pytest.mark.parametrize("iter", ITERS)
def test_cdc_scd1_scd2(topic, cdc, iter):
    compare_cdc_to_expected(topic, cdc, iter, "update", soft_delete=False)


@pytest.mark.parametrize("iter", ITERS)
@pytest.mark.parametrize("mode", ["overwrite", "append"])
def test_nocdc_overwrite_append(iter, mode):
    compare_cdc_to_expected("monarch", "nocdc", iter, mode=mode, soft_delete=False)


@pytest.mark.parametrize("iter", ITERS)
def test_nocdc_overwrite_latest(iter):
    compare_cdc_to_expected("royal", "nocdc", iter, mode="latest", soft_delete=False)
