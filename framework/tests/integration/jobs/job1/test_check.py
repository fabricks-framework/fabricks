from logging import ERROR

import pytest

from fabricks.context.log import DEFAULT_LOGGER
from fabricks.core import get_job
from tests.integration.compare import get_last_error

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(161)
def test_gold_check_fail():
    j = get_job(step="gold", topic="check", item="fail")
    error = get_last_error(j.job_id)

    assert error == "Please don't fail on me :("
    assert j.table.rows == 0, "table should be empty"


@pytest.mark.order(162)
def test_gold_check_warning():
    j = get_job(step="gold", topic="check", item="warning")
    error = get_last_error(j.job_id, status="warned")

    assert error == "I want you to warn me !"
    assert j.table.rows > 0, "table should not be empty"


@pytest.mark.order(163)
def test_gold_check_max_rows():
    j = get_job(step="gold", topic="check", item="max_rows")
    error = get_last_error(j.job_id)

    assert error == "max rows check failed (3 > 2)"
    assert j.table.rows == 0, "table should be empty"


@pytest.mark.order(164)
def test_gold_check_min_rows():
    j = get_job(step="gold", topic="check", item="min_rows")
    error = get_last_error(j.job_id)

    assert error == "min rows check failed (1 < 2)"
    assert j.table.rows == 0, "table should be empty"


@pytest.mark.order(165)
def test_gold_check_count_must_equal():
    j = get_job(step="gold", topic="check", item="count_must_equal")
    error = get_last_error(j.job_id)

    assert error == "count must equal check failed (fabricks.dummy - 2 != 1)"
    assert j.table.rows == 0, "table should be empty"


@pytest.mark.order(166)
def test_gold_check_skip():
    j = get_job(step="gold", topic="check", item="skip")
    error = get_last_error(j.job_id, status="skipped")

    assert error == "I want you to skip this !"
    assert j.table.rows == 0, "table should be empty"


# @pytest.mark.order(166)
# def test_gold_check_no_dependency_fail():
#     j = get_job(step="gold", topic="check", item="no_dependency_fail")
#     error = get_last_error(j.job_id)
#     assert error == "no dependency fail check failed (gold.check_fail)"
#     assert j.table.rows == 0, "table should be empty"


# @pytest.mark.order(167)
# def test_gold_check_duplicate_key():
#     j = get_job(step="gold", topic="check", item="duplicate_key")
#     error = get_last_error(j.job_id)
#     assert error == "duplicate key"


# @pytest.mark.order(168)
# def test_gold_check_duplicate_identity():
#     j = get_job(step="gold", topic="check", item="duplicate_identity")
#     error = get_last_error(j.job_id)
#     assert error == "duplicate identity"
