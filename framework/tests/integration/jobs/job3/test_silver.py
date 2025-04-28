import os
import sys
from logging import ERROR
from pathlib import Path

import pytest

p = Path(os.getcwd())
while not (p / "pyproject.toml").exists():
    p = p.parent

root = p.absolute()

if str(root) not in sys.path:
    sys.path.append(str(root))


from fabricks.context.log import DEFAULT_LOGGER  # noqa: E402
from fabricks.core import get_job  # noqa: E402
from tests.integration.compare import compare_silver_to_expected  # noqa: E402

DEFAULT_LOGGER.setLevel(ERROR)


@pytest.mark.order(311)
def test_silver_monarch_scd2():
    job = get_job(step="silver", topic="monarch", item="scd2")
    compare_silver_to_expected(job=job, cdc="scd2", iter=11)

    job.truncate()
    job.run()
    compare_silver_to_expected(job=job, cdc="scd2", iter=11)


@pytest.mark.order(312)
def test_silver_monarch_scd1():
    job = get_job(step="silver", topic="monarch", item="scd1")
    compare_silver_to_expected(job=job, cdc="scd1", iter=11)

    job.truncate()
    job.run()
    compare_silver_to_expected(job=job, cdc="scd1", iter=11)


@pytest.mark.order(313)
def test_silver_regent_scd2():
    job = get_job(step="silver", topic="regent", item="scd2")
    compare_silver_to_expected(job=job, cdc="scd2", iter=11)


@pytest.mark.order(314)
def test_silver_regent_scd1():
    job = get_job(step="silver", topic="regent", item="scd1")
    compare_silver_to_expected(job=job, cdc="scd1", iter=11)


@pytest.mark.order(313)
def test_silver_memory_scd2():
    job = get_job(step="silver", topic="memory", item="scd2")
    compare_silver_to_expected(job=job, cdc="scd2", iter=11)


@pytest.mark.order(314)
def test_silver_memory_scd1():
    job = get_job(step="silver", topic="memory", item="scd1")
    compare_silver_to_expected(job=job, cdc="scd1", iter=11)


@pytest.mark.order(315)
def test_silver_king_and_queen_scd2():
    job = get_job(step="silver", topic="king_and_queen", item="scd2")
    compare_silver_to_expected(job=job, cdc="scd2", iter=11)

    job.truncate()
    job.run()
    compare_silver_to_expected(job=job, cdc="scd2", iter=11)


@pytest.mark.order(316)
def test_silver_king_and_queen_scd1():
    job = get_job(step="silver", topic="king_and_queen", item="scd1")
    compare_silver_to_expected(job=job, cdc="scd1", iter=11)

    job.truncate()
    job.run()
    compare_silver_to_expected(job=job, cdc="scd1", iter=11)
