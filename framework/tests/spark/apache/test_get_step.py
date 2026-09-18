"""Proves get_step() resolves real YAML step config -- the step-level
counterpart to test_get_job.py's job-level proof.
"""

from fabricks.core import get_step


def test_get_step_resolves_bronze_and_silver():
    bronze = get_step(step="bronze")
    silver = get_step(step="silver")

    assert bronze.name == "bronze"
    assert silver.name == "silver"
