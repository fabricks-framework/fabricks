"""Split out from tests/tier_policy.py (which re-exports it) so
tests/spark/databricks/conftest.py can import it as a sibling module --
see the comment in registered_delta_rows.py for why.
"""

import os
from typing import Literal

import pytest

Tier = Literal["plain", "config", "apache", "databricks"]
_ACTIVE_TIER = "FABRICKS_ACTIVE_TEST_TIER"


def activate_tier(tier: Tier) -> None:
    active = os.environ.get(_ACTIVE_TIER)
    if active and active != tier:
        raise pytest.UsageError(
            f"cannot mix {active} and {tier} test tiers; run each tier with its separate just command"
        )
    os.environ[_ACTIVE_TIER] = tier
