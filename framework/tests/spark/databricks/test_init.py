"""Forces collection order (pytest-order) so the session-scoped
_schedule_run fixture's real wait shows up here, not on whichever test
happens to be collected first alphabetically (test_dependencies.py's
test_notebook_derived_gold_dependency_is_persisted) -- that test doing
nothing slow itself would otherwise look like the slow one.
"""

import pytest


@pytest.mark.order("first")
def test_init():
    pass
