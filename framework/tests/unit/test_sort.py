"""Unit tests for topological sorting utilities.

Note: These are marked as integration tests since they require a Spark session.
"""

import pytest

from fabricks.utils.sort import CircularDependency
from fabricks.utils.sort import topological as topological_sort


def test_topological_sort_pure_python():
    """Test pure Python topological sort without Spark."""
    nodes = ["job_a", "job_b", "job_c"]
    deps = [("job_b", "job_a"), ("job_c", "job_b")]

    sorted_ids = topological_sort(nodes, deps)

    assert sorted_ids == ["job_a", "job_b", "job_c"]


def test_topological_sort_diamond():
    """Test diamond dependency pattern."""
    nodes = ["job_a", "job_b", "job_c", "job_d"]
    deps = [
        ("job_b", "job_a"),
        ("job_c", "job_a"),
        ("job_d", "job_b"),
        ("job_d", "job_c"),
    ]

    sorted_ids = topological_sort(nodes, deps)

    # A must come first, D must come last
    assert sorted_ids[0] == "job_a"
    assert sorted_ids[-1] == "job_d"
    # B and C can be in any order, but must be between A and D
    assert set(sorted_ids[1:3]) == {"job_b", "job_c"}


def test_topological_sort_no_dependencies():
    """Test nodes with no dependencies."""
    nodes = ["job_a", "job_b", "job_c"]
    deps = []

    sorted_ids = topological_sort(nodes, deps)

    # All nodes independent, any order is valid
    assert set(sorted_ids) == {"job_a", "job_b", "job_c"}
    assert len(sorted_ids) == 3


def test_topological_sort_circular_dependency():
    """Test that circular dependencies are detected."""
    nodes = ["job_a", "job_b", "job_c"]
    deps = [("job_a", "job_b"), ("job_b", "job_c"), ("job_c", "job_a")]

    with pytest.raises(CircularDependency) as exc_info:
        topological_sort(nodes, deps)

    assert "Circular dependency detected" in str(exc_info.value)
