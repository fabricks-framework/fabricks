from graphlib import CycleError, TopologicalSorter
from typing import Any, Dict, List, Tuple


class CircularDependency(Exception):
    """Raised when a circular dependency is detected in the graph."""


def topological(nodes: List[str], dependencies: List[Tuple[str, str]]) -> List[str]:
    """
    Sort nodes by their dependencies.

    Args:
        nodes: List of node identifiers
        dependencies: List of (child_id, parent_id) tuples representing edges

    Returns:
        List of node identifiers sorted in topological order (dependencies first)

    Raises:
        CircularDependency: If circular dependencies are detected

    Example:
        >>> nodes = ["job_a", "job_b", "job_c"]
        >>> deps = [("job_b", "job_a"), ("job_c", "job_b")]
        >>> topological(nodes, deps)
        ['job_a', 'job_b', 'job_c']
    """
    ts = TopologicalSorter()
    for node in nodes:
        ts.add(node)
    for child_id, parent_id in dependencies:
        ts.add(child_id, parent_id)
    try:
        return list(ts.static_order())

    except CycleError as e:
        cycle = e.args[1]
        raise CircularDependency(
            f"Circular dependency detected. Nodes involved: {', '.join(str(n) for n in cycle)}"
        ) from e


def topological_with_data(
    items: List[Tuple[str, Any]],
    dependencies: List[Tuple[str, str]],
) -> List[Tuple[str, Any]]:
    """
    Sort items by their dependencies, preserving associated data.

    Args:
        items: List of (id, data) tuples
        dependencies: List of (child_id, parent_id) tuples representing edges

    Returns:
        List of (id, data) tuples sorted in topological order

    Raises:
        CircularDependency: If circular dependencies are detected

    Example:
        >>> items = [("a", {"name": "Job A"}), ("b", {"name": "Job B"})]
        >>> deps = [("b", "a")]
        >>> topological_with_data(items, deps)
        [('a', {'name': 'Job A'}), ('b', {'name': 'Job B'})]
    """
    item_map: Dict[str, Any] = dict(items)
    sorted_ids = topological(list(item_map.keys()), dependencies)
    return [(item_id, item_map[item_id]) for item_id in sorted_ids]
