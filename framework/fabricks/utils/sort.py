from collections import defaultdict, deque
from typing import Any, Dict, List, Set, Tuple


class CircularDependency(Exception):
    """Raised when a circular dependency is detected in the graph."""


def topological(nodes: List[str], dependencies: List[Tuple[str, str]]) -> List[str]:
    """
    Sort nodes by their dependencies using Kahn's Algorithm.

    Pure Python implementation with no external dependencies.

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
    # Build adjacency list and in-degree counts
    graph: Dict[str, List[str]] = defaultdict(list)  # parent_id -> [child_ids]
    in_degree: Dict[str, int] = defaultdict(int)

    # Initialize all nodes with in-degree 0
    for node in nodes:
        if node not in in_degree:
            in_degree[node] = 0

    # Build graph from dependency edges
    for child_id, parent_id in dependencies:
        graph[parent_id].append(child_id)
        in_degree[child_id] += 1

    # Kahn's Algorithm
    # Start with all nodes that have no dependencies
    queue: deque[str] = deque()

    for node in nodes:
        if in_degree[node] == 0:
            queue.append(node)

    sorted_nodes: List[str] = []
    processed: Set[str] = set()

    while queue:
        current_id = queue.popleft()
        sorted_nodes.append(current_id)
        processed.add(current_id)

        # Reduce in-degree for all children
        for child_id in graph[current_id]:
            in_degree[child_id] -= 1
            # If in-degree becomes 0, add to queue
            if in_degree[child_id] == 0 and child_id not in processed:
                queue.append(child_id)

    # Check for cycles
    if len(sorted_nodes) != len(nodes):
        # Find the nodes that are part of the cycle
        unprocessed = [node for node in nodes if node not in processed]
        raise CircularDependency(f"Circular dependency detected. Nodes involved: {', '.join(unprocessed)}")

    return sorted_nodes


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
    # Extract nodes and build item map
    item_map: Dict[str, Any] = {item_id: data for item_id, data in items}
    nodes = list(item_map.keys())
    # Get sorted order
    sorted_ids = topological(nodes, dependencies)
    # Return items in sorted order
    return [(item_id, item_map[item_id]) for item_id in sorted_ids]
