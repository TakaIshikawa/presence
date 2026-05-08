"""Branch task dependency graph analyzer for batch planning."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any


def analyze_branch_task_dependency_graph(tasks: object) -> dict[str, Any]:
    """Summarize task dependency edges and dependency violations."""
    if tasks is None:
        tasks = []
    if not isinstance(tasks, list):
        raise ValueError("tasks must be a list of dictionaries")

    titles: list[str] = []
    for index, task in enumerate(tasks):
        title = _title_for(task, index)
        titles.append(title)

    title_counts = Counter(titles)
    duplicate_titles = sorted(title for title, count in title_counts.items() if count > 1)
    known_titles = set(titles)
    edges: list[dict[str, str]] = []
    missing_dependencies: list[dict[str, str]] = []
    self_dependencies: list[dict[str, str]] = []

    for index, task in enumerate(tasks):
        title = titles[index]
        for dependency in _dependencies_for(task):
            if dependency == title:
                self_dependencies.append({"task": title, "dependency": dependency})
            elif dependency not in known_titles:
                missing_dependencies.append({"task": title, "dependency": dependency})
            edges.append({"from": dependency, "to": title})

    adjacency: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        if edge["from"] in known_titles and edge["to"] in known_titles:
            adjacency[edge["from"]].append(edge["to"])

    cycles = _detect_cycles(adjacency)
    dependency_depths = _dependency_depths(titles, edges, known_titles, cycles)
    dependent_titles = {edge["to"] for edge in edges if edge["from"] in known_titles}
    root_titles = [title for title in titles if title not in dependent_titles]

    return {
        "node_count": len(tasks),
        "nodes": titles,
        "edge_count": len(edges),
        "edges": edges,
        "root_task_count": len(root_titles),
        "dependent_task_count": len(dependent_titles),
        "root_tasks": root_titles,
        "duplicate_titles": duplicate_titles,
        "missing_dependencies": missing_dependencies,
        "self_dependencies": self_dependencies,
        "cycles": cycles,
        "dependency_depths": dependency_depths,
        "max_dependency_depth": max(dependency_depths.values()) if dependency_depths else 0,
        "has_violations": bool(duplicate_titles or missing_dependencies or self_dependencies or cycles),
    }


def _title_for(task: object, index: int) -> str:
    if isinstance(task, dict) and task.get("title"):
        return str(task["title"])
    return f"<missing-title-{index}>"


def _dependencies_for(task: object) -> list[str]:
    if not isinstance(task, dict):
        return []
    value = task.get("dependsOn", task.get("depends_on", []))
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        dependencies = [str(item) for item in value if item]
        return list(dict.fromkeys(dependencies))
    return []


def _detect_cycles(adjacency: dict[str, list[str]]) -> list[list[str]]:
    cycles: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()

    def visit(node: str, path: list[str]) -> None:
        if node in path:
            cycle = path[path.index(node):] + [node]
            key = tuple(sorted(cycle[:-1]))
            if key not in seen:
                seen.add(key)
                cycles.append(cycle)
            return
        for child in adjacency.get(node, []):
            visit(child, path + [node])

    for node in sorted(adjacency):
        visit(node, [])
    return cycles


def _dependency_depths(
    titles: list[str],
    edges: list[dict[str, str]],
    known_titles: set[str],
    cycles: list[list[str]],
) -> dict[str, int]:
    cyclic_titles = {title for cycle in cycles for title in cycle[:-1]}
    dependencies: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        parent = edge["from"]
        child = edge["to"]
        if (
            parent in known_titles
            and child in known_titles
            and parent != child
            and parent not in cyclic_titles
            and child not in cyclic_titles
        ):
            dependencies[child].append(parent)

    memo: dict[str, int] = {}

    def depth(title: str) -> int:
        if title in memo:
            return memo[title]
        parents = dependencies.get(title, [])
        memo[title] = 0 if not parents else 1 + max(depth(parent) for parent in parents)
        return memo[title]

    return {title: depth(title) for title in titles}
