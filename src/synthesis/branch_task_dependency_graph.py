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
        return [str(item) for item in value if item]
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
