"""Execution pack risk score aggregator.

Computes aggregate risk scores for execution packs based on task-level
signals including file overlap, dependency chain depth, scope inflation,
and test command coverage.

Metrics:
- total_tasks, total_packs
- file_overlap_pairs: count of task pairs sharing files
- max_dependency_depth: longest dependency chain
- scope_inflation_count: small tasks touching 5+ files
- tasks_without_test_command: tasks missing test verification
- aggregate_risk_score: weighted composite 0-1
- risk_distribution: dict mapping low/medium/high to task counts
- highest_risk_task: task with contributing factors
- pack_risk_scores: per-pack risk scores
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_execution_pack_risk_score(records: object) -> dict[str, Any]:
    """Compute aggregate risk scores for execution pack task records.

    Args:
        records: List of pack dicts, each containing a 'tasks' list with
            task metadata (title, expectedFiles, testCommand, scope,
            dependsOn).

    Returns:
        Dict with risk metrics.

    Raises:
        ValueError: If records is not a list.
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    all_tasks: list[dict[str, Any]] = []
    packs: list[dict[str, Any]] = []
    pack_risk_scores: dict[str, float] = {}

    for record in records:
        if not isinstance(record, Mapping):
            continue
        tasks = record.get("tasks")
        if not isinstance(tasks, list):
            continue
        pack_id = str(record.get("pack_id") or record.get("id") or f"pack-{len(packs)}")
        pack_tasks: list[dict[str, Any]] = []
        for task in tasks:
            if not isinstance(task, Mapping):
                continue
            pack_tasks.append(dict(task))
            all_tasks.append(dict(task))
        if pack_tasks:
            packs.append({"pack_id": pack_id, "tasks": pack_tasks})

    total_tasks = len(all_tasks)
    total_packs = len(packs)

    if total_tasks == 0:
        return {
            "total_tasks": 0,
            "total_packs": 0,
            "file_overlap_pairs": 0,
            "max_dependency_depth": 0,
            "scope_inflation_count": 0,
            "tasks_without_test_command": 0,
            "aggregate_risk_score": 0.0,
            "risk_distribution": {"low": 0, "medium": 0, "high": 0},
            "highest_risk_task": None,
            "pack_risk_scores": {},
        }

    # Compute per-pack metrics
    total_overlap_pairs = 0
    max_dep_depth = 0
    total_scope_inflation = 0
    total_no_test = 0
    task_risks: list[tuple[str, float, list[str]]] = []

    for pack in packs:
        pack_id = pack["pack_id"]
        pack_tasks = pack["tasks"]

        overlap = _file_overlap_pairs(pack_tasks)
        total_overlap_pairs += overlap

        dep_depth = _max_dependency_depth(pack_tasks)
        max_dep_depth = max(max_dep_depth, dep_depth)

        inflation = _scope_inflation_count(pack_tasks)
        total_scope_inflation += inflation

        no_test = _tasks_without_test_command(pack_tasks)
        total_no_test += no_test

        pack_score, pack_task_risks = _pack_risk_score(
            pack_tasks, overlap, dep_depth, inflation, no_test,
        )
        pack_risk_scores[pack_id] = round(pack_score, 4)
        task_risks.extend(pack_task_risks)

    aggregate = _aggregate_risk(
        total_tasks, total_overlap_pairs, max_dep_depth,
        total_scope_inflation, total_no_test,
    )

    risk_dist = {"low": 0, "medium": 0, "high": 0}
    for _title, risk, _factors in task_risks:
        if risk >= 0.7:
            risk_dist["high"] += 1
        elif risk >= 0.4:
            risk_dist["medium"] += 1
        else:
            risk_dist["low"] += 1

    highest = None
    if task_risks:
        task_risks.sort(key=lambda t: -t[1])
        top = task_risks[0]
        highest = {
            "title": top[0],
            "risk_score": round(top[1], 4),
            "factors": top[2],
        }

    return {
        "total_tasks": total_tasks,
        "total_packs": total_packs,
        "file_overlap_pairs": total_overlap_pairs,
        "max_dependency_depth": max_dep_depth,
        "scope_inflation_count": total_scope_inflation,
        "tasks_without_test_command": total_no_test,
        "aggregate_risk_score": round(aggregate, 4),
        "risk_distribution": risk_dist,
        "highest_risk_task": highest,
        "pack_risk_scores": pack_risk_scores,
    }


def _file_overlap_pairs(tasks: list[dict[str, Any]]) -> int:
    """Count pairs of tasks that share expected files."""
    file_sets: list[set[str]] = []
    for task in tasks:
        files = _get_file_list(task)
        file_sets.append(set(files))

    overlap_count = 0
    for i in range(len(file_sets)):
        for j in range(i + 1, len(file_sets)):
            if file_sets[i] & file_sets[j]:
                overlap_count += 1
    return overlap_count


def _max_dependency_depth(tasks: list[dict[str, Any]]) -> int:
    """Compute the longest dependency chain depth."""
    task_map: dict[str, dict[str, Any]] = {}
    for task in tasks:
        task_id = str(task.get("taskId") or task.get("task_id") or task.get("title") or "")
        if task_id:
            task_map[task_id] = task

    def _depth(task_id: str, visited: set[str]) -> int:
        if task_id in visited or task_id not in task_map:
            return 0
        visited.add(task_id)
        deps = task_map[task_id].get("dependsOn") or []
        if not isinstance(deps, list):
            deps = [deps]
        if not deps:
            return 1
        return 1 + max((_depth(str(d), visited) for d in deps), default=0)

    max_depth = 0
    for task_id in task_map:
        depth = _depth(task_id, set())
        max_depth = max(max_depth, depth)
    return max_depth


def _scope_inflation_count(tasks: list[dict[str, Any]]) -> int:
    """Count small-scope tasks touching 5+ files."""
    count = 0
    for task in tasks:
        scope = str(task.get("scope") or task.get("estimatedScope") or "").lower()
        files = _get_file_list(task)
        if scope == "small" and len(files) >= 5:
            count += 1
    return count


def _tasks_without_test_command(tasks: list[dict[str, Any]]) -> int:
    """Count tasks missing a test command."""
    count = 0
    for task in tasks:
        test_cmd = str(task.get("testCommand") or task.get("test_command") or "").strip()
        if not test_cmd:
            count += 1
    return count


def _get_file_list(task: dict[str, Any]) -> list[str]:
    """Extract file list from task metadata."""
    files = task.get("expectedFiles") or task.get("expected_files") or []
    if isinstance(files, str):
        return [f.strip() for f in files.split(",") if f.strip()]
    if isinstance(files, list):
        return [str(f).strip() for f in files if str(f).strip()]
    return []


def _task_risk_score(
    task: dict[str, Any],
    has_overlap: bool,
    dep_depth: int,
) -> tuple[float, list[str]]:
    """Compute individual task risk score and contributing factors."""
    score = 0.0
    factors: list[str] = []

    # File overlap
    if has_overlap:
        score += 0.25
        factors.append("file_overlap")

    # Dependency depth (depth 1 = no chain, 2+ = chain)
    if dep_depth >= 4:
        score += 0.2
        factors.append("deep_dependency_chain")
    elif dep_depth >= 2:
        score += 0.1
        factors.append("dependency_chain")

    # Scope inflation
    scope = str(task.get("scope") or task.get("estimatedScope") or "").lower()
    files = _get_file_list(task)
    if scope == "small" and len(files) >= 5:
        score += 0.25
        factors.append("scope_inflation")

    # Missing test command
    test_cmd = str(task.get("testCommand") or task.get("test_command") or "").strip()
    if not test_cmd:
        score += 0.2
        factors.append("no_test_command")

    return (min(1.0, score), factors)


def _pack_risk_score(
    tasks: list[dict[str, Any]],
    overlap: int,
    dep_depth: int,
    inflation: int,
    no_test: int,
) -> tuple[float, list[tuple[str, float, list[str]]]]:
    """Compute pack-level risk and per-task risks."""
    if not tasks:
        return (0.0, [])

    # Determine which tasks have overlapping files
    file_sets: dict[str, set[str]] = {}
    for task in tasks:
        title = str(task.get("title") or "untitled")
        file_sets[title] = set(_get_file_list(task))

    overlapping_tasks: set[str] = set()
    titles = list(file_sets.keys())
    for i in range(len(titles)):
        for j in range(i + 1, len(titles)):
            if file_sets[titles[i]] & file_sets[titles[j]]:
                overlapping_tasks.add(titles[i])
                overlapping_tasks.add(titles[j])

    task_risks: list[tuple[str, float, list[str]]] = []
    for task in tasks:
        title = str(task.get("title") or "untitled")
        has_overlap = title in overlapping_tasks
        risk, factors = _task_risk_score(task, has_overlap, dep_depth)
        task_risks.append((title, risk, factors))

    n = len(tasks)
    # Weighted pack score from component signals
    score = 0.0
    if n > 0:
        score += 0.3 * min(1.0, overlap / max(1, n * (n - 1) / 2))
        score += 0.2 * min(1.0, max(0, dep_depth - 1) / 4)
        score += 0.25 * (inflation / n)
        score += 0.25 * (no_test / n)

    return (min(1.0, score), task_risks)


def _aggregate_risk(
    total_tasks: int,
    overlap_pairs: int,
    max_depth: int,
    scope_inflation: int,
    no_test: int,
) -> float:
    """Compute overall aggregate risk score 0-1."""
    if total_tasks == 0:
        return 0.0

    n = total_tasks
    score = 0.0
    score += 0.3 * min(1.0, overlap_pairs / max(1, n * (n - 1) / 2))
    score += 0.2 * min(1.0, max(0, max_depth - 1) / 4)
    score += 0.25 * (scope_inflation / n)
    score += 0.25 * (no_test / n)

    return min(1.0, score)
