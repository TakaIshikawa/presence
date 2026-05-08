"""Tests for branch task dependency graph analysis."""

from synthesis.branch_task_dependency_graph import analyze_branch_task_dependency_graph


def test_independent_tasks_are_roots():
    report = analyze_branch_task_dependency_graph([{"title": "A"}, {"title": "B"}])

    assert report["node_count"] == 2
    assert report["edge_count"] == 0
    assert report["root_task_count"] == 2
    assert report["dependent_task_count"] == 0
    assert not report["has_violations"]


def test_valid_dependency_chain_reports_edges():
    report = analyze_branch_task_dependency_graph(
        [
            {"title": "A"},
            {"title": "B", "dependsOn": ["A"]},
            {"title": "C", "dependsOn": ["B"]},
        ]
    )

    assert report["edge_count"] == 2
    assert {"from": "A", "to": "B"} in report["edges"]
    assert {"from": "B", "to": "C"} in report["edges"]
    assert report["root_task_count"] == 1
    assert report["dependent_task_count"] == 2


def test_missing_dependencies_are_reported():
    report = analyze_branch_task_dependency_graph([{"title": "A", "dependsOn": ["Missing"]}])

    assert report["missing_dependencies"] == [{"task": "A", "dependency": "Missing"}]
    assert report["has_violations"]


def test_duplicate_titles_are_reported():
    report = analyze_branch_task_dependency_graph([{"title": "A"}, {"title": "A"}])

    assert report["duplicate_titles"] == ["A"]
    assert report["has_violations"]


def test_self_dependencies_are_reported():
    report = analyze_branch_task_dependency_graph([{"title": "A", "dependsOn": "A"}])

    assert report["self_dependencies"] == [{"task": "A", "dependency": "A"}]
    assert report["has_violations"]


def test_simple_cycle_is_detected():
    report = analyze_branch_task_dependency_graph(
        [{"title": "A", "dependsOn": ["B"]}, {"title": "B", "dependsOn": ["A"]}]
    )

    assert report["cycles"]
    assert set(report["cycles"][0][:-1]) == {"A", "B"}
    assert report["has_violations"]
