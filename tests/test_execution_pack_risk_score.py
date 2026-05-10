"""Tests for execution pack risk score aggregator."""

from __future__ import annotations

from synthesis.execution_pack_risk_score import analyze_execution_pack_risk_score


def _pack(pack_id: str, tasks: list[dict]) -> dict:
    return {"pack_id": pack_id, "tasks": tasks}


def _task(
    title: str,
    *,
    expected_files: list[str] | None = None,
    test_command: str = "",
    scope: str = "small",
    depends_on: list[str] | None = None,
    task_id: str | None = None,
) -> dict:
    t = {
        "title": title,
        "taskId": task_id or title,
        "expectedFiles": expected_files or [],
        "testCommand": test_command,
        "scope": scope,
    }
    if depends_on is not None:
        t["dependsOn"] = depends_on
    return t


def test_empty_records_returns_zeros():
    result = analyze_execution_pack_risk_score([])
    assert result["total_tasks"] == 0
    assert result["total_packs"] == 0
    assert result["aggregate_risk_score"] == 0.0
    assert result["risk_distribution"] == {"low": 0, "medium": 0, "high": 0}
    assert result["highest_risk_task"] is None


def test_none_records_returns_zeros():
    result = analyze_execution_pack_risk_score(None)
    assert result["total_tasks"] == 0


def test_invalid_records_raises():
    try:
        analyze_execution_pack_risk_score("not a list")
        assert False, "Should have raised ValueError"
    except ValueError:
        pass


def test_single_task_no_risk_signals():
    records = [_pack("p1", [
        _task("Task A", expected_files=["a.py"], test_command="pytest", scope="small"),
    ])]
    result = analyze_execution_pack_risk_score(records)

    assert result["total_tasks"] == 1
    assert result["total_packs"] == 1
    assert result["file_overlap_pairs"] == 0
    assert result["scope_inflation_count"] == 0
    assert result["tasks_without_test_command"] == 0
    assert result["aggregate_risk_score"] == 0.0


def test_file_overlap_detected():
    records = [_pack("p1", [
        _task("Task A", expected_files=["shared.py", "a.py"]),
        _task("Task B", expected_files=["shared.py", "b.py"]),
    ])]
    result = analyze_execution_pack_risk_score(records)

    assert result["file_overlap_pairs"] == 1
    assert result["aggregate_risk_score"] > 0


def test_dependency_chain_depth():
    records = [_pack("p1", [
        _task("Base", task_id="base"),
        _task("Middle", task_id="middle", depends_on=["base"]),
        _task("Top", task_id="top", depends_on=["middle"]),
    ])]
    result = analyze_execution_pack_risk_score(records)

    assert result["max_dependency_depth"] == 3


def test_scope_inflation_flagged():
    records = [_pack("p1", [
        _task(
            "Big small task",
            expected_files=["a.py", "b.py", "c.py", "d.py", "e.py"],
            scope="small",
        ),
    ])]
    result = analyze_execution_pack_risk_score(records)

    assert result["scope_inflation_count"] == 1
    assert result["aggregate_risk_score"] > 0


def test_missing_test_command_detected():
    records = [_pack("p1", [
        _task("No tests", test_command=""),
        _task("Has tests", test_command="pytest tests/"),
    ])]
    result = analyze_execution_pack_risk_score(records)

    assert result["tasks_without_test_command"] == 1


def test_aggregate_risk_between_zero_and_one():
    records = [_pack("p1", [
        _task("A", expected_files=["x.py"], scope="small"),
        _task("B", expected_files=["x.py", "y.py", "z.py", "w.py", "v.py"], scope="small"),
    ])]
    result = analyze_execution_pack_risk_score(records)

    assert 0.0 <= result["aggregate_risk_score"] <= 1.0


def test_risk_distribution_categorizes_tasks():
    records = [_pack("p1", [
        _task("Low risk", expected_files=["a.py"], test_command="pytest", scope="medium"),
        _task(
            "High risk",
            expected_files=["a.py", "b.py", "c.py", "d.py", "e.py"],
            scope="small",
        ),
    ])]
    result = analyze_execution_pack_risk_score(records)

    dist = result["risk_distribution"]
    assert dist["low"] + dist["medium"] + dist["high"] == 2


def test_highest_risk_task_identified():
    records = [_pack("p1", [
        _task("Safe", expected_files=["a.py"], test_command="pytest", scope="medium"),
        _task(
            "Risky",
            expected_files=["a.py", "b.py", "c.py", "d.py", "e.py"],
            scope="small",
        ),
    ])]
    result = analyze_execution_pack_risk_score(records)

    assert result["highest_risk_task"] is not None
    assert result["highest_risk_task"]["title"] == "Risky"
    assert "scope_inflation" in result["highest_risk_task"]["factors"]


def test_pack_risk_scores_per_pack():
    records = [
        _pack("safe-pack", [
            _task("A", expected_files=["a.py"], test_command="pytest", scope="medium"),
        ]),
        _pack("risky-pack", [
            _task("B", expected_files=["x.py", "y.py", "z.py", "w.py", "v.py"], scope="small"),
            _task("C", expected_files=["x.py"]),
        ]),
    ]
    result = analyze_execution_pack_risk_score(records)

    assert "safe-pack" in result["pack_risk_scores"]
    assert "risky-pack" in result["pack_risk_scores"]
    assert result["pack_risk_scores"]["risky-pack"] > result["pack_risk_scores"]["safe-pack"]


def test_multiple_packs_aggregated():
    records = [
        _pack("p1", [_task("T1", expected_files=["a.py"])]),
        _pack("p2", [_task("T2", expected_files=["b.py"])]),
    ]
    result = analyze_execution_pack_risk_score(records)

    assert result["total_tasks"] == 2
    assert result["total_packs"] == 2
