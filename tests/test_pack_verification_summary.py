"""Tests for execution pack verification summary analysis."""

from synthesis.pack_verification_summary import analyze_pack_verification_summary


def test_empty_input_returns_zero_summary():
    report = analyze_pack_verification_summary([])

    assert report["task_count"] == 0
    assert report["verification_coverage_percentage"] == 0.0
    assert report["pass_rate_percentage"] == 0.0


def test_summarizes_pass_failed_and_missing_verification():
    report = analyze_pack_verification_summary(
        [
            {"task_id": "a", "verification_command": "pytest a", "verification_status": "passed"},
            {"task_id": "b", "verification_command": "pytest b", "verification_status": "failed"},
            {"task_id": "c", "status": "completed"},
        ]
    )

    assert report["task_count"] == 3
    assert report["verified_task_count"] == 2
    assert report["passed_count"] == 1
    assert report["failed_count"] == 1
    assert report["missing_count"] == 1
    assert report["verification_coverage_percentage"] == 66.67
    assert report["pass_rate_percentage"] == 50.0


def test_groups_verification_by_execution_pack_metadata():
    report = analyze_pack_verification_summary(
        [
            {
                "task_id": "a",
                "execution_pack": "pack-a",
                "verification_command": "pytest a",
                "verification_status": "passed",
            },
            {
                "task_id": "b",
                "pack": "pack-a",
                "verification_command": "pytest b",
                "verification_status": "failed",
            },
            {"task_id": "c", "pack_key": "pack-b", "status": "completed"},
            {"task_id": "d", "verification_command": "pytest d", "verification_status": "passed"},
        ]
    )

    assert report["packs"]["pack-a"] == {
        "task_count": 2,
        "passed": 1,
        "failed": 1,
        "missing": 0,
        "verification_coverage_percentage": 100.0,
        "pass_rate_percentage": 50.0,
    }
    assert report["packs"]["pack-b"]["missing"] == 1
    assert report["packs"]["unpackaged"]["passed"] == 1


def test_pack_summary_uses_unpacked_fallback_for_tasks_without_metadata():
    report = analyze_pack_verification_summary(
        [{"task_id": "a", "status": "completed"}, None]
    )

    assert set(report["packs"]) == {"unpackaged"}
    assert report["packs"]["unpackaged"]["task_count"] == 2
    assert report["packs"]["unpackaged"]["missing"] == 2


def test_uses_nested_execution_pack_key_metadata():
    report = analyze_pack_verification_summary(
        [
            {
                "task_id": "a",
                "execution_pack": {"key": "exports-reports"},
                "verification_command": "pytest a",
                "verification_status": "passed",
            },
            {
                "task_id": "b",
                "executionPack": {"key": "workflow-hygiene"},
                "verification_command": "pytest b",
                "verification_status": "failed",
            },
        ]
    )

    assert report["packs"]["exports-reports"]["passed"] == 1
    assert report["packs"]["workflow-hygiene"]["failed"] == 1


def test_nested_pack_metadata_without_key_falls_back_to_unpackaged():
    report = analyze_pack_verification_summary(
        [
            {
                "task_id": "a",
                "execution_pack": {"id": "exports-reports"},
                "verification_command": "pytest a",
                "verification_status": "passed",
            },
            {
                "task_id": "b",
                "executionPack": {"name": "workflow-hygiene"},
                "verification_command": "pytest b",
                "verification_status": "failed",
            },
        ]
    )

    assert set(report["packs"]) == {"unpackaged"}
    assert report["packs"]["unpackaged"]["passed"] == 1
    assert report["packs"]["unpackaged"]["failed"] == 1


def test_plain_pack_key_and_pack_string_metadata_still_group_normally():
    report = analyze_pack_verification_summary(
        [
            {
                "task_id": "a",
                "pack": "pack-a",
                "verification_command": "pytest a",
                "verification_status": "passed",
            },
            {
                "task_id": "b",
                "pack_key": "pack-b",
                "verification_command": "pytest b",
                "verification_status": "passed",
            },
        ]
    )

    assert report["packs"]["pack-a"]["passed"] == 1
    assert report["packs"]["pack-b"]["passed"] == 1
