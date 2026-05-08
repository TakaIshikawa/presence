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
