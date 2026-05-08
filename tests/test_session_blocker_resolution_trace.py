"""Tests for session blocker resolution trace analysis."""

import pytest

from synthesis.session_blocker_resolution_trace import analyze_session_blocker_resolution_trace


def test_empty_input_returns_zero_metrics():
    report = analyze_session_blocker_resolution_trace([])

    assert report["total_blockers"] == 0
    assert report["resolution_percentage"] == 0.0


def test_resolved_blocker_status_counts():
    report = analyze_session_blocker_resolution_trace(
        [{"session_id": "s1", "blockers": [{"description": "deps missing", "status": "resolved"}]}]
    )

    assert report["resolved_blockers"] == 1
    assert report["unresolved_blockers"] == 0


def test_resolution_evidence_counts():
    report = analyze_session_blocker_resolution_trace(
        [{"session_id": "s2", "blockers": [{"description": "test failed", "resolution": "fixed and verified"}]}]
    )

    assert report["resolved_blockers"] == 1


def test_unresolved_blocker_reports_example():
    report = analyze_session_blocker_resolution_trace([{"session_id": "s3", "blockers": ["CI unavailable"]}])

    assert report["unresolved_blockers"] == 1
    assert report["examples"][0]["session_id"] == "s3"


def test_examples_are_limited():
    report = analyze_session_blocker_resolution_trace(
        [{"session_id": str(i), "blockers": ["blocked"]} for i in range(7)]
    )

    assert len(report["examples"]) == 5


def test_non_list_input_raises():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_session_blocker_resolution_trace({"blockers": []})
