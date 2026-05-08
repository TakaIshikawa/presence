"""Tests for session failure triage analysis."""

import pytest

from synthesis.session_failure_triage import SessionFailureEvent, analyze_session_failure_triage


def test_pytest_failure_followed_by_diagnostic_read_is_triaged():
    report = analyze_session_failure_triage(
        [
            SessionFailureEvent(0, "test_failure", "pytest tests/test_app.py", failure_text="assert failed"),
            SessionFailureEvent(1, "read", file_path="tests/test_app.py"),
            SessionFailureEvent(2, "test", "pytest tests/test_app.py"),
        ]
    )

    assert report.total_failures == 1
    assert report.triaged_failures == 1
    assert report.triage_quality == "strong"


def test_direct_retry_without_diagnostics_is_counted_separately():
    report = analyze_session_failure_triage(
        [
            SessionFailureEvent(0, "command_failure", "npm test", failure_text="boom"),
            SessionFailureEvent(1, "command", "npm test"),
        ]
    )

    assert report.direct_retries_without_diagnostics == 1
    assert report.triaged_failures == 0
    assert report.triage_quality == "weak"


def test_abandoned_failure_is_counted():
    report = analyze_session_failure_triage(
        [SessionFailureEvent(0, "command_failure", "pytest", failure_text="failed")]
    )

    assert report.abandoned_failures == 1


def test_non_failure_session_reports_clean_quality():
    report = analyze_session_failure_triage(
        [SessionFailureEvent(0, "command", "pytest tests/test_app.py")]
    )

    assert report.total_failures == 0
    assert report.triage_quality == "clean"


def test_repeated_failures_are_independently_counted():
    report = analyze_session_failure_triage(
        [
            SessionFailureEvent(0, "command_failure", "pytest a", failure_text="a"),
            SessionFailureEvent(1, "search", file_path="src"),
            SessionFailureEvent(2, "command", "pytest a"),
            SessionFailureEvent(3, "command_failure", "pytest b", failure_text="b"),
            SessionFailureEvent(4, "command", "pytest b"),
        ]
    )

    assert report.triaged_failures == 1
    assert report.direct_retries_without_diagnostics == 1


def test_runtime_validation_rejects_bad_records_and_ordering():
    with pytest.raises(ValueError, match="SessionFailureEvent"):
        analyze_session_failure_triage([object()])
    with pytest.raises(ValueError, match="ordered"):
        analyze_session_failure_triage(
            [
                SessionFailureEvent(1, "command", "pytest"),
                SessionFailureEvent(0, "command", "pytest"),
            ]
        )
