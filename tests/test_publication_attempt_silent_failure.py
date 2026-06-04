from __future__ import annotations

from evaluation.publication_attempt_silent_failure import build_publication_attempt_silent_failure_report


def test_silent_failures_are_flagged():
    report = build_publication_attempt_silent_failure_report([
        {"attempt_id": "a1", "content_id": "c1", "platform": "x", "status": "failed", "attempted_at": "2026-01-01T00:00:00Z"}
    ])
    row = report["findings"][0]
    assert row["attempt_id"] == "a1"
    assert row["missing_fields"] == ["error_code", "response_code", "diagnostic_message"]


def test_diagnostic_failures_and_successes_are_ignored():
    report = build_publication_attempt_silent_failure_report([
        {"attempt_id": "a1", "status": "failed", "error_code": "401"},
        {"attempt_id": "a2", "status": "published"},
    ])
    assert report["findings"] == []


def test_limit_ordering():
    report = build_publication_attempt_silent_failure_report([
        {"attempt_id": "late", "status": "failed", "attempted_at": "2026-01-02"},
        {"attempt_id": "early", "status": "failed", "attempted_at": "2026-01-01"},
    ], limit=1)
    assert [row["attempt_id"] for row in report["findings"]] == ["early"]
