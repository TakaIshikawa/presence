"""Tests for newsletter subject-line fatigue reports."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.newsletter_subject_fatigue import (
    NEAR_DUPLICATE,
    OPENING,
    PUNCTUATION,
    build_newsletter_subject_fatigue_report,
    format_newsletter_subject_fatigue_json,
    format_newsletter_subject_fatigue_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "newsletter_subject_fatigue.py"
)
spec = importlib.util.spec_from_file_location("newsletter_subject_fatigue_script", SCRIPT_PATH)
newsletter_subject_fatigue_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_subject_fatigue_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _send(
    db,
    *,
    issue_id: str,
    subject: str,
    candidates: list[str] | None = None,
    opens: int = 40,
    clicks: int = 6,
    subscriber_count: int = 100,
    sent_at: str = "2026-04-30T12:00:00+00:00",
) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=subject,
        content_ids=[1],
        subscriber_count=subscriber_count,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        (sent_at, send_id),
    )
    db.insert_newsletter_subject_candidates(
        [{"subject": item, "score": 8.0, "rationale": "candidate"} for item in (candidates or [subject])],
        content_ids=[1],
        selected_subject=subject,
        newsletter_send_id=send_id,
        issue_id=issue_id,
    )
    db.insert_newsletter_engagement(
        newsletter_send_id=send_id,
        issue_id=issue_id,
        opens=opens,
        clicks=clicks,
        unsubscribes=0,
        fetched_at=sent_at,
    )
    return send_id


def test_report_flags_repeated_openings_punctuation_and_declining_metrics(db):
    _send(
        db,
        issue_id="issue-1",
        subject="Launch notes: database repairs",
        candidates=["Launch notes: database repairs"],
        opens=50,
        clicks=10,
        sent_at="2026-04-01T12:00:00+00:00",
    )
    _send(
        db,
        issue_id="issue-2",
        subject="Launch notes: dashboard cleanup",
        candidates=["Launch notes: dashboard cleanup"],
        opens=42,
        clicks=6,
        sent_at="2026-04-15T12:00:00+00:00",
    )
    _send(
        db,
        issue_id="issue-3",
        subject="Launch notes: billing checks",
        candidates=["Launch notes: billing checks"],
        opens=30,
        clicks=3,
        sent_at="2026-04-29T12:00:00+00:00",
    )

    report = build_newsletter_subject_fatigue_report(
        db,
        days=60,
        threshold=3,
        now=NOW,
    )

    opening = next(
        finding
        for finding in report.findings
        if finding.pattern_type == OPENING and finding.pattern == "launch notes"
    )
    punctuation = next(
        finding
        for finding in report.findings
        if finding.pattern_type == PUNCTUATION and finding.pattern == ":"
    )

    assert opening.occurrences == 3
    assert opening.selected_occurrences == 3
    assert opening.average_open_rate == 0.4067
    assert opening.average_click_rate == 0.0633
    assert opening.open_rate_delta == -0.14
    assert opening.click_rate_delta == -0.055
    assert "Retire the opening 'launch notes'" in opening.guidance
    assert punctuation.occurrences == 3
    assert "Replace the repeated punctuation shape ':'" in punctuation.guidance


def test_report_flags_near_duplicate_candidates_even_below_repeat_threshold(db):
    _send(
        db,
        issue_id="issue-1",
        subject="What changed in the release dashboard",
        candidates=[
            "What changed in the release dashboard",
            "What changed in release dashboard",
        ],
        opens=45,
        clicks=8,
    )

    report = build_newsletter_subject_fatigue_report(
        db,
        days=30,
        threshold=3,
        now=NOW,
    )

    near_duplicate = next(
        finding for finding in report.findings if finding.pattern_type == NEAR_DUPLICATE
    )

    assert near_duplicate.occurrences == 2
    assert near_duplicate.selected_occurrences == 1
    assert near_duplicate.average_open_rate == 0.45
    assert near_duplicate.average_click_rate == 0.08
    assert near_duplicate.guidance.startswith("Rewrite one of the near-duplicate")


def test_json_and_text_output_are_stable(db):
    _send(
        db,
        issue_id="issue-1",
        subject="Launch notes: database repairs",
        candidates=["Launch notes: database repairs"],
    )
    _send(
        db,
        issue_id="issue-2",
        subject="Launch notes: dashboard cleanup",
        candidates=["Launch notes: dashboard cleanup"],
    )

    report = build_newsletter_subject_fatigue_report(
        db,
        days=60,
        threshold=2,
        now=NOW,
    )
    payload = json.loads(format_newsletter_subject_fatigue_json(report))
    text = format_newsletter_subject_fatigue_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["candidate_count"] == 2
    assert payload["finding_count"] >= 2
    assert payload["findings"][0]["guidance"]
    assert "Newsletter Subject Fatigue Report" in text
    assert "Guidance:" in text


def test_cli_supports_days_threshold_and_json_output(db, monkeypatch, capsys):
    _send(
        db,
        issue_id="issue-1",
        subject="Launch notes: database repairs",
        candidates=["Launch notes: database repairs"],
    )
    _send(
        db,
        issue_id="issue-2",
        subject="Launch notes: dashboard cleanup",
        candidates=["Launch notes: dashboard cleanup"],
    )
    monkeypatch.setattr(
        newsletter_subject_fatigue_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        newsletter_subject_fatigue_script,
        "build_newsletter_subject_fatigue_report",
        lambda db, **kwargs: build_newsletter_subject_fatigue_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = newsletter_subject_fatigue_script.main(
        ["--days", "60", "--threshold", "2", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["period_days"] == 60
    assert payload["threshold"] == 2
    assert any(finding["pattern"] == "launch notes" for finding in payload["findings"])
