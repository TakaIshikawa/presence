"""Tests for newsletter subject-line fatigue reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from output.newsletter_subject_fatigue import (
    FREQUENT_TERM,
    REPEATED_OPENING,
    SIMILAR_STRUCTURE,
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
    sent_at: datetime = NOW,
    status: str = "sent",
) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=subject,
        content_ids=[],
        subscriber_count=10,
        status=status,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        (sent_at.isoformat(), send_id),
    )
    db.conn.commit()
    return send_id


def test_empty_result_when_no_newsletters_match_lookback(db):
    _send(
        db,
        issue_id="old",
        subject="Launch notes: too old",
        sent_at=NOW - timedelta(days=40),
    )

    report = build_newsletter_subject_fatigue_report(db, days=7, threshold=2, now=NOW)
    payload = json.loads(format_newsletter_subject_fatigue_json(report))

    assert report.has_findings is False
    assert report.findings == ()
    assert report.totals["subject_count"] == 0
    assert payload["artifact_type"] == "newsletter_subject_fatigue"
    assert payload["subject_count"] == 0
    assert "No newsletter subject fatigue patterns" in (
        format_newsletter_subject_fatigue_text(report)
    )


def test_no_fatigue_for_distinct_recent_subjects(db):
    _send(db, issue_id="one", subject="Reliability budget reaches stable rollout")
    _send(db, issue_id="two", subject="Pricing cleanup ships after audit")
    _send(db, issue_id="three", subject="Editor exports gain saved views")

    report = build_newsletter_subject_fatigue_report(db, days=7, threshold=2, now=NOW)

    assert report.has_findings is False
    assert report.findings == ()
    assert report.totals["subject_count"] == 3
    assert report.totals["by_finding_type"] == {}


def test_repeated_openings_terms_and_structure_are_reported(db):
    _send(
        db,
        issue_id="one",
        subject="Launch notes: database reliability fixes",
        sent_at=NOW - timedelta(days=3),
    )
    _send(
        db,
        issue_id="two",
        subject="Launch notes: dashboard reliability cleanup",
        sent_at=NOW - timedelta(days=2),
    )
    _send(
        db,
        issue_id="three",
        subject="Launch notes: billing reliability checks",
        sent_at=NOW - timedelta(days=1),
    )

    report = build_newsletter_subject_fatigue_report(db, days=7, threshold=3, now=NOW)
    findings = {(finding.finding_type, finding.pattern): finding for finding in report.findings}

    opening = findings[(REPEATED_OPENING, "launch notes")]
    term = findings[(FREQUENT_TERM, "reliability")]
    structure = next(
        finding for finding in report.findings if finding.finding_type == SIMILAR_STRUCTURE
    )

    assert opening.occurrence_count == 3
    assert opening.example_subjects == (
        "Launch notes: billing reliability checks",
        "Launch notes: dashboard reliability cleanup",
        "Launch notes: database reliability fixes",
    )
    assert "Retire the opening 'launch notes'" in opening.recommendation
    assert term.occurrence_count == 3
    assert "Replace or narrow the repeated term 'reliability'" in term.recommendation
    assert structure.pattern == "short subject with punctuation ':'"
    assert report.totals["by_finding_type"] == {
        FREQUENT_TERM: 3,
        REPEATED_OPENING: 1,
        SIMILAR_STRUCTURE: 1,
    }


def test_threshold_controls_findings(db):
    _send(db, issue_id="one", subject="Systems note: queue metrics")
    _send(db, issue_id="two", subject="Systems note: retry budgets")

    report = build_newsletter_subject_fatigue_report(db, days=7, threshold=3, now=NOW)

    assert report.findings == ()
    assert report.totals["subject_count"] == 2


def test_json_and_text_output_are_stable(db):
    _send(db, issue_id="one", subject="Signals: launch evidence")
    _send(db, issue_id="two", subject="Signals: launch checklist")

    report = build_newsletter_subject_fatigue_report(db, days=7, threshold=2, now=NOW)
    payload = json.loads(format_newsletter_subject_fatigue_json(report))
    text = format_newsletter_subject_fatigue_text(report)

    assert list(payload) == sorted(payload)
    assert payload["has_findings"] is True
    assert payload["subject_finding_count"] == len(report.findings)
    assert payload["findings"][0]["recommendation"]
    assert "Newsletter Subject Fatigue" in text
    assert "Subject fatigue findings:" in text
    assert "issue=two" in text


def test_cli_supports_days_threshold_and_json_output(db, monkeypatch, capsys):
    _send(db, issue_id="one", subject="Launch notes: database repairs")
    _send(db, issue_id="two", subject="Launch notes: dashboard cleanup")
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
        ["--days", "7", "--threshold", "2", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"]["days"] == 7
    assert payload["filters"]["threshold"] == 2
    assert any(
        finding["finding_type"] == REPEATED_OPENING
        and finding["pattern"] == "launch notes"
        for finding in payload["findings"]
    )


def test_cli_returns_nonzero_on_database_error(monkeypatch, capsys):
    monkeypatch.setattr(
        newsletter_subject_fatigue_script,
        "script_context",
        lambda: _script_context(SimpleNamespace()),
    )
    monkeypatch.setattr(
        newsletter_subject_fatigue_script,
        "build_newsletter_subject_fatigue_report",
        lambda *args, **kwargs: (_ for _ in ()).throw(sqlite3.Error("db failed")),
    )

    exit_code = newsletter_subject_fatigue_script.main([])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "error: db failed" in captured.err
