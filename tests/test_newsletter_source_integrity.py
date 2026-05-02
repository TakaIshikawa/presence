"""Tests for newsletter source_content_ids integrity auditing."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.newsletter_source_integrity import (
    build_newsletter_source_integrity_report,
    format_newsletter_source_integrity_json,
    format_newsletter_source_integrity_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_source_integrity.py"
spec = importlib.util.spec_from_file_location("newsletter_source_integrity_script", SCRIPT_PATH)
newsletter_source_integrity_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_source_integrity_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    text: str,
    *,
    published: int = 1,
    published_at: datetime | None = None,
) -> int:
    stored_published_at = published_at
    if stored_published_at is None and published == 1:
        stored_published_at = NOW - timedelta(days=1)
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET published = ?, published_at = ? WHERE id = ?",
        (
            published,
            stored_published_at.isoformat() if stored_published_at else None,
            content_id,
        ),
    )
    db.conn.commit()
    return content_id


def _send(
    db,
    source_content_ids,
    *,
    issue_id: str = "issue-1",
    sent_at: datetime = NOW,
) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=f"Newsletter {issue_id}",
        content_ids=[],
        subscriber_count=10,
    )
    raw_value = (
        source_content_ids
        if isinstance(source_content_ids, str) or source_content_ids is None
        else json.dumps(source_content_ids)
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET source_content_ids = ?, sent_at = ? WHERE id = ?",
        (raw_value, sent_at.isoformat(), send_id),
    )
    db.conn.commit()
    return send_id


def test_valid_send_has_no_integrity_issues(db):
    first = _content(db, "first")
    second = _content(db, "second")
    _send(db, [first, second], issue_id="valid")

    report = build_newsletter_source_integrity_report(db, days=7, now=NOW)

    assert report.has_issues is False
    assert report.issues == ()
    assert report.totals["send_count"] == 1
    assert "No newsletter source integrity issues" in format_newsletter_source_integrity_text(report)


def test_reports_malformed_missing_non_integer_and_duplicate_source_ids(db):
    content_id = _content(db, "published")
    missing_send = _send(db, None, issue_id="missing")
    malformed_send = _send(db, "{not-json", issue_id="malformed")
    bad_item_send = _send(db, [content_id, "2", 0, content_id], issue_id="bad-items")

    report = build_newsletter_source_integrity_report(db, days=7, now=NOW)
    issues = {(issue.newsletter_send_id, issue.issue_type) for issue in report.issues}

    assert (missing_send, "missing_source_content_ids") in issues
    assert (malformed_send, "malformed_source_content_ids") in issues
    assert (bad_item_send, "non_integer_source_content_id") in issues
    assert (bad_item_send, "duplicate_source_content_id") in issues
    for issue in report.issues:
        assert issue.newsletter_send_id
        assert issue.issue_id


def test_classifies_missing_duplicate_abandoned_unpublished_and_not_yet_published_separately(db):
    abandoned = _content(db, "abandoned", published=-1)
    unpublished = _content(db, "unpublished", published=0, published_at=None)
    future = _content(db, "future", published=1, published_at=NOW + timedelta(hours=2))
    send_id = _send(
        db,
        [abandoned, unpublished, future, 9999, abandoned],
        issue_id="integrity",
    )

    report = build_newsletter_source_integrity_report(db, days=7, now=NOW)
    by_type = {issue.issue_type: issue for issue in report.issues}

    assert by_type["abandoned_content_reference"].newsletter_send_id == send_id
    assert by_type["unpublished_content_reference"].source_content_id == unpublished
    assert by_type["not_yet_published_content_reference"].source_content_id == future
    assert by_type["missing_content_reference"].source_content_id == 9999
    assert by_type["duplicate_source_content_id"].source_content_id == abandoned
    assert report.totals["by_issue_type"] == {
        "abandoned_content_reference": 2,
        "duplicate_source_content_id": 1,
        "missing_content_reference": 1,
        "not_yet_published_content_reference": 1,
        "unpublished_content_reference": 1,
    }


def test_json_formatter_is_deterministic_and_includes_issue_context(db):
    missing_send = _send(db, [42], issue_id="json")

    report = build_newsletter_source_integrity_report(db, days=7, now=NOW)
    payload = json.loads(format_newsletter_source_integrity_json(report))

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "newsletter_source_integrity"
    assert payload["has_issues"] is True
    assert payload["issues"][0]["newsletter_send_id"] == missing_send
    assert payload["issues"][0]["issue_id"] == "json"
    assert payload["issues"][0]["issue_type"] == "missing_content_reference"


def test_cli_outputs_json_and_fail_on_issues_exit_behavior(db, monkeypatch, capsys):
    _send(db, [12345], issue_id="cli")
    monkeypatch.setattr(
        newsletter_source_integrity_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        newsletter_source_integrity_script,
        "build_newsletter_source_integrity_report",
        lambda db, **kwargs: build_newsletter_source_integrity_report(db, now=NOW, **kwargs),
    )

    exit_code = newsletter_source_integrity_script.main(["--days", "7", "--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["issue_count"] == 1

    exit_code = newsletter_source_integrity_script.main(["--days", "7", "--fail-on-issues"])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "missing_content_reference" in captured.out


def test_cli_fail_on_issues_returns_zero_without_issues(db, monkeypatch, capsys):
    content_id = _content(db, "valid")
    _send(db, [content_id], issue_id="clean")
    monkeypatch.setattr(
        newsletter_source_integrity_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        newsletter_source_integrity_script,
        "build_newsletter_source_integrity_report",
        lambda db, **kwargs: build_newsletter_source_integrity_report(db, now=NOW, **kwargs),
    )

    exit_code = newsletter_source_integrity_script.main(["--fail-on-issues"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "No newsletter source integrity issues" in captured.out


def test_missing_required_tables_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_newsletter_source_integrity_report(conn, now=NOW)

    assert report.issues == ()
    assert report.missing_tables == ("newsletter_sends", "generated_content")
