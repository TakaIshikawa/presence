"""Tests for newsletter source reference auditing."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.newsletter_source_reference_audit import (
    build_newsletter_source_reference_audit_report,
    build_newsletter_source_reference_audit_report_from_fixture,
    format_newsletter_source_reference_audit_json,
    format_newsletter_source_reference_audit_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "newsletter_source_reference_audit.py"
)
spec = importlib.util.spec_from_file_location(
    "newsletter_source_reference_audit_script",
    SCRIPT_PATH,
)
newsletter_source_reference_audit_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_source_reference_audit_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str, *, published_url: str | None = "https://example.test/post") -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET published_url = ? WHERE id = ?",
        (published_url, content_id),
    )
    db.conn.commit()
    return content_id


def _send(
    db,
    source_content_ids,
    *,
    issue_id: str = "issue-1",
    subject: str | None = None,
    sent_at: datetime = NOW,
) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=subject or f"Newsletter {issue_id}",
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


def test_valid_send_has_no_reference_issues(db):
    first = _content(db, "first")
    second = _content(db, "second", published_url="https://example.test/second")
    _send(db, [first, second], issue_id="valid")

    report = build_newsletter_source_reference_audit_report(db, days=7, now=NOW)

    assert report.has_issues is False
    assert report.issues == ()
    assert report.totals["send_count"] == 1
    assert "No newsletter source reference issues" in format_newsletter_source_reference_audit_text(report)


def test_reports_one_issue_row_per_affected_send_with_requested_codes(db):
    published = _content(db, "published")
    unpublished = _content(db, "unpublished", published_url=None)
    send_id = _send(
        db,
        [published, unpublished, 9999, unpublished, "bad"],
        issue_id="broken",
        subject="Broken Sources",
    )

    report = build_newsletter_source_reference_audit_report(db, days=7, now=NOW)

    assert len(report.issues) == 1
    issue = report.issues[0]
    assert issue.newsletter_send_id == send_id
    assert issue.issue_id == "broken"
    assert issue.subject == "Broken Sources"
    assert issue.issue_codes == (
        "malformed_json",
        "missing_content",
        "duplicate_source_id",
        "unpublished_source",
    )
    assert issue.affected_content_ids == (unpublished, 9999)
    assert issue.missing_content_ids == (9999,)
    assert issue.duplicate_source_ids == (unpublished,)
    assert issue.unpublished_source_ids == (unpublished,)
    assert report.totals["by_issue_code"] == {
        "malformed_json": 1,
        "missing_content": 1,
        "duplicate_source_id": 1,
        "unpublished_source": 1,
    }


def test_malformed_json_send_reports_without_affected_content_ids(db):
    send_id = _send(db, "{not-json", issue_id="bad-json")

    report = build_newsletter_source_reference_audit_report(db, days=7, now=NOW)

    assert len(report.issues) == 1
    assert report.issues[0].newsletter_send_id == send_id
    assert report.issues[0].issue_codes == ("malformed_json",)
    assert report.issues[0].affected_content_ids == ()


def test_respects_days_and_limit(db):
    newest = _send(db, [123], issue_id="newest", sent_at=NOW)
    _send(db, [456], issue_id="older", sent_at=NOW - timedelta(days=2))
    _send(db, [789], issue_id="too-old", sent_at=NOW - timedelta(days=30))

    report = build_newsletter_source_reference_audit_report(
        db,
        days=7,
        limit=1,
        now=NOW,
    )

    assert report.totals["send_count"] == 1
    assert [issue.newsletter_send_id for issue in report.issues] == [newest]


def test_builder_tolerates_missing_schema_and_reports_metadata():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_newsletter_source_reference_audit_report(conn, now=NOW)

    assert report.issues == ()
    assert report.missing_tables == ("newsletter_sends", "generated_content")
    assert report.totals["by_issue_code"] == {
        "malformed_json": 0,
        "missing_content": 0,
        "duplicate_source_id": 0,
        "unpublished_source": 0,
    }


def test_json_formatter_is_deterministic_and_includes_totals(db):
    _send(db, [42], issue_id="json")

    report = build_newsletter_source_reference_audit_report(db, days=7, now=NOW)
    payload = json.loads(format_newsletter_source_reference_audit_json(report))

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "newsletter_source_reference_audit"
    assert payload["totals"]["by_issue_code"]["missing_content"] == 1
    assert payload["issues"][0]["issue_codes"] == ["missing_content"]
    assert payload["issues"][0]["affected_content_ids"] == [42]


def test_text_output_includes_send_label_codes_and_affected_ids(db):
    _send(db, [42], issue_id="", subject="Subject Fallback")

    report = build_newsletter_source_reference_audit_report(db, days=7, now=NOW)
    output = format_newsletter_source_reference_audit_text(report)

    assert "send=1" in output
    assert "issue=Subject Fallback" in output
    assert "codes=missing_content" in output
    assert "affected=42" in output


def test_fixture_builder_and_cli_json_output(tmp_path, capsys):
    fixture = tmp_path / "fixture.json"
    fixture.write_text(
        json.dumps(
            {
                "newsletter_sends": [
                    {
                        "id": 10,
                        "issue_id": "fixture",
                        "subject": "Fixture Issue",
                        "source_content_ids": [1, 2],
                        "sent_at": NOW.isoformat(),
                    }
                ],
                "generated_content": [
                    {"id": 1, "published_url": "https://example.test/one"},
                    {"id": 2, "published_url": ""},
                ],
            }
        ),
        encoding="utf-8",
    )

    report = build_newsletter_source_reference_audit_report_from_fixture(
        fixture,
        days=7,
        now=NOW,
    )

    assert report.filters["source"] == "fixture"
    assert report.issues[0].issue_codes == ("unpublished_source",)
    assert report.issues[0].affected_content_ids == (2,)

    exit_code = newsletter_source_reference_audit_script.main(
        ["--fixture", str(fixture), "--days", "7", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["issues"][0]["newsletter_send_id"] == 10
    assert payload["totals"]["by_issue_code"]["unpublished_source"] == 1


def test_cli_uses_database_context(db, monkeypatch, capsys):
    _send(db, [12345], issue_id="cli")
    monkeypatch.setattr(
        newsletter_source_reference_audit_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        newsletter_source_reference_audit_script,
        "build_newsletter_source_reference_audit_report",
        lambda db, **kwargs: build_newsletter_source_reference_audit_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = newsletter_source_reference_audit_script.main(
        ["--days", "7", "--limit", "0"]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "missing_content" in output
