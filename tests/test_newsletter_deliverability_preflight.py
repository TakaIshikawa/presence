"""Tests for newsletter deliverability preflight reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from output.newsletter_deliverability_preflight import (
    build_newsletter_deliverability_preflight_report,
    format_newsletter_deliverability_preflight_json,
    format_newsletter_deliverability_preflight_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "newsletter_deliverability_preflight.py"
)
spec = importlib.util.spec_from_file_location(
    "newsletter_deliverability_preflight_script",
    SCRIPT_PATH,
)
newsletter_deliverability_preflight_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_deliverability_preflight_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _ensure_body_column(db) -> None:
    columns = {row["name"] for row in db.conn.execute("PRAGMA table_info(newsletter_sends)")}
    if "body" not in columns:
        db.conn.execute("ALTER TABLE newsletter_sends ADD COLUMN body TEXT")
        db.conn.commit()


def _send(
    db,
    issue_id: str,
    subject: str,
    *,
    body: str | None,
    content_ids: list[int] | None = None,
    status: str = "draft",
    days_ago: int = 0,
) -> int:
    _ensure_body_column(db)
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=subject,
        content_ids=content_ids if content_ids is not None else [101],
        subscriber_count=100,
        status=status,
    )
    sent_at = NOW - timedelta(days=days_ago)
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ?, body = ? WHERE id = ?",
        (sent_at.isoformat(), body, send_id),
    )
    db.conn.commit()
    return send_id


def test_clean_newsletter_has_no_warnings(db):
    _send(
        db,
        "issue-clean",
        "Practical launch notes",
        body=(
            "A concise update with one link https://example.com/read.\n\n"
            "You can unsubscribe or manage preferences anytime.\n"
            "Presence newsletter footer."
        ),
    )

    report = build_newsletter_deliverability_preflight_report(db, days=30, limit=5)
    text = format_newsletter_deliverability_preflight_text(report)

    assert report.total_rows_inspected == 1
    assert report.risky_rows == 0
    assert report.issues[0].warnings == ()
    assert report.issues[0].risk_score == 0
    assert "warnings=clean" in text


def test_spammy_subject_flags_and_scores(db):
    _send(
        db,
        "issue-spam",
        "FREE CASH ACT NOW!!!",
        body="Read https://example.com. Unsubscribe here. Newsletter footer.",
    )

    report = build_newsletter_deliverability_preflight_report(db, days=30, limit=5)
    issue = report.issues[0]

    assert issue.subject_flags == (
        "spammy_subject_terms",
        "excessive_subject_punctuation",
        "all_caps_subject",
    )
    assert issue.warnings[:3] == issue.subject_flags
    assert issue.risk_score == 35


def test_link_heavy_content_is_flagged(db):
    links = " ".join(f"https://example.com/{index}" for index in range(13))
    _send(
        db,
        "issue-links",
        "Useful reading list",
        body=f"{links}\nUnsubscribe here. Newsletter footer.",
    )

    report = build_newsletter_deliverability_preflight_report(db, days=30, limit=5)
    issue = report.issues[0]

    assert issue.link_count == 13
    assert issue.warnings == ("too_many_links",)
    assert issue.risk_score == 20


def test_missing_and_malformed_source_ids_are_warnings(db):
    send_id = _send(
        db,
        "issue-sources",
        "Source check",
        body="Read https://example.com. Unsubscribe here. Newsletter footer.",
        content_ids=[],
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET source_content_ids = ? WHERE id = ?",
        ('["bad", -1]', send_id),
    )
    db.conn.commit()

    report = build_newsletter_deliverability_preflight_report(db, days=30, limit=5)
    issue = report.issues[0]

    assert issue.source_count == 0
    assert issue.warnings == (
        "malformed_source_content_ids",
        "missing_source_content_ids",
    )


def test_empty_body_and_missing_footer_markers(db):
    _send(db, "issue-empty", "Empty body", body="", content_ids=[42])
    _send(
        db,
        "issue-footer",
        "Missing footer",
        body="Read this update at https://example.com/update.",
        content_ids=[42],
    )

    report = build_newsletter_deliverability_preflight_report(db, days=30, limit=5)
    by_issue = {issue.issue_id: issue for issue in report.issues}

    assert by_issue["issue-empty"].warnings == ("empty_body",)
    assert by_issue["issue-footer"].warnings == (
        "missing_unsubscribe_marker",
        "missing_footer_marker",
    )


def test_cli_json_output_is_sorted_and_limited(db, monkeypatch, capsys):
    _send(
        db,
        "issue-json",
        "FREE NOW!!",
        body="Read https://example.com. Unsubscribe here. Newsletter footer.",
    )
    _send(
        db,
        "issue-old",
        "Old",
        body="Read https://example.com. Unsubscribe here. Newsletter footer.",
        days_ago=45,
    )
    report = build_newsletter_deliverability_preflight_report(db, days=30, limit=1)
    payload = json.loads(format_newsletter_deliverability_preflight_json(report))

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "newsletter_deliverability_preflight"
    assert payload["total_rows_inspected"] == 1
    assert payload["issues"][0]["issue_id"] == "issue-json"

    monkeypatch.setattr(
        newsletter_deliverability_preflight_script,
        "script_context",
        lambda: _script_context(db),
    )
    exit_code = newsletter_deliverability_preflight_script.main(
        ["--days", "30", "--limit", "1", "--json"]
    )
    cli_payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert list(cli_payload) == sorted(cli_payload)
    assert cli_payload["issues"][0]["issue_id"] == "issue-json"
