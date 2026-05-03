"""Tests for newsletter CTA coverage reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from output.newsletter_cta_coverage import (
    analyze_newsletter_cta_coverage,
    build_newsletter_cta_coverage_report,
    format_newsletter_cta_coverage_json,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_cta_coverage.py"
spec = importlib.util.spec_from_file_location("newsletter_cta_coverage_script", SCRIPT_PATH)
newsletter_cta_coverage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_cta_coverage_script)


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
    body: str,
    *,
    subject: str | None = None,
    status: str = "draft",
    days_ago: int = 0,
    metadata: dict | None = None,
) -> int:
    _ensure_body_column(db)
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=subject or f"Subject {issue_id}",
        content_ids=[101],
        subscriber_count=100,
        status=status,
        metadata=metadata or {},
    )
    sent_at = NOW - timedelta(days=days_ago)
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ?, body = ? WHERE id = ?",
        (sent_at.isoformat(), body, send_id),
    )
    db.conn.commit()
    return send_id


def test_explicit_link_cta_is_strong():
    row = analyze_newsletter_cta_coverage(
        "The release notes are ready.\n\nRead the full guide: https://example.com/guide",
        draft_id="issue-link",
        subject="Guide",
    )

    assert row.coverage == "strong"
    assert row.cta_type == "link"
    assert row.link_present is True
    assert row.reason == "clear action phrase includes a link"
    assert row.draft_id == "issue-link"
    assert row.subject == "Guide"


def test_reply_prompt_is_strong_without_link():
    row = analyze_newsletter_cta_coverage(
        "The workflow is still early.\n\nReply with the step you want covered next."
    )

    assert row.coverage == "strong"
    assert row.cta_type == "reply_prompt"
    assert row.link_present is False
    assert "Reply with" in row.matched_text


def test_subscription_and_share_ctas_are_strong():
    subscription = analyze_newsletter_cta_coverage(
        "Subscribe for the weekly operator notes at https://example.com/newsletter."
    )
    share = analyze_newsletter_cta_coverage(
        "If this was useful, forward it to one teammate."
    )

    assert subscription.coverage == "strong"
    assert subscription.cta_type == "subscription"
    assert subscription.link_present is True
    assert share.coverage == "strong"
    assert share.cta_type == "share"
    assert share.link_present is False


def test_weak_cta_and_missing_cta_reasons_are_reported():
    weak = analyze_newsletter_cta_coverage(
        "The analysis is available now.\n\nLearn more when you have time."
    )
    link_only = analyze_newsletter_cta_coverage("Source: https://example.com/source")
    missing = analyze_newsletter_cta_coverage(
        "This issue summarizes the incident timeline and next release notes."
    )

    assert weak.coverage == "weak"
    assert weak.cta_type == "weak_link"
    assert weak.reason == "CTA wording is vague or low intent"
    assert link_only.coverage == "weak"
    assert link_only.link_present is True
    assert link_only.reason == "link present without a clear reader action"
    assert missing.coverage == "missing"
    assert missing.cta_type == "none"
    assert missing.reason == "no clear CTA phrase or link detected"


def test_build_report_reads_recent_drafts_and_filters_weak_or_missing(db):
    _send(
        db,
        "issue-strong",
        "Intro.\n\nReply with what you want next.",
        days_ago=2,
    )
    _send(
        db,
        "issue-weak",
        "Intro.\n\nSource: https://example.com/source",
        subject="Weak subject",
        days_ago=1,
        metadata={"title": "Weak title"},
    )
    _send(db, "issue-missing", "Intro and context only.", days_ago=0)
    _send(
        db,
        "issue-sent",
        "Sent rows are not draft review rows.",
        status="sent",
        days_ago=0,
    )

    report = build_newsletter_cta_coverage_report(
        db,
        limit=10,
        coverage_filter=("weak", "missing"),
        now=NOW,
    )
    payload = json.loads(format_newsletter_cta_coverage_json(report))

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "newsletter_cta_coverage"
    assert payload["totals"] == {"missing": 1, "rows": 2, "strong": 0, "weak": 1}
    assert [row["draft_id"] for row in payload["rows"]] == ["issue-missing", "issue-weak"]
    weak_row = payload["rows"][1]
    assert weak_row["subject"] == "Weak subject"
    assert weak_row["title"] == "Weak title"
    assert weak_row["cta_type"] == "link"
    assert weak_row["link_present"] is True


def test_structured_payload_and_metadata_cta_text_are_supported(db):
    _send(
        db,
        "issue-structured",
        "",
        metadata={
            "assembled_payload": {
                "title": "Structured title",
                "sections": [
                    {"title": "Intro", "body": "Opening context."},
                    {"title": "Close", "body": "Share this with the release team."},
                ],
            }
        },
    )
    _send(
        db,
        "issue-metadata-cta",
        "",
        metadata={"cta": {"text": "Book a demo", "url": "https://example.com/demo"}},
    )

    rows = build_newsletter_cta_coverage_report(db, limit=10, now=NOW).rows
    by_id = {row.draft_id: row for row in rows}

    assert by_id["issue-structured"].coverage == "strong"
    assert by_id["issue-structured"].cta_type == "share"
    assert by_id["issue-metadata-cta"].coverage == "strong"
    assert by_id["issue-metadata-cta"].link_present is True


def test_cli_emits_json_and_supports_weak_or_missing_filter(db, monkeypatch, capsys):
    _send(db, "issue-strong", "Reply with what you want next.", days_ago=1)
    _send(db, "issue-missing", "No reader action here.", days_ago=0)
    monkeypatch.setattr(
        newsletter_cta_coverage_script,
        "script_context",
        lambda: _script_context(db),
    )

    assert newsletter_cta_coverage_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    exit_code = newsletter_cta_coverage_script.main(["--weak-or-missing"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"]["coverage"] == ["weak", "missing"]
    assert [row["coverage"] for row in payload["rows"]] == ["missing"]
