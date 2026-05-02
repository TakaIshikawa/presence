"""Tests for newsletter sponsor disclosure auditing."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from output.newsletter_disclosure_audit import (
    build_newsletter_disclosure_audit_report,
    format_newsletter_disclosure_audit_json,
    format_newsletter_disclosure_audit_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "audit_newsletter_disclosures.py"
)
spec = importlib.util.spec_from_file_location(
    "audit_newsletter_disclosures_script",
    SCRIPT_PATH,
)
audit_newsletter_disclosures_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(audit_newsletter_disclosures_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str = "post") -> int:
    return db.insert_generated_content(
        content_type="blog_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )


def _send(
    db,
    *,
    issue_id: str,
    subject: str,
    days_ago: int = 1,
    status: str = "draft",
    content_ids: list[int] | None = None,
    metadata: dict | None = None,
) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=subject,
        content_ids=content_ids or [],
        subscriber_count=100,
        status=status,
        metadata=metadata,
    )
    sent_at = (NOW - timedelta(days=days_ago)).isoformat()
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        (sent_at, send_id),
    )
    db.conn.commit()
    return int(send_id)


def test_reports_sponsorship_language_without_disclosure(db):
    db.conn.execute("ALTER TABLE newsletter_sends ADD COLUMN body TEXT")
    send_id = _send(
        db,
        issue_id="issue-1",
        subject="Weekly sponsor spotlight",
        status="queued",
        metadata={"intro": "Our partner Acme has a promo code for readers."},
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET body = ? WHERE id = ?",
        ("Thanks to our sponsor Acme for supporting this issue.", send_id),
    )
    db.conn.commit()

    report = build_newsletter_disclosure_audit_report(db, days=7, now=NOW)
    payload = json.loads(format_newsletter_disclosure_audit_json(report))

    assert payload["artifact_type"] == "newsletter_disclosure_audit"
    assert payload["totals"]["send_count"] == 1
    assert payload["totals"]["finding_count"] == 1
    assert payload["totals"]["severity_totals"]["high"] == 1
    finding = payload["findings"][0]
    assert finding["newsletter_send_id"] == send_id
    assert finding["issue_id"] == "issue-1"
    assert finding["status"] == "queued"
    assert finding["severity"] == "high"
    assert finding["matched_terms"] == ["partner", "promo code", "sponsor"]
    assert "newsletter_sends.body" in finding["sources"]
    assert "newsletter_sends.metadata.intro" in finding["sources"]


def test_explicit_disclosure_language_suppresses_findings(db):
    _send(
        db,
        issue_id="issue-1",
        subject="Partner note",
        metadata={
            "body": (
                "Our partner Acme has a discount this week. "
                "Disclosure: affiliate links mean we may earn a commission."
            )
        },
    )

    report = build_newsletter_disclosure_audit_report(db, days=7, now=NOW)

    assert report.totals["finding_count"] == 0
    assert report.findings == ()


def test_status_days_limit_and_variants_are_audited(db):
    content_id = _content(db)
    matching = _send(
        db,
        issue_id="issue-1",
        subject="Weekly",
        status="archived",
        content_ids=[content_id],
    )
    _send(
        db,
        issue_id="issue-2",
        subject="Old sponsor",
        status="archived",
        days_ago=45,
        metadata={"body": "Sponsored story."},
    )
    _send(
        db,
        issue_id="issue-3",
        subject="Queued sponsor",
        status="queued",
        metadata={"body": "Sponsored story."},
    )
    db.upsert_content_variant(
        content_id,
        "newsletter",
        "body",
        "Affiliate referral offer for subscribers.",
    )

    report = build_newsletter_disclosure_audit_report(
        db,
        days=30,
        status="archived",
        limit=1,
        now=NOW,
    )

    assert report.totals["send_count"] == 1
    assert report.findings[0].newsletter_send_id == matching
    assert report.findings[0].severity == "medium"
    assert report.findings[0].matched_terms == ("affiliate", "referral")
    assert report.filters["status"] == ["archived"]
    assert report.filters["limit"] == 1


def test_configurable_disclosure_terms(db):
    _send(
        db,
        issue_id="issue-1",
        subject="Sponsor note",
        metadata={"body": "Sponsor note. Transparent funding note included."},
    )

    flagged = build_newsletter_disclosure_audit_report(db, days=7, now=NOW)
    suppressed = build_newsletter_disclosure_audit_report(
        db,
        days=7,
        disclosure_terms=("transparent funding note",),
        now=NOW,
    )

    assert flagged.totals["finding_count"] == 1
    assert suppressed.totals["finding_count"] == 0


def test_json_and_text_include_totals_by_severity(db):
    _send(
        db,
        issue_id="issue-1",
        subject="Referral offer",
        metadata={"body": "Referral offer for readers."},
    )

    report = build_newsletter_disclosure_audit_report(db, days=7, now=NOW)
    payload = json.loads(format_newsletter_disclosure_audit_json(report))
    text = format_newsletter_disclosure_audit_text(report)

    assert payload["totals"]["severity_totals"]["medium"] == 1
    assert "Newsletter Disclosure Audit" in text
    assert "medium=1" in text
    assert "terms=referral" in text


def test_missing_newsletter_table_is_graceful():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_newsletter_disclosure_audit_report(conn, now=NOW)
    text = format_newsletter_disclosure_audit_text(report)

    assert report.missing_tables == ("newsletter_sends",)
    assert report.totals["finding_count"] == 0
    assert "Missing tables: newsletter_sends" in text


def test_cli_supports_requested_flags(db, monkeypatch, capsys):
    _send(
        db,
        issue_id="issue-1",
        subject="Sponsor note",
        status="queued",
        metadata={"body": "Sponsor note."},
    )
    monkeypatch.setattr(
        audit_newsletter_disclosures_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        audit_newsletter_disclosures_script,
        "build_newsletter_disclosure_audit_report",
        lambda db, **kwargs: build_newsletter_disclosure_audit_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = audit_newsletter_disclosures_script.main(
        ["--days", "7", "--status", "queued", "--json", "--limit", "5"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"]["status"] == ["queued"]
    assert payload["filters"]["limit"] == 5
    assert payload["totals"]["finding_count"] == 1
