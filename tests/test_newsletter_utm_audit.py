"""Tests for newsletter UTM coverage auditing."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from output.newsletter_utm_audit import (
    build_newsletter_utm_audit_report,
    classify_newsletter_utm_url,
    format_newsletter_utm_audit_json,
    format_newsletter_utm_audit_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_utm_audit.py"
spec = importlib.util.spec_from_file_location("newsletter_utm_audit_script", SCRIPT_PATH)
newsletter_utm_audit_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_utm_audit_script)


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
    content_ids: list[int],
    days_ago: int = 1,
    metadata: dict | None = None,
) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=subject,
        content_ids=content_ids,
        subscriber_count=100,
        metadata=metadata,
    )
    sent_at = (NOW - timedelta(days=days_ago)).isoformat()
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        (sent_at, send_id),
    )
    db.conn.commit()
    return int(send_id)


def test_classifies_tracking_deterministically():
    assert classify_newsletter_utm_url(
        "https://example.com/post?utm_source=n&utm_medium=e&utm_campaign=w"
    ) == ("complete", (), "example.com")
    assert classify_newsletter_utm_url("https://example.com/post?utm_medium=e") == (
        "missing_utm_source",
        ("utm_source", "utm_campaign"),
        "example.com",
    )
    assert classify_newsletter_utm_url("https://example.com/post?utm_source=n") == (
        "missing_utm_medium",
        ("utm_medium", "utm_campaign"),
        "example.com",
    )
    assert classify_newsletter_utm_url("https://localhost/post?utm_source=n") == (
        "not_trackable",
        (),
        "localhost",
    )


def test_audit_extracts_send_body_click_rows_and_newsletter_variants(db):
    db.conn.execute("ALTER TABLE newsletter_sends ADD COLUMN body TEXT")
    first_content = _content(db)
    second_content = _content(db)
    first = _send(
        db,
        issue_id="issue-1",
        subject="Weekly",
        content_ids=[first_content],
        metadata={
            "links": [
                {"url": "https://meta.example/a?utm_source=n&utm_medium=e"},
            ]
        },
    )
    _send(
        db,
        issue_id="issue-2",
        subject="Fully tracked",
        content_ids=[second_content],
        metadata={
            "body": "Read https://complete.example/post?utm_source=n&utm_medium=e&utm_campaign=w"
        },
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET body = ? WHERE id = ?",
        (
            "Body [missing](https://body.example/post?utm_source=n&utm_campaign=w) "
            "and complete https://complete.example/post?utm_source=n&utm_medium=e&utm_campaign=w",
            first,
        ),
    )
    db.upsert_content_variant(
        first_content,
        "newsletter",
        "body",
        "Variant https://variant.example/post?utm_campaign=w",
        metadata={"cta_url": "https://variant.example/cta?utm_source=n&utm_medium=e"},
    )
    db.insert_newsletter_link_clicks(
        first,
        "issue-1",
        [
            {
                "url": "https://click.example/post?utm_source=n",
                "raw_url": "https://raw.example/post?utm_medium=e&utm_campaign=w",
                "clicks": 3,
            }
        ],
        fetched_at=NOW.isoformat(),
    )

    report = build_newsletter_utm_audit_report(db, days=7, now=NOW)
    payload = json.loads(format_newsletter_utm_audit_json(report))

    assert payload["artifact_type"] == "newsletter_utm_audit"
    assert payload["totals"]["send_count"] == 2
    assert payload["totals"]["status_totals"]["complete"] == 0
    assert payload["totals"]["status_totals"]["missing_utm_medium"] == 2
    assert payload["totals"]["status_totals"]["missing_utm_campaign"] == 2
    first_send = next(
        send for send in payload["sends"] if send["newsletter_send_id"] == first
    )
    urls = {link["url"]: link for link in first_send["links"]}
    assert urls["https://body.example/post?utm_source=n&utm_campaign=w"]["missing_parameters"] == [
        "utm_medium"
    ]
    assert urls["https://click.example/post?utm_source=n"]["sources"] == [
        "newsletter_link_clicks.link_url"
    ]
    assert urls["https://raw.example/post?utm_medium=e&utm_campaign=w"]["status"] == (
        "missing_utm_source"
    )
    assert urls["https://variant.example/post?utm_campaign=w"]["sources"] == [
        "content_variants:newsletter:body"
    ]
    assert "complete.example" not in report.domain_totals


def test_include_complete_keeps_fully_tracked_links(db):
    send_id = _send(
        db,
        issue_id="issue-1",
        subject="Weekly",
        content_ids=[],
        metadata={
            "body": "Read https://example.com/post?utm_source=n&utm_medium=e&utm_campaign=w"
        },
    )

    report = build_newsletter_utm_audit_report(
        db,
        days=7,
        include_complete=True,
        now=NOW,
    )

    assert report.sends[0].newsletter_send_id == send_id
    assert report.sends[0].links[0].status == "complete"
    assert report.totals["status_totals"]["complete"] == 1


def test_json_and_text_include_totals_by_status_and_domain(db):
    _send(
        db,
        issue_id="issue-1",
        subject="Weekly",
        content_ids=[],
        metadata={"body": "Read https://example.com/post?utm_source=n"},
    )

    report = build_newsletter_utm_audit_report(db, days=7, now=NOW)
    payload = json.loads(format_newsletter_utm_audit_json(report))
    text = format_newsletter_utm_audit_text(report)

    assert payload["totals"]["status_totals"]["missing_utm_medium"] == 1
    assert payload["domain_totals"]["example.com"]["missing_utm_medium"] == 1
    assert payload["sends"][0]["links"][0]["missing_parameters"] == [
        "utm_medium",
        "utm_campaign",
    ]
    assert "Newsletter UTM Audit" in text
    assert "missing=utm_medium,utm_campaign" in text


def test_missing_tables_are_graceful():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_newsletter_utm_audit_report(conn, now=NOW)

    assert report.missing_tables == (
        "newsletter_sends",
        "newsletter_link_clicks",
        "content_variants",
    )
    assert report.totals["link_count"] == 0


def test_cli_supports_requested_flags(db, monkeypatch, capsys):
    _send(
        db,
        issue_id="issue-1",
        subject="Weekly",
        content_ids=[],
        metadata={
            "body": "Read https://example.com/post?utm_source=n&utm_medium=e&utm_campaign=w"
        },
    )
    monkeypatch.setattr(
        newsletter_utm_audit_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        newsletter_utm_audit_script,
        "build_newsletter_utm_audit_report",
        lambda db, **kwargs: build_newsletter_utm_audit_report(db, now=NOW, **kwargs),
    )

    exit_code = newsletter_utm_audit_script.main(
        ["--days", "7", "--format", "json", "--include-complete"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"]["include_complete"] is True
    assert payload["totals"]["status_totals"]["complete"] == 1
