"""Tests for newsletter intro click-alignment reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.newsletter_intro_click_alignment import (
    build_newsletter_intro_click_alignment_report,
    format_newsletter_intro_click_alignment_json,
    format_newsletter_intro_click_alignment_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "newsletter_intro_click_alignment.py"
)
spec = importlib.util.spec_from_file_location(
    "newsletter_intro_click_alignment_script",
    SCRIPT_PATH,
)
newsletter_intro_click_alignment_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_intro_click_alignment_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, body: str, *, url: str = "https://example.com/content") -> int:
    content_id = db.insert_generated_content(
        content_type="blog_post",
        source_commits=[],
        source_messages=[],
        content=body,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET published = 1, published_url = ? WHERE id = ?",
        (url, content_id),
    )
    db.conn.commit()
    return content_id


def _send(
    db,
    issue_id: str,
    metadata,
    *,
    days_ago: int = 0,
    status: str = "sent",
) -> int:
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=f"Subject {issue_id}",
        content_ids=[],
        subscriber_count=100,
        status=status,
        metadata=metadata,
    )
    sent_at = NOW - timedelta(days=days_ago)
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        (sent_at.isoformat(), send_id),
    )
    db.conn.commit()
    return send_id


def _clicks(
    db,
    send_id: int,
    issue_id: str,
    link_clicks: list[dict],
    fetched_at: str = "2026-05-02T11:00:00+00:00",
) -> None:
    db.insert_newsletter_link_clicks(
        newsletter_send_id=send_id,
        issue_id=issue_id,
        link_clicks=link_clicks,
        fetched_at=fetched_at,
    )


def test_flags_clicked_links_unrelated_to_intro_terms(db):
    aligned_content_id = _content(
        db,
        "A pricing migration checklist for billing plans and rollout decisions.",
        url="https://example.com/pricing-migration-checklist",
    )
    unrelated_content_id = _content(
        db,
        "A camera gear teardown for studio lighting and lenses.",
        url="https://example.com/camera-gear",
    )
    aligned_send = _send(
        db,
        "issue-aligned",
        {"intro": "This week: a pricing migration checklist for billing teams."},
        days_ago=1,
    )
    unrelated_send = _send(
        db,
        "issue-unrelated",
        {"intro": "This week: a pricing migration checklist for billing teams."},
        days_ago=2,
    )
    _clicks(
        db,
        aligned_send,
        "issue-aligned",
        [{"url": "https://example.com/pricing-migration-checklist", "clicks": 8}],
    )
    _clicks(
        db,
        unrelated_send,
        "issue-unrelated",
        [{"url": "https://example.com/camera-gear", "clicks": 9}],
    )
    db.conn.execute(
        "UPDATE newsletter_link_clicks SET content_id = ? WHERE newsletter_send_id = ?",
        (aligned_content_id, aligned_send),
    )
    db.conn.execute(
        "UPDATE newsletter_link_clicks SET content_id = ? WHERE newsletter_send_id = ?",
        (unrelated_content_id, unrelated_send),
    )
    db.conn.commit()

    report = build_newsletter_intro_click_alignment_report(
        db,
        days=30,
        limit=10,
        min_clicks=3,
        now=NOW,
    )

    assert report.totals["sends_scanned"] == 2
    assert report.totals["included_send_count"] == 2
    assert report.totals["flagged_count"] == 1
    assert [finding.issue_id for finding in report.findings] == [
        "issue-unrelated",
        "issue-aligned",
    ]
    flagged = report.findings[0]
    assert flagged.alignment_score < 0.2
    assert flagged.warnings == ("low_intro_click_alignment",)
    assert "top unrelated clicked link" in flagged.reasons[-1]
    assert report.findings[1].alignment_score >= 0.2
    assert "pricing" in report.findings[1].top_links[0].overlap_terms


def test_min_click_rows_are_excluded_from_ranking_but_counted_in_totals(db):
    included_send = _send(db, "issue-included", {"summary": "Database indexing query plans"})
    excluded_send = _send(db, "issue-low", {"summary": "Database indexing query plans"})
    _clicks(
        db,
        included_send,
        "issue-included",
        [{"url": "https://example.com/database-indexing", "clicks": 4}],
    )
    _clicks(
        db,
        excluded_send,
        "issue-low",
        [{"url": "https://example.com/database-indexing", "clicks": 2}],
    )

    report = build_newsletter_intro_click_alignment_report(
        db,
        days=30,
        min_clicks=3,
        now=NOW,
    )

    assert report.totals["sends_with_clicks"] == 2
    assert report.totals["total_clicks"] == 6
    assert report.totals["excluded_below_min_clicks_count"] == 1
    assert [finding.issue_id for finding in report.findings] == ["issue-included"]


def test_malformed_metadata_is_counted_as_warning_not_raised(db):
    send_id = _send(db, "issue-bad", {"intro": "Will be overwritten"})
    db.conn.execute(
        "UPDATE newsletter_sends SET metadata = ? WHERE id = ?",
        ("not-json", send_id),
    )
    db.conn.commit()
    _clicks(db, send_id, "issue-bad", [{"url": "https://example.com/anything", "clicks": 5}])

    report = build_newsletter_intro_click_alignment_report(db, days=30, now=NOW)

    assert report.totals["malformed_metadata_count"] == 1
    assert report.findings[0].warnings == (
        "missing_intro_terms",
        "low_intro_click_alignment",
    )


def test_missing_schema_gaps_are_reported_without_generated_content_requirement():
    empty = sqlite3.connect(":memory:")
    empty.row_factory = sqlite3.Row

    missing = build_newsletter_intro_click_alignment_report(empty, now=NOW)
    assert missing.missing_tables == ("newsletter_sends", "newsletter_link_clicks")

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.execute(
        """CREATE TABLE newsletter_sends (
            id INTEGER PRIMARY KEY,
            issue_id TEXT,
            metadata TEXT,
            sent_at TEXT
        )"""
    )
    partial.execute(
        """CREATE TABLE newsletter_link_clicks (
            id INTEGER PRIMARY KEY,
            newsletter_send_id INTEGER,
            link_url TEXT,
            clicks INTEGER,
            fetched_at TEXT
        )"""
    )
    partial.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, metadata, sent_at)
           VALUES (1, 'issue-lite', ?, ?)""",
        (json.dumps({"intro": "Search ranking update"}), NOW.isoformat()),
    )
    partial.execute(
        """INSERT INTO newsletter_link_clicks
           (id, newsletter_send_id, link_url, clicks, fetched_at)
           VALUES (1, 1, 'https://example.com/search-ranking', 5, ?)""",
        (NOW.isoformat(),),
    )
    partial.commit()

    report = build_newsletter_intro_click_alignment_report(partial, now=NOW)
    assert report.missing_tables == ()
    assert report.missing_columns == {}
    assert report.findings[0].issue_id == "issue-lite"


def test_json_text_and_cli_outputs_are_deterministic(db, monkeypatch, capsys):
    send_id = _send(db, "issue-cli", {"preview_text": "Pipeline observability dashboard"})
    _clicks(
        db,
        send_id,
        "issue-cli",
        [{"url": "https://example.com/pipeline-observability-dashboard", "clicks": 5}],
    )

    report = build_newsletter_intro_click_alignment_report(db, days=30, now=NOW)
    payload = json.loads(format_newsletter_intro_click_alignment_json(report))
    text = format_newsletter_intro_click_alignment_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "newsletter_intro_click_alignment"
    assert "Newsletter Intro Click Alignment" in text
    assert "issue-cli" in text

    monkeypatch.setattr(
        newsletter_intro_click_alignment_script,
        "script_context",
        lambda: _script_context(db),
    )
    exit_code = newsletter_intro_click_alignment_script.main(
        ["--days", "30", "--limit", "5", "--min-clicks", "3", "--format", "json"]
    )
    cli_payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert cli_payload["filters"]["min_clicks"] == 3
    assert cli_payload["findings"][0]["issue_id"] == "issue-cli"


def test_invalid_builder_and_cli_numeric_args_return_expected_errors(db, monkeypatch, capsys):
    with pytest.raises(ValueError, match="min_clicks must be positive"):
        build_newsletter_intro_click_alignment_report(db, min_clicks=0)

    monkeypatch.setattr(
        newsletter_intro_click_alignment_script,
        "script_context",
        lambda: _script_context(db),
    )
    assert newsletter_intro_click_alignment_script.main(["--min-clicks", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
