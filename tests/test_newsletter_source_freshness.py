"""Tests for newsletter source freshness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.newsletter_source_freshness import (
    build_newsletter_source_freshness_report,
    format_newsletter_source_freshness_json,
    format_newsletter_source_freshness_text,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_source_freshness.py"
spec = importlib.util.spec_from_file_location("newsletter_source_freshness_script", SCRIPT_PATH)
newsletter_source_freshness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_source_freshness_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _send(db, issue_id: str, sources=None, metadata=None) -> int:
    cursor = db.conn.execute(
        """INSERT INTO newsletter_sends (issue_id, subject, source_content_ids, metadata, sent_at)
           VALUES (?, 'subject', ?, ?, ?)""",
        (issue_id, json.dumps(sources or []), json.dumps(metadata or {}), NOW.isoformat()),
    )
    db.conn.commit()
    return cursor.lastrowid


def _content(db, days_ago: int) -> int:
    content_id = db.insert_generated_content("x_post", [], [], "source", 7, "ok")
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        ((NOW - timedelta(days=days_ago)).isoformat(), content_id),
    )
    db.conn.commit()
    return content_id


def test_missing_source_payloads_are_source_light(db):
    _send(db, "issue-1")

    report = build_newsletter_source_freshness_report(db, now=NOW)

    assert report.issues[0].freshness_status == "source-light"
    assert "source_light" in report.issues[0].reason_codes


def test_alias_reuse_is_detected_across_metadata_sources(db):
    metadata = {"sources": [{"url": "https://Example.com/Post/", "published_at": NOW.isoformat()}]}
    _send(db, "issue-1", metadata=metadata)
    _send(db, "issue-2", metadata=metadata)

    report = build_newsletter_source_freshness_report(db, reuse_threshold=2, now=NOW)

    assert report.issues[0].reused_source_ids == ("example.com/post",)
    assert "reused_sources" in report.issues[0].reason_codes


def test_stale_sources_classify_issue_as_stale(db):
    old = _content(db, 80)
    older = _content(db, 90)
    _send(db, "issue-1", sources=[old, older])

    report = build_newsletter_source_freshness_report(db, max_source_age_days=30, now=NOW)

    assert report.issues[0].freshness_status == "stale"
    assert report.issues[0].newest_source_age_days == 80


def test_mixed_fresh_stale_issues_and_formatter_cli_json(db, monkeypatch, capsys):
    fresh = _content(db, 2)
    stale = _content(db, 60)
    _send(db, "issue-1", sources=[fresh, stale])
    monkeypatch.setattr(newsletter_source_freshness_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        newsletter_source_freshness_script,
        "build_newsletter_source_freshness_report",
        lambda db, **kwargs: build_newsletter_source_freshness_report(db, now=NOW, **kwargs),
    )

    report = build_newsletter_source_freshness_report(db, max_source_age_days=30, now=NOW)
    payload = json.loads(format_newsletter_source_freshness_json(report))
    text = format_newsletter_source_freshness_text(report)
    exit_code = newsletter_source_freshness_script.main(["--format", "json", "--max-source-age-days", "30"])
    cli_payload = json.loads(capsys.readouterr().out)

    assert payload["issues"][0]["freshness_status"] == "aging"
    assert "Newsletter Source Freshness" in text
    assert cli_payload["issue_count"] == 1
    assert exit_code == 0
