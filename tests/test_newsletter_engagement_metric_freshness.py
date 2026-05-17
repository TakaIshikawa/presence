"""Tests for newsletter engagement metric freshness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.newsletter_engagement_metric_freshness import (
    build_newsletter_engagement_metric_freshness_report,
    format_newsletter_engagement_metric_freshness_json,
    format_newsletter_engagement_metric_freshness_text,
)


NOW = datetime(2026, 5, 18, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_engagement_metric_freshness.py"
spec = importlib.util.spec_from_file_location("newsletter_engagement_metric_freshness_script", SCRIPT_PATH)
newsletter_engagement_metric_freshness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_engagement_metric_freshness_script)


@contextmanager
def _script_context(conn):
    yield SimpleNamespace(), conn


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE newsletter_sends (
            id INTEGER PRIMARY KEY, issue_id TEXT, subject TEXT, status TEXT, sent_at TEXT
        );
        CREATE TABLE newsletter_engagement (
            id INTEGER PRIMARY KEY, newsletter_send_id INTEGER, fetched_at TEXT, opens INTEGER, clicks INTEGER
        );"""
    )
    return conn


def _send(conn: sqlite3.Connection, send_id: int, sent_at: datetime, fetched_at: datetime | None = None) -> None:
    conn.execute("INSERT INTO newsletter_sends VALUES (?, ?, ?, 'sent', ?)", (send_id, f"i-{send_id}", f"Subject {send_id}", sent_at.isoformat()))
    if fetched_at:
        conn.execute("INSERT INTO newsletter_engagement (newsletter_send_id, fetched_at) VALUES (?, ?)", (send_id, fetched_at.isoformat()))
    conn.commit()


def test_fresh_stale_missing_and_old_sends():
    conn = _conn()
    _send(conn, 1, NOW - timedelta(days=1), NOW - timedelta(hours=2))
    _send(conn, 2, NOW - timedelta(days=2), NOW - timedelta(hours=50))
    _send(conn, 3, NOW - timedelta(days=3), None)
    _send(conn, 4, NOW - timedelta(days=40), None)

    report = build_newsletter_engagement_metric_freshness_report(conn, now=NOW, days=30, stale_hours=24)

    assert report["totals"] == {"sent_issue_count": 3, "fresh_metric_count": 1, "stale_issue_count": 1, "missing_metric_count": 1}
    assert report["stale_issues"][0]["newsletter_send_id"] == 2
    assert report["missing_metric_issues"][0]["newsletter_send_id"] == 3


def test_json_text_cli_and_schema_gaps(monkeypatch, capsys):
    conn = _conn()
    _send(conn, 1, NOW - timedelta(days=1), NOW - timedelta(hours=2))
    report = build_newsletter_engagement_metric_freshness_report(conn, now=NOW)

    assert json.loads(format_newsletter_engagement_metric_freshness_json(report))["artifact_type"] == "newsletter_engagement_metric_freshness"
    assert "Newsletter Engagement Metric Freshness" in format_newsletter_engagement_metric_freshness_text(report)
    monkeypatch.setattr(newsletter_engagement_metric_freshness_script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        newsletter_engagement_metric_freshness_script,
        "build_newsletter_engagement_metric_freshness_report",
        lambda db, **kwargs: build_newsletter_engagement_metric_freshness_report(db, now=NOW, **kwargs),
    )
    assert newsletter_engagement_metric_freshness_script.main(["--format", "text"]) == 0
    assert "Totals: sent=1" in capsys.readouterr().out

    missing = build_newsletter_engagement_metric_freshness_report(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["newsletter_sends", "newsletter_engagement"]
