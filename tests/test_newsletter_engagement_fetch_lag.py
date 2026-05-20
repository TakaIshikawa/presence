"""Tests for newsletter engagement fetch lag reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path
import sqlite3

from evaluation.newsletter_engagement_fetch_lag import (
    build_newsletter_engagement_fetch_lag_report,
    build_newsletter_engagement_fetch_lag_report_from_db,
    format_newsletter_engagement_fetch_lag_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_engagement_fetch_lag.py"
spec = importlib.util.spec_from_file_location("newsletter_engagement_fetch_lag_script", SCRIPT_PATH)
newsletter_engagement_fetch_lag_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_engagement_fetch_lag_script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE newsletter_sends (
            id INTEGER PRIMARY KEY,
            issue_id TEXT,
            subject TEXT,
            status TEXT,
            sent_at TEXT
        );
        CREATE TABLE newsletter_engagement (
            id INTEGER PRIMARY KEY,
            newsletter_send_id INTEGER,
            issue_id TEXT,
            fetched_at TEXT
        );
        """
    )
    return conn


def test_builder_flags_missing_stale_and_orphan_with_limit():
    rows = [
        {
            "row_type": "send",
            "newsletter_send_id": 2,
            "issue_id": "stale",
            "status": "sent",
            "sent_at": (NOW - timedelta(days=3)).isoformat(),
            "latest_fetched_at": (NOW - timedelta(hours=72)).isoformat(),
        },
        {
            "row_type": "send",
            "newsletter_send_id": 1,
            "issue_id": "missing",
            "status": "sent",
            "sent_at": (NOW - timedelta(days=2)).isoformat(),
            "latest_fetched_at": None,
        },
        {
            "row_type": "send",
            "newsletter_send_id": 3,
            "issue_id": "fresh",
            "status": "sent",
            "sent_at": (NOW - timedelta(hours=2)).isoformat(),
            "latest_fetched_at": None,
        },
        {
            "row_type": "engagement_without_send",
            "engagement_id": 9,
            "newsletter_send_id": 99,
            "issue_id": "orphan",
            "latest_fetched_at": (NOW - timedelta(hours=1)).isoformat(),
        },
    ]

    report = build_newsletter_engagement_fetch_lag_report(
        rows,
        now=NOW,
        grace_hours=24,
        stale_after_hours=48,
        limit=2,
    )

    assert report["artifact_type"] == "newsletter_engagement_fetch_lag"
    assert report["summary"]["issue_count"] == 3
    assert [item["issue_type"] for item in report["issue_items"]] == [
        "missing_engagement_metrics",
        "stale_engagement_metrics",
    ]


def test_db_loader_joins_by_send_id_and_falls_back_to_issue_id():
    conn = _conn()
    conn.executemany(
        "INSERT INTO newsletter_sends VALUES (?, ?, ?, ?, ?)",
        [
            (1, "i-1", "Missing", "sent", (NOW - timedelta(days=2)).isoformat()),
            (2, "i-2", "Stale", "sent", (NOW - timedelta(days=4)).isoformat()),
            (3, "i-3", "Issue fallback", "sent", (NOW - timedelta(days=4)).isoformat()),
            (4, "i-4", "Draft", "draft", (NOW - timedelta(days=4)).isoformat()),
        ],
    )
    conn.executemany(
        "INSERT INTO newsletter_engagement VALUES (?, ?, ?, ?)",
        [
            (10, 2, "i-2", (NOW - timedelta(hours=80)).isoformat()),
            (11, None, "i-3", (NOW - timedelta(hours=1)).isoformat()),
            (12, 99, "missing-send", (NOW - timedelta(hours=1)).isoformat()),
        ],
    )

    report = build_newsletter_engagement_fetch_lag_report_from_db(
        conn,
        now=NOW,
        grace_hours=24,
        stale_after_hours=48,
        limit=10,
    )

    assert [item["issue_type"] for item in report["issue_items"]] == [
        "missing_engagement_metrics",
        "stale_engagement_metrics",
        "engagement_without_send",
    ]
    assert [item["newsletter_send_id"] for item in report["issue_items"]] == [1, 2, 99]
    assert "Newsletter Engagement Fetch Lag" in format_newsletter_engagement_fetch_lag_text(report)


def test_missing_tables_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_newsletter_engagement_fetch_lag_report_from_db(conn, now=NOW)
    assert report["missing_tables"] == ["newsletter_sends", "newsletter_engagement"]
    assert report["summary"]["send_rows_scanned"] == 0


def test_cli_outputs_json_and_text(tmp_path, capsys):
    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE newsletter_sends (id INTEGER PRIMARY KEY, issue_id TEXT, status TEXT, sent_at TEXT);
        CREATE TABLE newsletter_engagement (id INTEGER PRIMARY KEY, newsletter_send_id INTEGER, issue_id TEXT, fetched_at TEXT);
        INSERT INTO newsletter_sends VALUES (1, 'i-1', 'sent', '2026-05-18T00:00:00+00:00');
        """
    )
    conn.close()

    assert newsletter_engagement_fetch_lag_script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert '"artifact_type": "newsletter_engagement_fetch_lag"' in capsys.readouterr().out
    assert newsletter_engagement_fetch_lag_script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Newsletter Engagement Fetch Lag" in capsys.readouterr().out
