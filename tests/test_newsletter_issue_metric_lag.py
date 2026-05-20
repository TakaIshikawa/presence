"""Tests for newsletter issue metric lag reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.newsletter_issue_metric_lag import (
    build_newsletter_issue_metric_lag_report,
    format_newsletter_issue_metric_lag_json,
    format_newsletter_issue_metric_lag_text,
)


NOW = datetime(2026, 5, 18, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_issue_metric_lag.py"
spec = importlib.util.spec_from_file_location("newsletter_issue_metric_lag_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE newsletter_sends (
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
           );"""
    )
    return conn


def _send(
    conn: sqlite3.Connection,
    send_id: int,
    *,
    sent_at: datetime,
    status: str = "sent",
) -> None:
    conn.execute(
        "INSERT INTO newsletter_sends VALUES (?, ?, ?, ?, ?)",
        (send_id, f"issue-{send_id}", f"Subject {send_id}", status, sent_at.isoformat()),
    )


def _engagement(
    conn: sqlite3.Connection,
    engagement_id: int,
    send_id: int,
    fetched_at: datetime,
) -> None:
    conn.execute(
        "INSERT INTO newsletter_engagement VALUES (?, ?, ?, ?)",
        (engagement_id, send_id, f"issue-{send_id}", fetched_at.isoformat()),
    )


def test_default_report_excludes_ok_and_classifies_metric_gaps():
    conn = _conn()
    _send(conn, 1, sent_at=NOW - timedelta(hours=8))
    _engagement(conn, 1, 1, NOW - timedelta(hours=7))
    _engagement(conn, 2, 1, NOW - timedelta(hours=1))
    _send(conn, 2, sent_at=NOW - timedelta(hours=12))
    _engagement(conn, 3, 2, NOW - timedelta(hours=3))
    _send(conn, 3, sent_at=NOW - timedelta(hours=48))
    _engagement(conn, 4, 3, NOW - timedelta(hours=47))
    _engagement(conn, 5, 3, NOW - timedelta(hours=30))
    _send(conn, 4, sent_at=NOW - timedelta(hours=2))
    conn.commit()

    report = build_newsletter_issue_metric_lag_report(
        conn,
        first_fetch_sla_hours=6,
        max_refresh_age_hours=24,
        now=NOW,
    )

    assert report["summary"]["sent_issue_count"] == 4
    assert report["summary"]["reported_issue_count"] == 3
    assert report["summary"]["ok_issue_count"] == 1
    assert report["summary"]["by_gap_type"] == {
        "first_fetch_sla_breach": 1,
        "no_metrics": 1,
        "stale_refresh": 1,
    }
    by_send = {item["newsletter_send_id"]: item for item in report["issues"]}
    assert by_send[2]["gap_type"] == "first_fetch_sla_breach"
    assert by_send[2]["first_fetch_lag_hours"] == 9.0
    assert by_send[3]["gap_type"] == "stale_refresh"
    assert by_send[3]["refresh_age_hours"] == 30.0
    assert by_send[4]["gap_type"] == "no_metrics"


def test_include_ok_returns_timely_fresh_metrics():
    conn = _conn()
    _send(conn, 1, sent_at=NOW - timedelta(hours=4))
    _engagement(conn, 1, 1, NOW - timedelta(hours=3))
    conn.commit()

    default = build_newsletter_issue_metric_lag_report(conn, now=NOW)
    include_ok = build_newsletter_issue_metric_lag_report(conn, include_ok=True, now=NOW)

    assert default["issues"] == []
    assert include_ok["issues"][0]["gap_type"] == "ok"
    assert include_ok["issues"][0]["first_fetched_at"] == (NOW - timedelta(hours=3)).isoformat()


def test_issue_id_fallback_and_schema_gaps():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE newsletter_sends (
               id INTEGER PRIMARY KEY,
               issue_id TEXT,
               sent_at TEXT
           );
           CREATE TABLE newsletter_engagement (
               id INTEGER PRIMARY KEY,
               issue_id TEXT,
               fetched_at TEXT
           );"""
    )
    conn.execute("INSERT INTO newsletter_sends VALUES (1, 'issue-a', ?)", ((NOW - timedelta(hours=10)).isoformat(),))
    conn.execute("INSERT INTO newsletter_engagement VALUES (1, 'issue-a', ?)", ((NOW - timedelta(hours=2)).isoformat(),))
    conn.commit()

    report = build_newsletter_issue_metric_lag_report(conn, first_fetch_sla_hours=6, now=NOW)
    assert report["issues"][0]["issue_id"] == "issue-a"
    assert report["issues"][0]["gap_type"] == "first_fetch_sla_breach"

    missing = build_newsletter_issue_metric_lag_report(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["newsletter_sends", "newsletter_engagement"]
    assert missing["issues"] == []


def test_formatters_cli_and_argument_validation(tmp_path, capsys):
    conn = _conn()
    _send(conn, 1, sent_at=NOW - timedelta(hours=8))
    db_path = tmp_path / "newsletter.sqlite"
    conn.commit()
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path), "--first-fetch-sla-hours", "4"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "newsletter_issue_metric_lag"
    assert payload["issues"][0]["gap_type"] == "no_metrics"

    assert script.main(["--db", str(db_path), "--format", "text", "--include-ok"]) == 0
    assert "Newsletter Issue Metric Lag" in capsys.readouterr().out

    assert script.main(["--db", str(db_path), "--max-refresh-age-hours", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_json_formatter_and_invalid_thresholds():
    conn = _conn()
    report = build_newsletter_issue_metric_lag_report(conn, now=NOW)
    assert json.loads(format_newsletter_issue_metric_lag_json(report))["artifact_type"] == "newsletter_issue_metric_lag"
    assert "No newsletter issue metric lag gaps found" in format_newsletter_issue_metric_lag_text(report)
    with pytest.raises(ValueError, match="first_fetch_sla_hours must be positive"):
        build_newsletter_issue_metric_lag_report(conn, first_fetch_sla_hours=0)
    with pytest.raises(ValueError, match="max_refresh_age_hours must be positive"):
        build_newsletter_issue_metric_lag_report(conn, max_refresh_age_hours=0)
