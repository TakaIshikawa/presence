"""Tests for publish queue retry backoff effectiveness reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.publish_queue_retry_backoff_effectiveness import (
    build_publish_queue_retry_backoff_effectiveness_report_from_db,
    format_publish_queue_retry_backoff_effectiveness_json,
    format_publish_queue_retry_backoff_effectiveness_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_queue_retry_backoff_effectiveness.py"
spec = importlib.util.spec_from_file_location("publish_queue_retry_backoff_effectiveness_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE publish_queue (
             id INTEGER PRIMARY KEY,
             platform TEXT,
             status TEXT,
             error_category TEXT,
             created_at TEXT,
             scheduled_at TEXT
           );"""
    )
    return conn


def test_report_flags_backoff_violation_categories():
    conn = _conn()
    conn.executemany(
        """INSERT INTO publish_queue
           (id, platform, status, error_category, created_at, scheduled_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [
            (1, "x", "failed", "rate_limit", "2026-05-20T10:00:00+00:00", "2026-05-20T10:01:00+00:00"),
            (2, "x", "failed", "timeout", "2026-05-20T00:00:00+00:00", "2026-05-22T00:00:00+00:00"),
            (3, "bluesky", "failed", "auth", "2026-05-18T00:00:00+00:00", None),
            (4, "bluesky", "queued", "timeout", "2026-05-20T09:00:00+00:00", "2026-05-20T10:00:00+00:00"),
            (5, "x", "published", "none", "2026-05-20T09:00:00+00:00", "2026-05-20T09:01:00+00:00"),
        ],
    )

    report = build_publish_queue_retry_backoff_effectiveness_report_from_db(
        conn,
        now=NOW,
        min_backoff_minutes=5,
        max_backoff_hours=24,
        stale_failed_hours=24,
    )

    assert report["artifact_type"] == "publish_queue_retry_backoff_effectiveness"
    assert report["summary"]["retry_rows_scanned"] == 4
    assert report["summary"]["by_violation_type"] == {
        "excessive_delay": 1,
        "immediate_retry": 1,
        "overdue_retry": 1,
        "stale_failed_unscheduled": 1,
    }
    assert {item["violation_type"] for item in report["violations"]} == {
        "immediate_retry",
        "excessive_delay",
        "stale_failed_unscheduled",
        "overdue_retry",
    }
    assert any(group["platform"] == "x" and group["error_category"] == "rate_limit" for group in report["grouped_violations"])


def test_formatters_cli_schema_and_validation(tmp_path, capsys):
    conn = _conn()
    conn.execute(
        """INSERT INTO publish_queue
           (id, platform, status, error_category, created_at, scheduled_at)
           VALUES (1, 'x', 'failed', 'rate_limit', '2026-05-20T10:00:00+00:00', '2026-05-20T10:01:00+00:00')"""
    )
    report = build_publish_queue_retry_backoff_effectiveness_report_from_db(conn, now=NOW)
    assert json.loads(format_publish_queue_retry_backoff_effectiveness_json(report))["summary"]["violation_count"] == 1
    assert "violations=1" in format_publish_queue_retry_backoff_effectiveness_text(report)

    db_path = tmp_path / "queue.sqlite"
    conn.commit()
    dest = sqlite3.connect(db_path)
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(db_path), "--format", "json", "--now", NOW.isoformat()]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["violation_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Publish Queue Retry Backoff Effectiveness" in capsys.readouterr().out

    missing = sqlite3.connect(":memory:")
    assert build_publish_queue_retry_backoff_effectiveness_report_from_db(missing)["missing_tables"] == ["publish_queue"]
    with pytest.raises(ValueError, match="limit must be positive"):
        build_publish_queue_retry_backoff_effectiveness_report_from_db(conn, limit=0)
