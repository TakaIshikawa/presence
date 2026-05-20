"""Tests for publish_queue error category trends."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sqlite3

from evaluation.publish_queue_error_category_trends import (
    build_publish_queue_error_category_trends_report_from_db,
    format_publish_queue_error_category_trends_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_queue_error_category_trends.py"
spec = importlib.util.spec_from_file_location("publish_queue_error_category_trends_script", SCRIPT_PATH)
publish_queue_error_category_trends_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publish_queue_error_category_trends_script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE publish_queue (
            id INTEGER PRIMARY KEY,
            platform TEXT,
            status TEXT,
            error_category TEXT,
            error_message TEXT,
            created_at TEXT,
            scheduled_at TEXT
        );
        """
    )
    return conn


def test_report_groups_failed_rows_by_category_platform_and_examples():
    conn = _conn()
    conn.executemany(
        "INSERT INTO publish_queue VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (1, "x", "failed", "Auth", "token expired", "2026-05-20T10:00:00+00:00", None),
            (2, "x", "failed", "auth", "401", "2026-05-20T09:00:00+00:00", None),
            (3, "bluesky", "failed", "", "blank category", "2026-05-20T08:00:00+00:00", None),
            (4, "x", "posted", "auth", "ok", "2026-05-20T07:00:00+00:00", None),
            (5, "x", "failed", "old", "old", "2026-05-01T07:00:00+00:00", None),
        ],
    )

    report = build_publish_queue_error_category_trends_report_from_db(
        conn, now=NOW, lookback_days=7, min_count=1, limit=10
    )

    assert report["artifact_type"] == "publish_queue_error_category_trends"
    assert report["summary"]["failed_rows_scanned"] == 3
    assert report["category_buckets"][0]["error_category"] == "auth"
    assert report["category_buckets"][0]["example_queue_ids"] == [1, 2]
    assert any(bucket["error_category"] == "unknown" for bucket in report["category_buckets"])
    assert "Category buckets" in format_publish_queue_error_category_trends_text(report)


def test_platform_filter_limits_rows():
    conn = _conn()
    conn.executemany(
        "INSERT INTO publish_queue VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (1, "x", "failed", "auth", "token", "2026-05-20T10:00:00+00:00", None),
            (2, "bluesky", "failed", "auth", "token", "2026-05-20T10:00:00+00:00", None),
        ],
    )
    report = build_publish_queue_error_category_trends_report_from_db(
        conn, now=NOW, lookback_days=7, platform="x"
    )
    assert report["summary"]["failed_rows_scanned"] == 1
    assert report["platform_buckets"] == [{"platform": "x", "count": 1}]


def test_missing_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_publish_queue_error_category_trends_report_from_db(conn, now=NOW)
    assert report["missing_tables"] == ["publish_queue"]


def test_cli_outputs_json(tmp_path, capsys):
    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE publish_queue (id INTEGER PRIMARY KEY, platform TEXT, status TEXT, error_category TEXT, error_message TEXT, created_at TEXT);
        INSERT INTO publish_queue VALUES (1, 'x', 'failed', 'auth', 'token', '2026-05-20T10:00:00+00:00');
        """
    )
    conn.close()
    assert publish_queue_error_category_trends_script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert '"artifact_type": "publish_queue_error_category_trends"' in capsys.readouterr().out
