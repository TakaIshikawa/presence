"""Tests for content publication retry schedule drift reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.content_publication_retry_schedule_drift import (
    build_content_publication_retry_schedule_drift_report,
    build_content_publication_retry_schedule_drift_report_from_db,
    format_content_publication_retry_schedule_drift_json,
    format_content_publication_retry_schedule_drift_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_publication_retry_schedule_drift.py"
spec = importlib.util.spec_from_file_location("content_publication_retry_schedule_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _ts(hours_ago: int) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE content_publications (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            status TEXT,
            attempt_count INTEGER,
            next_retry_at TEXT,
            last_error_at TEXT,
            error_category TEXT
        )"""
    )
    return conn


def test_builder_detects_all_retry_schedule_issues():
    report = build_content_publication_retry_schedule_drift_report(
        [
            {"id": 1, "content_id": 10, "platform": "x", "status": "failed", "attempt_count": 1, "error_category": "rate_limit"},
            {"id": 2, "content_id": 11, "platform": "x", "status": "failed", "attempt_count": 2, "next_retry_at": _ts(30), "error_category": "api"},
            {"id": 3, "content_id": 12, "platform": "blog", "status": "published", "next_retry_at": _ts(1), "error_category": "none"},
            {"id": 4, "content_id": 13, "platform": "x", "status": "failed", "attempt_count": 1, "next_retry_at": _ts(4), "last_error_at": _ts(1), "error_category": "api"},
            {"id": 5, "content_id": 14, "platform": "x", "status": "failed", "attempt_count": 0},
        ],
        overdue_hours=24,
        now=NOW,
    )

    assert report["artifact_type"] == "content_publication_retry_schedule_drift"
    assert report["summary"]["by_issue_type"] == {
        "inverted_error_retry": 1,
        "missing_retry": 1,
        "overdue_retry": 1,
        "stale_retry_on_published": 1,
    }
    assert report["summary"]["drift_count"] == 4


def test_db_loader_and_filters():
    conn = _conn()
    conn.executemany(
        "INSERT INTO content_publications VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 10, "x", "failed", 1, None, _ts(1), "rate_limit"),
            (2, 11, "blog", "failed", 1, None, _ts(1), "api"),
        ],
    )
    report = build_content_publication_retry_schedule_drift_report_from_db(conn, platform="x", status="failed", now=NOW)
    assert [item["publication_id"] for item in report["drift_items"]] == [1]


def test_cli_json_text_and_validation(tmp_path, capsys):
    conn = _conn()
    conn.execute("INSERT INTO content_publications VALUES (1, 10, 'x', 'failed', 1, NULL, ?, 'api')", (_ts(1),))
    conn.commit()
    db_path = tmp_path / "retry.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path), "--format", "json", "--platform", "x"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["drift_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Content Publication Retry Schedule Drift" in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--overdue-hours", "-1"]) == 2
    assert "value must be non-negative" in capsys.readouterr().err


def test_formatters_and_invalid_thresholds():
    report = build_content_publication_retry_schedule_drift_report([], now=NOW)
    assert json.loads(format_content_publication_retry_schedule_drift_json(report))["artifact_type"] == "content_publication_retry_schedule_drift"
    assert "No content publication retry schedule drift found" in format_content_publication_retry_schedule_drift_text(report)
    with pytest.raises(ValueError, match="overdue_hours must be non-negative"):
        build_content_publication_retry_schedule_drift_report([], overdue_hours=-1)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_content_publication_retry_schedule_drift_report([], limit=0)
