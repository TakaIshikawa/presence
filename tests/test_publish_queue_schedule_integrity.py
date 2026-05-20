"""Tests for publish queue schedule integrity reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.publish_queue_schedule_integrity import (
    build_publish_queue_schedule_integrity_report,
    build_publish_queue_schedule_integrity_report_from_db,
    format_publish_queue_schedule_integrity_json,
    format_publish_queue_schedule_integrity_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_queue_schedule_integrity.py"
spec = importlib.util.spec_from_file_location("publish_queue_schedule_integrity_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _ts(hours_ago: int) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE generated_content (
               id INTEGER PRIMARY KEY,
               status TEXT
           );
           CREATE TABLE publish_queue (
               id INTEGER PRIMARY KEY,
               content_id INTEGER,
               platform TEXT,
               status TEXT,
               scheduled_at TEXT,
               published_at TEXT,
               error TEXT,
               error_category TEXT,
               hold_reason TEXT
           );"""
    )
    return conn


def test_builder_groups_schedule_and_state_integrity_findings():
    report = build_publish_queue_schedule_integrity_report(
        [
            {"queue_id": 1, "content_id": 10, "platform": "x", "status": "queued", "scheduled_at": _ts(8)},
            {"queue_id": 2, "platform": "x", "status": "published"},
            {"queue_id": 3, "platform": "blog", "status": "failed"},
            {"queue_id": 4, "platform": "blog", "status": "held"},
            {"queue_id": 5, "platform": "x", "status": "queued", "generated_content_status": "published"},
        ],
        grace_hours=2,
        now=NOW,
    )

    assert report["artifact_type"] == "publish_queue_schedule_integrity"
    assert report["summary"]["by_issue_type"] == {
        "failed_missing_error": 1,
        "held_missing_hold_reason": 1,
        "published_missing_published_at": 1,
        "queued_content_published": 1,
        "queued_scheduled_in_past": 1,
    }
    assert {"issue_type": "queued_scheduled_in_past", "platform": "x", "count": 1} in report["groups"]


def test_db_loader_joins_generated_content_and_reports_missing_tables():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (10, 'abandoned')")
    conn.execute("INSERT INTO publish_queue VALUES (1, 10, 'x', 'queued', ?, NULL, NULL, NULL, NULL)", (_ts(4),))

    report = build_publish_queue_schedule_integrity_report_from_db(conn, grace_hours=2, now=NOW)

    assert report["missing_tables"] == []
    assert report["summary"]["by_issue_type"]["queued_content_abandoned"] == 1
    assert report["summary"]["by_issue_type"]["queued_scheduled_in_past"] == 1

    missing = build_publish_queue_schedule_integrity_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["generated_content", "publish_queue"]
    assert missing["findings"] == []


def test_cli_json_text_and_argument_validation(tmp_path, capsys):
    conn = _conn()
    conn.execute("INSERT INTO publish_queue VALUES (1, NULL, 'x', 'published', NULL, NULL, NULL, NULL, NULL)")
    conn.commit()
    db_path = tmp_path / "queue.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text", "--limit", "1"]) == 0
    assert "Publish Queue Schedule Integrity" in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--grace-hours", "-1"]) == 2
    assert "value must be non-negative" in capsys.readouterr().err
    assert script.main(["--db", str(db_path), "--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_formatters_and_invalid_thresholds():
    report = build_publish_queue_schedule_integrity_report([], now=NOW)
    assert json.loads(format_publish_queue_schedule_integrity_json(report))["artifact_type"] == "publish_queue_schedule_integrity"
    assert "No publish queue schedule integrity issues found" in format_publish_queue_schedule_integrity_text(report)
    with pytest.raises(ValueError, match="grace_hours must be non-negative"):
        build_publish_queue_schedule_integrity_report([], grace_hours=-1)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_publish_queue_schedule_integrity_report([], limit=0)
