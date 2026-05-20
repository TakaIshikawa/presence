"""Tests for publication queue schedule drift reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.publication_queue_schedule_drift import (
    build_publication_queue_schedule_drift_report,
    build_publication_queue_schedule_drift_report_from_db,
    format_publication_queue_schedule_drift_json,
    format_publication_queue_schedule_drift_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_queue_schedule_drift.py"
spec = importlib.util.spec_from_file_location("publication_queue_schedule_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _ts(hours_ago: int) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE publish_queue (
               id INTEGER PRIMARY KEY,
               content_id INTEGER,
               platform TEXT,
               status TEXT,
               scheduled_at TEXT,
               published_at TEXT
           );
           CREATE TABLE publication_attempts (
               id INTEGER PRIMARY KEY,
               queue_id INTEGER,
               status TEXT
           );"""
    )
    return conn


def test_builder_flags_only_active_rows_past_grace_without_success():
    report = build_publication_queue_schedule_drift_report(
        [
            {"queue_id": 1, "content_id": 10, "platform": "x", "status": "queued", "scheduled_at": _ts(8)},
            {"queue_id": 2, "content_id": 11, "platform": "x", "status": "draft", "scheduled_at": _ts(8)},
            {"queue_id": 3, "content_id": 12, "platform": "x", "status": "queued", "scheduled_at": _ts(1)},
            {"queue_id": 4, "content_id": 13, "platform": "x", "status": "queued", "scheduled_at": _ts(8), "published_at": _ts(1)},
            {"queue_id": 5, "content_id": 14, "platform": "x", "status": "queued", "scheduled_at": _ts(8), "has_successful_attempt": 1},
        ],
        grace_hours=2,
        now=NOW,
    )

    assert report["artifact_type"] == "publication_queue_schedule_drift"
    assert report["summary"]["drift_count"] == 1
    assert report["drift_items"][0]["queue_id"] == 1
    assert report["bucket_summary"] == [{"platform": "x", "status": "queued", "drift_bucket": "6_24h", "count": 1}]


def test_db_loader_suppresses_published_and_successful_attempts():
    conn = _conn()
    conn.executemany(
        "INSERT INTO publish_queue VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, 10, "x", "queued", _ts(10), None),
            (2, 11, "x", "queued", _ts(10), _ts(1)),
            (3, 12, "bluesky", "held", _ts(30), None),
        ],
    )
    conn.execute("INSERT INTO publication_attempts (queue_id, status) VALUES (3, 'success')")

    report = build_publication_queue_schedule_drift_report_from_db(conn, grace_hours=2, now=NOW)

    assert [item["queue_id"] for item in report["drift_items"]] == [1]
    assert report["summary"]["rows_scanned"] == 3


def test_cli_json_text_filters_and_validation(tmp_path, capsys):
    conn = _conn()
    conn.execute("INSERT INTO publish_queue VALUES (1, 10, 'x', 'queued', ?, NULL)", (_ts(10),))
    conn.execute("INSERT INTO publish_queue VALUES (2, 11, 'blog', 'queued', ?, NULL)", (_ts(10),))
    conn.commit()
    db_path = tmp_path / "drift.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path), "--platform", "x", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"]["drift_count"] == 1

    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Publication Queue Schedule Drift" in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_formatters_and_invalid_thresholds():
    report = build_publication_queue_schedule_drift_report([], now=NOW)
    assert json.loads(format_publication_queue_schedule_drift_json(report))["artifact_type"] == "publication_queue_schedule_drift"
    assert "No publication queue schedule drift found" in format_publication_queue_schedule_drift_text(report)
    with pytest.raises(ValueError, match="grace_hours must be non-negative"):
        build_publication_queue_schedule_drift_report([], grace_hours=-1)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_publication_queue_schedule_drift_report([], limit=0)
