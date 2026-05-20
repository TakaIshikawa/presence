"""Tests for publish queue slot contention reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.publish_queue_slot_contention import (
    build_publish_queue_slot_contention_report,
    build_publish_queue_slot_contention_report_from_db,
    format_publish_queue_slot_contention_json,
    format_publish_queue_slot_contention_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_queue_slot_contention.py"
spec = importlib.util.spec_from_file_location("publish_queue_slot_contention_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE publish_queue (
               id INTEGER PRIMARY KEY,
               content_id INTEGER,
               platform TEXT,
               status TEXT,
               scheduled_at TEXT
           )"""
    )
    return conn


def _insert(
    conn: sqlite3.Connection,
    queue_id: int,
    *,
    content_id: int | None = None,
    platform: str = "x",
    status: str = "queued",
    scheduled_at: datetime | None = None,
) -> None:
    conn.execute(
        "INSERT INTO publish_queue VALUES (?, ?, ?, ?, ?)",
        (
            queue_id,
            content_id if content_id is not None else queue_id,
            platform,
            status,
            (scheduled_at or (NOW + timedelta(minutes=10))).isoformat(),
        ),
    )


def test_report_groups_by_platform_and_bucket_counts_statuses():
    conn = _conn()
    _insert(conn, 1, content_id=10, platform="x", status="queued", scheduled_at=NOW + timedelta(minutes=10))
    _insert(conn, 2, content_id=10, platform="x", status="held", scheduled_at=NOW + timedelta(minutes=40))
    _insert(conn, 3, content_id=11, platform="x", status="failed", scheduled_at=NOW + timedelta(minutes=55))
    _insert(conn, 4, content_id=12, platform="bluesky", status="queued", scheduled_at=NOW + timedelta(minutes=20))
    _insert(conn, 5, platform="x", status="published", scheduled_at=NOW + timedelta(minutes=30))

    report = build_publish_queue_slot_contention_report_from_db(
        conn,
        bucket_size_minutes=60,
        lookahead_hours=4,
        max_posts_per_bucket=2,
        now=NOW,
    )

    assert report["summary"]["rows_scanned"] == 4
    buckets = {(item["bucket_start"], item["platform"]): item for item in report["buckets"]}
    bucket_start = NOW.isoformat()
    assert buckets[(bucket_start, "x")] == {
        "bucket_start": bucket_start,
        "platform": "x",
        "queued_count": 1,
        "held_count": 1,
        "failed_count": 1,
        "distinct_content_count": 2,
        "severity": "overloaded",
    }
    assert buckets[(bucket_start, "bluesky")]["queued_count"] == 1
    assert buckets[(bucket_start, "bluesky")]["severity"] == "ok"


def test_threshold_severity_and_deterministic_sorting():
    rows = [
        {"content_id": 1, "platform": "x", "status": "queued", "scheduled_at": (NOW + timedelta(minutes=75)).isoformat()},
        {"content_id": 2, "platform": "x", "status": "held", "scheduled_at": (NOW + timedelta(minutes=70)).isoformat()},
        {"content_id": 3, "platform": "x", "status": "failed", "scheduled_at": (NOW + timedelta(minutes=80)).isoformat()},
        {"content_id": 4, "platform": "x", "status": "queued", "scheduled_at": (NOW + timedelta(minutes=85)).isoformat()},
        {"content_id": 5, "platform": "bluesky", "status": "held", "scheduled_at": (NOW + timedelta(minutes=10)).isoformat()},
    ]

    report = build_publish_queue_slot_contention_report(
        rows,
        bucket_size_minutes=60,
        max_posts_per_bucket=2,
        now=NOW,
    )

    assert [item["platform"] for item in report["buckets"]] == ["bluesky", "x"]
    assert report["buckets"][1]["severity"] == "critical"
    assert report["summary"]["overloaded_bucket_count"] == 1


def test_empty_database_and_missing_table_behavior():
    conn = _conn()
    report = build_publish_queue_slot_contention_report_from_db(conn, now=NOW)
    assert report["buckets"] == []
    assert report["summary"]["bucket_count"] == 0
    assert "No publish queue slot contention found." in format_publish_queue_slot_contention_text(report)

    missing = build_publish_queue_slot_contention_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["publish_queue"]
    assert missing["buckets"] == []


def test_formatters_cli_and_argument_validation(tmp_path, capsys):
    conn = _conn()
    _insert(conn, 1, platform="x", status="queued", scheduled_at=NOW + timedelta(minutes=15))
    conn.commit()
    db_path = tmp_path / "queue.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path), "--bucket-size-minutes", "30", "--lookahead-hours", "2"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "publish_queue_slot_contention"
    assert payload["filters"]["bucket_size_minutes"] == 30

    assert script.main(["--db", str(db_path), "--format", "text", "--max-posts-per-bucket", "1"]) == 0
    assert "Publish Queue Slot Contention" in capsys.readouterr().out

    assert script.main(["--db", str(db_path), "--lookahead-hours", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_json_formatter_and_invalid_thresholds():
    report = build_publish_queue_slot_contention_report([], now=NOW)
    assert json.loads(format_publish_queue_slot_contention_json(report))["artifact_type"] == "publish_queue_slot_contention"
    with pytest.raises(ValueError, match="bucket_size_minutes must be positive"):
        build_publish_queue_slot_contention_report([], bucket_size_minutes=0)
    with pytest.raises(ValueError, match="lookahead_hours must be positive"):
        build_publish_queue_slot_contention_report([], lookahead_hours=0)
    with pytest.raises(ValueError, match="max_posts_per_bucket must be positive"):
        build_publish_queue_slot_contention_report([], max_posts_per_bucket=0)
