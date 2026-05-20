"""Tests for publish queue dead-letter candidate reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.publish_queue_dead_letter_candidates import (
    build_publish_queue_dead_letter_candidates_report_from_db,
    format_publish_queue_dead_letter_candidates_json,
    format_publish_queue_dead_letter_candidates_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_queue_dead_letter_candidates.py"
spec = importlib.util.spec_from_file_location("publish_queue_dead_letter_candidates_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _ts(days_ago: int) -> str:
    return (NOW - timedelta(days=days_ago)).isoformat()


def _conn(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE publish_queue (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            status TEXT,
            scheduled_at TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY,
            queue_id INTEGER,
            content_id INTEGER,
            status TEXT,
            success INTEGER,
            attempted_at TEXT,
            error TEXT
        );
        """
    )
    return conn


def _queue(conn: sqlite3.Connection, queue_id: int, content_id: int, platform: str, status: str, days_ago: int) -> None:
    conn.execute(
        "INSERT INTO publish_queue VALUES (?, ?, ?, ?, ?, ?, ?)",
        (queue_id, content_id, platform, status, _ts(days_ago), _ts(days_ago + 1), _ts(days_ago)),
    )


def test_report_flags_stale_queued_held_failed_and_repeated_failures():
    conn = _conn()
    _queue(conn, 1, 101, "x", "queued", 10)
    _queue(conn, 2, 102, "bluesky", "held", 8)
    _queue(conn, 3, 103, "linkedin", "failed", 20)
    _queue(conn, 4, 104, "x", "queued", 1)
    conn.executemany(
        "INSERT INTO publication_attempts VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 3, 103, "failed", 0, _ts(19), "timeout 1"),
            (2, 3, 103, "failed", 0, _ts(18), "timeout 2"),
            (3, 3, 103, "failed", 0, _ts(17), "timeout 3"),
        ],
    )

    report = build_publish_queue_dead_letter_candidates_report_from_db(conn, now=NOW, days=7, min_failed_attempts=3)
    payload = json.loads(format_publish_queue_dead_letter_candidates_json(report))

    assert payload["artifact_type"] == "publish_queue_dead_letter_candidates"
    assert payload["summary"]["candidate_count"] == 3
    assert payload["summary"]["by_status"] == {"failed": 1, "held": 1, "queued": 1}
    assert payload["summary"]["oldest_scheduled_at"] == _ts(20)
    failed = next(item for item in payload["candidates"] if item["queue_id"] == 3)
    assert failed["repeated_failure_evidence"] is True
    assert failed["failed_attempt_count"] == 3
    assert failed["latest_error"] == "timeout 3"


def test_limit_ordering_formatters_cli_and_missing_schema(tmp_path, capsys):
    db_path = tmp_path / "queue.sqlite"
    conn = _conn(db_path)
    _queue(conn, 10, 110, "x", "queued", 30)
    _queue(conn, 11, 111, "x", "held", 15)
    _queue(conn, 12, 112, "x", "failed", 9)
    conn.commit()
    conn.close()

    conn = sqlite3.connect(db_path)
    report = build_publish_queue_dead_letter_candidates_report_from_db(conn, now=NOW, limit=2)
    assert [item["queue_id"] for item in report["candidates"]] == [10, 11]
    assert "candidates=3" in format_publish_queue_dead_letter_candidates_text(report)

    original_builder = script.build_publish_queue_dead_letter_candidates_report_from_db

    def fixed_now(conn, **kwargs):
        return original_builder(conn, now=NOW, **kwargs)

    import pytest

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(script, "build_publish_queue_dead_letter_candidates_report_from_db", fixed_now)
        assert script.main(["--db", str(db_path), "--days", "7", "--min-failed-attempts", "2", "--limit", "2", "--format", "json"]) == 0
        assert json.loads(capsys.readouterr().out)["summary"]["shown_count"] == 2
        assert script.main(["--db", str(db_path), "--format", "text"]) == 0
        assert "Publish Queue Dead Letter Candidates" in capsys.readouterr().out

    missing = build_publish_queue_dead_letter_candidates_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["publication_attempts", "publish_queue"]
    assert missing["candidates"] == []

    partial = sqlite3.connect(":memory:")
    partial.execute("CREATE TABLE publish_queue (id INTEGER PRIMARY KEY)")
    partial.execute("CREATE TABLE publication_attempts (id INTEGER PRIMARY KEY)")
    gaps = build_publish_queue_dead_letter_candidates_report_from_db(partial, now=NOW)
    assert gaps["missing_columns"] == {
        "publication_attempts": ["queue_id|content_id"],
        "publish_queue": ["scheduled_at", "status"],
    }
