"""Tests for reply quality flag trend reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from engagement.reply_quality_flag_trends import (
    build_reply_quality_flag_trends_report_from_db,
    format_reply_quality_flag_trends_json,
    format_reply_quality_flag_trends_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_quality_flag_trends.py"
spec = importlib.util.spec_from_file_location("reply_quality_flag_trends_script", SCRIPT_PATH)
reply_quality_flag_trends_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_quality_flag_trends_script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            platform TEXT,
            intent TEXT,
            status TEXT,
            quality_flags TEXT,
            detected_at TEXT
        );
        """
    )
    return conn


def test_report_buckets_flags_and_keeps_malformed_rows_visible():
    conn = _conn()
    conn.executemany(
        "INSERT INTO reply_queue VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, "x", "question", "pending", '["generic", "thin"]', "2026-05-20T10:00:00+00:00"),
            (2, "x", "question", "pending", '["generic"]', "2026-05-20T09:00:00+00:00"),
            (3, "bluesky", "thanks", "approved", '["tone"]', "2026-05-20T08:00:00+00:00"),
            (4, "x", "question", "pending", "[bad", "2026-05-20T07:00:00+00:00"),
            (5, "x", "question", "pending", '["old"]', "2026-05-01T07:00:00+00:00"),
        ],
    )

    report = build_reply_quality_flag_trends_report_from_db(
        conn, now=NOW, window_hours=24, min_count=2, min_rate=0.6, limit=10
    )
    payload = json.loads(format_reply_quality_flag_trends_json(report))

    assert payload["artifact_type"] == "reply_quality_flag_trends"
    assert payload["summary"]["rows_scanned"] == 4
    assert payload["summary"]["malformed_quality_flag_rows"] == 1
    assert payload["counts"]["by_platform"] == {"bluesky": 1, "x": 3}
    assert payload["flag_buckets"][0]["flag"] == "generic"
    assert payload["flag_buckets"][0]["count"] == 2
    assert payload["malformed_flag_rows"][0]["reply_queue_id"] == 4
    assert "Malformed quality_flags" in format_reply_quality_flag_trends_text(report)


def test_missing_reply_queue_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_reply_quality_flag_trends_report_from_db(conn, now=NOW)

    assert report["missing_tables"] == ["reply_queue"]
    assert report["summary"]["rows_scanned"] == 0


def test_cli_supports_json_and_text_with_db(tmp_path, capsys):
    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            platform TEXT,
            intent TEXT,
            status TEXT,
            quality_flags TEXT,
            detected_at TEXT
        );
        INSERT INTO reply_queue VALUES (1, 'x', 'question', 'pending', '["generic"]', '2026-05-20T10:00:00+00:00');
        """
    )
    conn.close()

    assert reply_quality_flag_trends_script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert '"artifact_type": "reply_quality_flag_trends"' in capsys.readouterr().out
    assert reply_quality_flag_trends_script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Reply Quality Flag Trends" in capsys.readouterr().out
