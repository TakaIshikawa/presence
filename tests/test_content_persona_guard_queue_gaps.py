"""Tests for content persona guard queue gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.content_persona_guard_queue_gaps import (
    build_content_persona_guard_queue_gaps_report,
    build_content_persona_guard_queue_gaps_report_from_db,
    format_content_persona_guard_queue_gaps_json,
    format_content_persona_guard_queue_gaps_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_persona_guard_queue_gaps.py"
spec = importlib.util.spec_from_file_location("content_persona_guard_queue_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _ts(hours_ago: int) -> str:
    return (NOW - timedelta(hours=hours_ago)).strftime("%Y-%m-%d %H:%M:%S")


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE generated_content (
               id INTEGER PRIMARY KEY,
               content_type TEXT,
               content TEXT,
               created_at TEXT
           );
           CREATE TABLE content_persona_guard (
               content_id INTEGER PRIMARY KEY,
               checked INTEGER,
               passed INTEGER,
               status TEXT,
               score REAL,
               reasons TEXT,
               metrics TEXT,
               created_at TEXT,
               updated_at TEXT
           );
           CREATE TABLE publish_queue (
               id INTEGER PRIMARY KEY,
               content_id INTEGER,
               platform TEXT,
               status TEXT,
               scheduled_at TEXT,
               created_at TEXT
           );
           CREATE TABLE content_publications (
               id INTEGER PRIMARY KEY,
               content_id INTEGER,
               platform TEXT,
               status TEXT,
               next_retry_at TEXT,
               updated_at TEXT
           );"""
    )
    return conn


def _content(conn: sqlite3.Connection, content_id: int, content: str = "Queued copy") -> None:
    conn.execute(
        "INSERT INTO generated_content (id, content_type, content, created_at) VALUES (?, 'x_post', ?, ?)",
        (content_id, content, _ts(48)),
    )


def _guard(
    conn: sqlite3.Connection,
    content_id: int,
    *,
    checked: int = 1,
    passed: int = 1,
    status: str = "passed",
    reasons: str | None = "[]",
    metrics: str | None = "{}",
) -> None:
    conn.execute(
        """INSERT INTO content_persona_guard
           (content_id, checked, passed, status, score, reasons, metrics, created_at, updated_at)
           VALUES (?, ?, ?, ?, 0.9, ?, ?, ?, ?)""",
        (content_id, checked, passed, status, reasons, metrics, _ts(24), _ts(23)),
    )


def test_builder_flags_missing_unchecked_failed_and_malformed_guard_rows():
    report = build_content_persona_guard_queue_gaps_report(
        [
            {
                "source": "publish_queue",
                "queue_id": 1,
                "content_id": 10,
                "platform": "x",
                "queue_status": "queued",
                "guard_content_id": None,
                "scheduled_at": _ts(3),
            },
            {
                "source": "publish_queue",
                "queue_id": 2,
                "content_id": 11,
                "platform": "x",
                "queue_status": "queued",
                "guard_content_id": 11,
                "guard_checked": 0,
                "guard_passed": 1,
                "guard_status": "passed",
            },
            {
                "source": "content_publications",
                "publication_id": 7,
                "content_id": 12,
                "platform": "bluesky",
                "queue_status": "queued",
                "guard_content_id": 12,
                "guard_checked": 1,
                "guard_passed": 0,
                "guard_status": "failed",
                "guard_reasons": "[",
                "guard_metrics": "not-json",
            },
        ],
        now=NOW,
    )

    assert report["artifact_type"] == "content_persona_guard_queue_gaps"
    assert report["summary"]["gap_count"] == 3
    assert report["summary"]["by_issue_type"] == {
        "failed_guard": 1,
        "malformed_metrics_json": 1,
        "malformed_reasons_json": 1,
        "missing_guard": 1,
        "unchecked_guard": 1,
    }
    assert {"platform": "x", "guard_status": "missing_guard", "count": 1} in report["grouped_counts"]


def test_db_loader_reads_publish_queue_and_content_publications():
    conn = _conn()
    for content_id in (1, 2, 3, 4):
        _content(conn, content_id)
    _guard(conn, 1)
    _guard(conn, 2, checked=0)
    _guard(conn, 3, passed=0, status="failed")
    _guard(conn, 4, reasons="{bad", metrics="{}")
    conn.execute("INSERT INTO publish_queue (id, content_id, platform, status, scheduled_at, created_at) VALUES (10, 1, 'x', 'queued', ?, ?)", (_ts(5), _ts(6)))
    conn.execute("INSERT INTO publish_queue (id, content_id, platform, status, scheduled_at, created_at) VALUES (11, 2, 'x', 'queued', ?, ?)", (_ts(5), _ts(6)))
    conn.execute("INSERT INTO content_publications (id, content_id, platform, status, next_retry_at, updated_at) VALUES (20, 3, 'bluesky', 'queued', ?, ?)", (_ts(4), _ts(7)))
    conn.execute("INSERT INTO content_publications (id, content_id, platform, status, next_retry_at, updated_at) VALUES (21, 4, 'blog', 'queued', ?, ?)", (_ts(4), _ts(7)))
    conn.commit()

    report = build_content_persona_guard_queue_gaps_report_from_db(conn, now=NOW)

    assert report["summary"]["rows_scanned"] == 4
    assert report["summary"]["gap_count"] == 3
    assert [item["content_id"] for item in report["gap_items"]] == [4, 3, 2]
    assert report["gap_items"][0]["source"] == "content_publications"
    assert report["gap_items"][0]["issue_types"] == ["malformed_reasons_json"]
    assert report["gap_items"][2]["issue_types"] == ["unchecked_guard"]


def test_db_loader_missing_guard_table_reports_all_queued_as_missing():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE publish_queue (id INTEGER PRIMARY KEY, content_id INTEGER, platform TEXT, status TEXT)")
    conn.execute("INSERT INTO publish_queue (content_id, platform, status) VALUES (5, 'x', 'queued')")

    report = build_content_persona_guard_queue_gaps_report_from_db(conn, now=NOW)

    assert report["missing_tables"] == ["content_persona_guard", "content_publications", "generated_content"]
    assert report["gap_items"][0]["issue_types"] == ["missing_guard"]


def test_formatters_and_cli_support_json_text_filters_and_validation(tmp_path, monkeypatch, capsys):
    conn = _conn()
    _content(conn, 1)
    conn.execute("INSERT INTO publish_queue (content_id, platform, status, scheduled_at, created_at) VALUES (1, 'x', 'queued', ?, ?)", (_ts(8), _ts(9)))
    conn.execute("INSERT INTO publish_queue (content_id, platform, status, scheduled_at, created_at) VALUES (1, 'x', 'held', ?, ?)", (_ts(1), _ts(1)))
    conn.commit()
    db_path = tmp_path / "queue.sqlite"
    conn.backup(sqlite3.connect(db_path))

    assert script.main(["--db", str(db_path), "--format", "json", "--status", "queued", "--min-age-hours", "2"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "content_persona_guard_queue_gaps"
    assert payload["summary"]["gap_count"] == 1

    assert script.main(["--db", str(db_path), "--format", "text", "--status", "all"]) == 0
    assert "Content Persona Guard Queue Gaps" in capsys.readouterr().out

    memory = _conn()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(memory))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["gap_items"] == []
    assert script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_builder_rejects_invalid_thresholds():
    with pytest.raises(ValueError, match="min_age_hours must be non-negative"):
        build_content_persona_guard_queue_gaps_report([], min_age_hours=-1)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_content_persona_guard_queue_gaps_report([], limit=0)


def test_json_formatter_is_stable():
    report = build_content_persona_guard_queue_gaps_report([], now=NOW)
    assert list(json.loads(format_content_persona_guard_queue_gaps_json(report))) == sorted(report)
    assert "No queued content persona guard gaps found" in format_content_persona_guard_queue_gaps_text(report)
