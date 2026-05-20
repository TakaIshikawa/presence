"""Tests for publication attempt timeout risk reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.publication_attempt_timeout_risk import (
    build_publication_attempt_timeout_risk_report,
    build_publication_attempt_timeout_risk_report_from_db,
    format_publication_attempt_timeout_risk_json,
    format_publication_attempt_timeout_risk_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_timeout_risk.py"
spec = importlib.util.spec_from_file_location("publication_attempt_timeout_risk_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE publish_queue (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            status TEXT
        );
        CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY,
            queue_id INTEGER,
            content_id INTEGER,
            platform TEXT,
            status TEXT,
            started_at TEXT,
            succeeded_at TEXT,
            failed_at TEXT
        );
        """
    )
    return conn


def _insert_attempt(
    conn: sqlite3.Connection,
    attempt_id: int,
    *,
    hours_ago: float,
    status: str = "started",
    platform: str = "x",
    queue_id: int | None = None,
    content_id: int | None = 10,
    succeeded_at: str | None = None,
    failed_at: str | None = None,
) -> None:
    conn.execute(
        """INSERT INTO publication_attempts
           (id, queue_id, content_id, platform, status, started_at, succeeded_at, failed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            attempt_id,
            queue_id,
            content_id,
            platform,
            status,
            (NOW - timedelta(hours=hours_ago)).isoformat(),
            succeeded_at,
            failed_at,
        ),
    )
    conn.commit()


def test_builder_filters_terminal_recent_and_completed_attempts():
    report = build_publication_attempt_timeout_risk_report(
        [
            {"attempt_id": 1, "platform": "x", "status": "started", "started_at": (NOW - timedelta(hours=4)).isoformat()},
            {"attempt_id": 2, "platform": "x", "status": "started", "started_at": (NOW - timedelta(minutes=30)).isoformat()},
            {"attempt_id": 3, "platform": "x", "status": "success", "started_at": (NOW - timedelta(hours=5)).isoformat()},
            {
                "attempt_id": 4,
                "platform": "bluesky",
                "status": "running",
                "started_at": (NOW - timedelta(hours=3)).isoformat(),
                "terminal_failure_at": (NOW - timedelta(hours=2)).isoformat(),
            },
        ],
        hours=2,
        now=NOW,
    )

    assert report["artifact_type"] == "publication_attempt_timeout_risk"
    assert [finding["attempt_id"] for finding in report["findings"]] == [1]
    assert report["findings"][0]["age_hours"] == 4.0
    assert report["summary"]["by_platform_status"] == [
        {"platform": "x", "status": "started", "count": 1, "example_attempt_ids": [1]}
    ]


def test_db_adapter_reads_queue_linkage_and_groups_by_platform_status():
    conn = _conn()
    conn.execute("INSERT INTO publish_queue VALUES (100, 20, 'x', 'publishing')")
    _insert_attempt(conn, 1, hours_ago=5, status="started", queue_id=100, content_id=20)
    _insert_attempt(conn, 2, hours_ago=4, status="in-flight", platform="bluesky", content_id=30)
    _insert_attempt(conn, 3, hours_ago=6, status="failed", content_id=40)
    _insert_attempt(conn, 4, hours_ago=7, status="running", succeeded_at=(NOW - timedelta(hours=1)).isoformat())

    report = build_publication_attempt_timeout_risk_report_from_db(conn, hours=2, now=NOW)

    assert report["summary"]["finding_count"] == 2
    assert [finding["attempt_id"] for finding in report["findings"]] == [1, 2]
    assert report["findings"][0]["queue_id"] == 100
    assert report["findings"][0]["queue_content_id"] == 20
    assert report["findings"][0]["queue_status"] == "publishing"
    assert report["summary"]["by_platform_status"] == [
        {"platform": "bluesky", "status": "in_flight", "count": 1, "example_attempt_ids": [2]},
        {"platform": "x", "status": "started", "count": 1, "example_attempt_ids": [1]},
    ]


def test_missing_schema_returns_empty_report_metadata():
    missing_table = build_publication_attempt_timeout_risk_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing_table["artifact_type"] == "publication_attempt_timeout_risk"
    assert missing_table["missing_tables"] == ["publication_attempts"]
    assert missing_table["findings"] == []

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE publication_attempts (id INTEGER PRIMARY KEY, platform TEXT)")
    missing_columns = build_publication_attempt_timeout_risk_report_from_db(conn, now=NOW)
    assert missing_columns["missing_columns"]["publication_attempts"] == [
        "started_at|created_at|attempted_at",
        "status",
    ]
    assert missing_columns["findings"] == []


def test_json_and_text_formatters_are_stable():
    report = build_publication_attempt_timeout_risk_report(
        [
            {
                "attempt_id": 1,
                "platform": "x",
                "status": "queued",
                "started_at": (NOW - timedelta(hours=3)).isoformat(),
                "content_id": 10,
                "queue_id": 20,
            }
        ],
        hours=2,
        now=NOW,
    )

    payload = json.loads(format_publication_attempt_timeout_risk_json(report))
    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "publication_attempt_timeout_risk"
    text = format_publication_attempt_timeout_risk_text(report)
    assert "Publication Attempt Timeout Risk" in text
    assert "attempt_id=1 platform=x status=queued age_hours=3.0" in text


def test_cli_supports_db_hours_limit_json_text_and_invalid_numbers(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "attempts.sqlite"
    conn = _conn()
    _insert_attempt(conn, 1, hours_ago=5)
    conn.backup(sqlite3.connect(db_path))
    conn.close()

    assert script.main(["--db", str(db_path), "--hours", "2", "--limit", "5", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Publication Attempt Timeout Risk" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_tables"] == ["publication_attempts"]
    with pytest.raises(SystemExit):
        script.parse_args(["--hours", "0"])
