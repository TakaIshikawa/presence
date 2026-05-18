"""Tests for publish queue orphaned content reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.publish_queue_orphaned_content import (
    build_publish_queue_orphaned_content_report,
    build_publish_queue_orphaned_content_report_from_db,
    format_publish_queue_orphaned_content_json,
    format_publish_queue_orphaned_content_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_queue_orphaned_content.py"
spec = importlib.util.spec_from_file_location("publish_queue_orphaned_content_script", SCRIPT_PATH)
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
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            published INTEGER,
            status TEXT
        );
        CREATE TABLE publish_queue (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            status TEXT,
            scheduled_at TEXT
        );
        """
    )
    return conn


def test_builder_flags_missing_abandoned_published_queued_and_malformed_schedule():
    report = build_publish_queue_orphaned_content_report(
        [
            {"queue_id": 1, "content_id": 99, "resolved_content_id": None, "queue_status": "queued", "scheduled_at": "2026-05-01T00:00:00+00:00"},
            {
                "queue_id": 2,
                "content_id": 2,
                "resolved_content_id": 2,
                "queue_status": "held",
                "content_status": "abandoned",
                "content_published": -1,
                "scheduled_at": "bad-date",
            },
            {
                "queue_id": 3,
                "content_id": 3,
                "resolved_content_id": 3,
                "queue_status": "queued",
                "content_status": "published",
                "content_published": 1,
                "scheduled_at": "2026-04-01T00:00:00+00:00",
            },
        ],
        now=NOW,
    )

    counts = report["summary"]["by_issue_type"]
    assert report["artifact_type"] == "publish_queue_orphaned_content"
    assert counts["missing_generated_content"] == 1
    assert counts["abandoned_generated_content"] == 1
    assert counts["already_published_queued"] == 1
    assert counts["malformed_scheduled_at"] == 1
    assert [finding["queue_id"] for finding in report["findings"][:2]] == [3, 1]


def test_status_platform_filter_and_limit_are_applied():
    report = build_publish_queue_orphaned_content_report(
        [
            {"queue_id": 1, "platform": "x", "queue_status": "queued", "resolved_content_id": None},
            {"queue_id": 2, "platform": "bluesky", "queue_status": "held", "resolved_content_id": None},
        ],
        status="queued",
        platform="x",
        limit=1,
        now=NOW,
    )

    assert report["summary"]["queue_count"] == 1
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["queue_id"] == 1


def test_db_adapter_reads_joined_queue_and_handles_missing_tables():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (1, 1, 'published')")
    conn.execute("INSERT INTO generated_content VALUES (2, -1, 'abandoned')")
    conn.execute("INSERT INTO publish_queue VALUES (10, 1, 'x', 'queued', ?)", (NOW.isoformat(),))
    conn.execute("INSERT INTO publish_queue VALUES (11, 2, 'x', 'held', 'bad')")
    conn.execute("INSERT INTO publish_queue VALUES (12, 999, 'bluesky', 'queued', ?)", (NOW.isoformat(),))

    report = build_publish_queue_orphaned_content_report_from_db(conn, now=NOW)

    assert report["summary"]["queue_count"] == 3
    assert report["summary"]["by_issue_type"]["missing_generated_content"] == 1
    assert report["summary"]["by_issue_type"]["abandoned_generated_content"] == 1
    assert report["summary"]["by_issue_type"]["already_published_queued"] == 1
    assert report["summary"]["by_issue_type"]["malformed_scheduled_at"] == 1

    empty = build_publish_queue_orphaned_content_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert empty["missing_tables"] == ["generated_content", "publish_queue"]
    assert empty["findings"] == []


def test_json_and_text_formatters_are_stable():
    report = build_publish_queue_orphaned_content_report(
        [{"queue_id": 1, "content_id": 99, "resolved_content_id": None}],
        now=NOW,
    )

    payload = json.loads(format_publish_queue_orphaned_content_json(report))
    assert payload["artifact_type"] == "publish_queue_orphaned_content"
    assert list(payload) == sorted(payload)
    text = format_publish_queue_orphaned_content_text(report)
    assert "Publish Queue Orphaned Content" in text
    assert "queue_id | scheduled_at | platform" in text


def test_cli_supports_db_status_platform_json_text_and_invalid_limit(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "queue.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE generated_content (id INTEGER PRIMARY KEY, published INTEGER, status TEXT);
        CREATE TABLE publish_queue (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            status TEXT,
            scheduled_at TEXT
        );
        INSERT INTO publish_queue VALUES (1, 123, 'x', 'queued', 'bad');
        """
    )
    conn.close()

    assert script.main(["--db", str(db_path), "--status", "queued", "--platform", "x", "--limit", "5", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "publish_queue_orphaned_content"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "missing_generated_content" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["findings"] == []
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
