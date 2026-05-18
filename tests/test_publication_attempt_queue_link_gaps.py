"""Tests for publication attempt queue link gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.publication_attempt_queue_link_gaps import (
    build_publication_attempt_queue_link_gaps_report,
    build_publication_attempt_queue_link_gaps_report_from_db,
    format_publication_attempt_queue_link_gaps_json,
    format_publication_attempt_queue_link_gaps_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_queue_link_gaps.py"
spec = importlib.util.spec_from_file_location("publication_attempt_queue_link_gaps_script", SCRIPT_PATH)
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
        CREATE TABLE generated_content (id INTEGER PRIMARY KEY);
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
            attempted_at TEXT
        );
        """
    )
    return conn


def test_builder_flags_missing_queue_mismatched_content_and_successful_open_queue():
    report = build_publication_attempt_queue_link_gaps_report(
        [
            {"attempt_id": 1, "queue_id": 99, "resolved_queue_id": None, "content_id": 1, "platform": "x"},
            {
                "attempt_id": 2,
                "queue_id": 10,
                "resolved_queue_id": 10,
                "content_id": 1,
                "queue_content_id": 2,
                "queue_status": "queued",
                "attempt_status": "success",
                "platform": "bluesky",
            },
        ],
        now=NOW,
    )

    counts = report["summary"]["by_issue_type"]
    assert report["artifact_type"] == "publication_attempt_queue_link_gaps"
    assert counts["missing_queue"] == 1
    assert counts["content_id_mismatch"] == 1
    assert counts["successful_attempt_unpublished_queue"] == 1
    assert report["summary"]["by_platform_issue_type"]["bluesky"]["content_id_mismatch"] == 1


def test_platform_filter_and_limit_are_applied():
    report = build_publication_attempt_queue_link_gaps_report(
        [
            {"attempt_id": 1, "queue_id": 1, "resolved_queue_id": None, "platform": "x"},
            {"attempt_id": 2, "queue_id": 2, "resolved_queue_id": None, "platform": "bluesky"},
        ],
        platform="x",
        limit=1,
        now=NOW,
    )

    assert report["summary"]["attempt_count"] == 1
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["platform"] == "x"


def test_db_adapter_reads_joined_attempts_and_handles_missing_tables():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (1)")
    conn.execute("INSERT INTO generated_content VALUES (2)")
    conn.execute("INSERT INTO publish_queue VALUES (10, 2, 'bluesky', 'queued')")
    conn.execute("INSERT INTO publication_attempts VALUES (1, 10, 1, 'bluesky', 'success', ?)", (NOW.isoformat(),))
    conn.execute("INSERT INTO publication_attempts VALUES (2, 999, 1, 'x', 'failed', ?)", (NOW.isoformat(),))

    report = build_publication_attempt_queue_link_gaps_report_from_db(conn, days=7, now=NOW)

    assert report["summary"]["attempt_count"] == 2
    assert report["summary"]["by_issue_type"]["missing_queue"] == 1
    assert report["summary"]["by_issue_type"]["content_id_mismatch"] == 1
    assert report["summary"]["by_issue_type"]["successful_attempt_unpublished_queue"] == 1

    empty = build_publication_attempt_queue_link_gaps_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert empty["missing_tables"] == ["generated_content", "publication_attempts", "publish_queue"]
    assert empty["findings"] == []


def test_json_and_text_formatters_are_stable():
    report = build_publication_attempt_queue_link_gaps_report(
        [{"attempt_id": 1, "queue_id": 99, "resolved_queue_id": None, "platform": "x"}],
        now=NOW,
    )

    payload = json.loads(format_publication_attempt_queue_link_gaps_json(report))
    assert payload["artifact_type"] == "publication_attempt_queue_link_gaps"
    assert list(payload) == sorted(payload)
    text = format_publication_attempt_queue_link_gaps_text(report)
    assert "Publication Attempt Queue Link Gaps" in text
    assert "attempt_id | queue_id | platform" in text


def test_cli_supports_db_days_platform_json_text_and_invalid_numbers(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "attempts.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE generated_content (id INTEGER PRIMARY KEY);
        CREATE TABLE publish_queue (id INTEGER PRIMARY KEY, content_id INTEGER, platform TEXT, status TEXT);
        CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY,
            queue_id INTEGER,
            content_id INTEGER,
            platform TEXT,
            status TEXT,
            attempted_at TEXT
        );
        """
    )
    conn.execute("INSERT INTO publication_attempts VALUES (1, 123, 1, 'x', 'failed', ?)", (NOW.isoformat(),))
    conn.close()

    assert script.main(["--db", str(db_path), "--days", "7", "--platform", "x", "--limit", "5", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "publication_attempt_queue_link_gaps"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "missing_queue" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["findings"] == []
    with pytest.raises(SystemExit):
        script.parse_args(["--days", "0"])
