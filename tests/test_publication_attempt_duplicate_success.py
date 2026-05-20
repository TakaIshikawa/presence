"""Tests for duplicate successful publication attempt reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.publication_attempt_duplicate_success import (
    build_publication_attempt_duplicate_success_report,
    build_publication_attempt_duplicate_success_report_from_db,
    format_publication_attempt_duplicate_success_json,
    format_publication_attempt_duplicate_success_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_duplicate_success.py"
spec = importlib.util.spec_from_file_location("publication_attempt_duplicate_success_script", SCRIPT_PATH)
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
        CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            attempted_at TEXT,
            success INTEGER,
            platform_post_id TEXT,
            platform_url TEXT
        );
        """
    )
    return conn


def test_builder_groups_duplicate_successes_and_flags_conflicts():
    report = build_publication_attempt_duplicate_success_report(
        [
            {
                "attempt_id": 1,
                "content_id": 10,
                "platform": "x",
                "success": 1,
                "platform_post_id": "p1",
                "platform_url": "https://x/1",
            },
            {
                "attempt_id": 2,
                "content_id": 10,
                "platform": "x",
                "status": "success",
                "platform_post_id": "p2",
                "platform_url": "https://x/1",
            },
            {"attempt_id": 3, "content_id": 10, "platform": "bluesky", "success": 1},
            {"attempt_id": 4, "content_id": 11, "platform": "x", "success": 0},
        ],
        now=NOW,
    )

    assert report["artifact_type"] == "publication_attempt_duplicate_success"
    assert report["summary"]["attempt_count"] == 4
    assert report["summary"]["duplicate_group_count"] == 1
    finding = report["findings"][0]
    assert finding["content_id"] == "10"
    assert finding["platform"] == "x"
    assert finding["successful_attempt_count"] == 2
    assert finding["post_id_conflict"] is True
    assert finding["url_conflict"] is False


def test_platform_filter_and_limit_are_applied():
    report = build_publication_attempt_duplicate_success_report(
        [
            {"attempt_id": 1, "content_id": 1, "platform": "x", "success": 1, "platform_url": "a"},
            {"attempt_id": 2, "content_id": 1, "platform": "x", "success": 1, "platform_url": "b"},
            {"attempt_id": 3, "content_id": 2, "platform": "bluesky", "success": 1},
            {"attempt_id": 4, "content_id": 2, "platform": "bluesky", "success": 1},
        ],
        platform="x",
        limit=1,
        now=NOW,
    )

    assert report["summary"]["attempt_count"] == 2
    assert report["summary"]["duplicate_group_count"] == 1
    assert report["findings"][0]["platform"] == "x"


def test_db_loader_reads_successes_and_handles_missing_schema():
    conn = _conn()
    conn.execute(
        "INSERT INTO publication_attempts VALUES (1, 10, 'x', ?, 1, 'p1', 'u1')",
        (NOW.isoformat(),),
    )
    conn.execute(
        "INSERT INTO publication_attempts VALUES (2, 10, 'x', ?, 1, 'p1', 'u2')",
        (NOW.isoformat(),),
    )
    conn.execute(
        "INSERT INTO publication_attempts VALUES (3, 10, 'x', ?, 0, 'p3', 'u3')",
        (NOW.isoformat(),),
    )

    report = build_publication_attempt_duplicate_success_report_from_db(conn, now=NOW)

    assert report["summary"]["duplicate_group_count"] == 1
    assert report["findings"][0]["post_id_conflict"] is False
    assert report["findings"][0]["url_conflict"] is True

    missing_table = build_publication_attempt_duplicate_success_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing_table["missing_tables"] == ["publication_attempts"]
    assert missing_table["findings"] == []

    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE publication_attempts (content_id INTEGER, platform TEXT)")
    missing_columns = build_publication_attempt_duplicate_success_report_from_db(bad, now=NOW)
    assert missing_columns["missing_columns"] == {"publication_attempts": ["success/status"]}


def test_json_and_text_formatters_are_stable():
    report = build_publication_attempt_duplicate_success_report(
        [
            {"attempt_id": 1, "content_id": 1, "platform": "x", "success": 1},
            {"attempt_id": 2, "content_id": 1, "platform": "x", "success": 1},
        ],
        now=NOW,
    )

    payload = json.loads(format_publication_attempt_duplicate_success_json(report))
    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "publication_attempt_duplicate_success"
    text = format_publication_attempt_duplicate_success_text(report)
    assert "Publication Attempt Duplicate Success" in text
    assert "content_id | platform | successes" in text


def test_cli_supports_db_platform_limit_json_text_and_invalid_numbers(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "attempts.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            attempted_at TEXT,
            success INTEGER,
            platform_post_id TEXT,
            platform_url TEXT
        );
        INSERT INTO publication_attempts VALUES (1, 1, 'x', '2026-05-01T12:00:00+00:00', 1, 'p1', 'u1');
        INSERT INTO publication_attempts VALUES (2, 1, 'x', '2026-05-01T12:01:00+00:00', 1, 'p2', 'u2');
        """
    )
    conn.close()

    assert script.main(["--db", str(db_path), "--platform", "x", "--limit", "5", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "publication_attempt_duplicate_success"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "post_id_conflict" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_tables"] == ["publication_attempts"]
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
