"""Tests for publication attempt response metadata gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.publication_attempt_response_metadata_gaps import (
    build_publication_attempt_response_metadata_gaps_report,
    build_publication_attempt_response_metadata_gaps_report_from_db,
    format_publication_attempt_response_metadata_gaps_json,
    format_publication_attempt_response_metadata_gaps_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_response_metadata_gaps.py"
spec = importlib.util.spec_from_file_location("publication_attempt_response_metadata_gaps_script", SCRIPT_PATH)
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
            success INTEGER,
            attempted_at TEXT,
            response_metadata TEXT
        );
        """
    )
    return conn


def test_builder_classifies_metadata_gap_types_and_required_fields():
    report = build_publication_attempt_response_metadata_gaps_report(
        [
            {"attempt_id": 1, "content_id": 10, "platform": "x", "success": 0, "attempted_at": "2026-05-01T10:00:00+00:00", "response_metadata": None},
            {"attempt_id": 2, "content_id": 11, "platform": "x", "success": 1, "attempted_at": "2026-05-01T10:01:00+00:00", "response_metadata": "{bad"},
            {"attempt_id": 3, "content_id": 12, "platform": "bluesky", "success": 1, "attempted_at": "2026-05-01T10:02:00+00:00", "response_metadata": "{}"},
            {"attempt_id": 4, "content_id": 13, "platform": "bluesky", "success": 1, "attempted_at": "2026-05-01T10:03:00+00:00", "response_metadata": '{"status":"ok"}'},
            {"attempt_id": 5, "content_id": 14, "platform": "x", "success": 1, "attempted_at": "2026-05-01T10:04:00+00:00", "response_metadata": '{"tweet_id":"t1"}'},
        ],
        now=NOW,
    )

    assert report["artifact_type"] == "publication_attempt_response_metadata_gaps"
    assert report["summary"]["attempt_count"] == 5
    assert [item["gap_type"] for item in report["items"]] == [
        "missing_metadata",
        "malformed_metadata",
        "empty_metadata",
        "missing_platform_identifier",
    ]
    assert set(report["items"][0]) == {"attempt_id", "content_id", "platform", "success", "attempted_at", "gap_type"}


def test_db_loader_platform_filter_limit_and_schema_gaps():
    conn = _conn()
    conn.execute("INSERT INTO publication_attempts VALUES (1, 10, 'x', 1, ?, NULL)", (NOW.isoformat(),))
    conn.execute("INSERT INTO publication_attempts VALUES (2, 11, 'bluesky', 1, ?, '{\"uri\":\"at://post\"}')", (NOW.isoformat(),))
    conn.execute("INSERT INTO publication_attempts VALUES (3, 12, 'x', 0, ?, '{}')", (NOW.isoformat(),))

    report = build_publication_attempt_response_metadata_gaps_report_from_db(conn, platform="x", limit=1, now=NOW)

    assert report["summary"]["attempt_count"] == 2
    assert report["summary"]["gap_count"] == 2
    assert len(report["items"]) == 1
    assert report["items"][0]["attempt_id"] == 1

    missing_table = build_publication_attempt_response_metadata_gaps_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing_table["missing_tables"] == ["publication_attempts"]

    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE publication_attempts (id INTEGER, content_id INTEGER, platform TEXT)")
    missing_columns = build_publication_attempt_response_metadata_gaps_report_from_db(bad, now=NOW)
    assert missing_columns["missing_columns"] == {"publication_attempts": ["response_metadata"]}


def test_formatters_and_cli(tmp_path, monkeypatch, capsys):
    report = build_publication_attempt_response_metadata_gaps_report(
        [{"attempt_id": 1, "content_id": 1, "platform": "x", "success": 1, "response_metadata": ""}],
        now=NOW,
    )
    assert json.loads(format_publication_attempt_response_metadata_gaps_json(report))["artifact_type"] == "publication_attempt_response_metadata_gaps"
    assert "attempt_id | content_id | platform" in format_publication_attempt_response_metadata_gaps_text(report)

    db_path = tmp_path / "attempts.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY, content_id INTEGER, platform TEXT, success INTEGER, attempted_at TEXT, response_metadata TEXT
        );
        INSERT INTO publication_attempts VALUES (1, 1, 'x', 1, '2026-05-01T12:00:00+00:00', '');
        """
    )
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["items"][0]["gap_type"] == "missing_metadata"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Publication Attempt Response Metadata Gaps" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_tables"] == ["publication_attempts"]
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
