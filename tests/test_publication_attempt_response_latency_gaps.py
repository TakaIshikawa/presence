"""Tests for publication attempt response latency gap reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.publication_attempt_response_latency_gaps import (
    build_publication_attempt_response_latency_gaps_report,
    build_publication_attempt_response_latency_gaps_report_from_db,
    format_publication_attempt_response_latency_gaps_json,
    format_publication_attempt_response_latency_gaps_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_response_latency_gaps.py"
spec = importlib.util.spec_from_file_location("publication_attempt_response_latency_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


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
            status TEXT,
            attempted_at TEXT,
            completed_at TEXT,
            response_metadata TEXT
        );
        """
    )
    return conn


def test_builder_flags_each_latency_gap_type_and_skips_clean_rows():
    report = build_publication_attempt_response_latency_gaps_report(
        [
            {"attempt_id": 1, "content_id": 10, "platform": "x", "success": 1, "started_at": None, "completed_at": "2026-05-20T10:00:00+00:00", "response_metadata": "{}"},
            {"attempt_id": 2, "content_id": 11, "platform": "x", "success": 0, "started_at": "2026-05-20T10:10:00+00:00", "completed_at": "2026-05-20T10:00:00+00:00", "response_metadata": "{}"},
            {"attempt_id": 3, "content_id": 12, "platform": "bluesky", "status": "failed", "started_at": "2026-05-20T09:00:00+00:00", "completed_at": "2026-05-20T10:00:01+00:00", "response_metadata": "{}"},
            {"attempt_id": 4, "content_id": 13, "platform": "x", "status": "pending", "started_at": "2026-05-20T10:00:00+00:00", "completed_at": "2026-05-20T10:01:00+00:00", "response_metadata": "{}"},
            {"attempt_id": 5, "content_id": 14, "platform": "x", "success": 1, "started_at": "2026-05-20T10:00:00+00:00", "completed_at": "2026-05-20T10:01:00+00:00", "response_metadata": "{}"},
            {"attempt_id": 6, "content_id": 15, "platform": "x", "success": 1, "started_at": None, "completed_at": None, "response_metadata": ""},
        ],
        max_latency_minutes=60,
        now=NOW,
    )

    payload = json.loads(format_publication_attempt_response_latency_gaps_json(report))
    assert payload["artifact_type"] == "publication_attempt_response_latency_gaps"
    assert payload["summary"]["by_issue_type"] == {
        "missing_timestamps": 1,
        "negative_latency": 1,
        "non_terminal_with_response_metadata": 1,
        "over_threshold_latency": 1,
    }
    assert payload["summary"]["sample_attempt_ids"] == [1, 2, 3, 4]
    assert payload["findings"]["negative_latency"][0]["latency_minutes"] == -10.0
    assert payload["findings"]["over_threshold_latency"][0]["threshold_minutes"] == 60


def test_db_loader_reports_empty_state_and_schema_gaps():
    conn = _conn()
    conn.execute(
        "INSERT INTO publication_attempts VALUES (1, 10, 'x', 1, 'success', ?, ?, ?)",
        ("2026-05-20T10:00:00+00:00", "2026-05-20T10:02:00+00:00", '{"id":"p"}'),
    )
    report = build_publication_attempt_response_latency_gaps_report_from_db(conn, now=NOW)
    assert report["summary"]["finding_count"] == 0
    assert report["empty_state"]["is_empty"] is True

    missing_table = build_publication_attempt_response_latency_gaps_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing_table["missing_tables"] == ["publication_attempts"]

    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE publication_attempts (id INTEGER, content_id INTEGER, platform TEXT)")
    missing_columns = build_publication_attempt_response_latency_gaps_report_from_db(bad, now=NOW)
    assert missing_columns["missing_columns"] == {"publication_attempts": ["response_metadata"]}


def test_db_loader_handles_missing_optional_columns_without_crashing():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            response_metadata TEXT
        );
        INSERT INTO publication_attempts VALUES (1, 10, 'x', '{}');
        """
    )

    report = build_publication_attempt_response_latency_gaps_report_from_db(conn, now=NOW)

    assert report["summary"]["finding_count"] == 1
    assert report["findings"]["non_terminal_with_response_metadata"][0]["attempt_id"] == 1
    assert "publication_attempts" in report["optional_missing_columns"]


def test_formatters_and_cli(tmp_path, capsys):
    report = build_publication_attempt_response_latency_gaps_report(
        [{"attempt_id": 7, "content_id": 1, "platform": "x", "status": "pending", "response_metadata": "{}"}],
        now=NOW,
    )
    text = format_publication_attempt_response_latency_gaps_text(report)
    assert "Totals: attempts=1 with_metadata=1 findings=1 shown=1" in text
    assert "Sample attempt IDs: 7" in text

    db_path = tmp_path / "attempts.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY, content_id INTEGER, platform TEXT, success INTEGER,
            attempted_at TEXT, completed_at TEXT, response_metadata TEXT
        );
        INSERT INTO publication_attempts VALUES (1, 1, 'x', 1, '2026-05-20T10:00:00+00:00', '2026-05-20T11:00:00+00:00', '{}');
        """
    )
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json", "--max-latency-minutes", "30", "--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"] == {"limit": 5, "max_latency_minutes": 30}
    assert payload["findings"]["over_threshold_latency"][0]["attempt_id"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Publication Attempt Response Latency Gaps" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--max-latency-minutes", "0"])
