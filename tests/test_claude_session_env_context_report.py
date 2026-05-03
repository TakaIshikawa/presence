"""Tests for Claude session environment context reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_session_env_context_report import (
    build_claude_session_env_context_report,
    format_claude_session_env_context_json,
    format_claude_session_env_context_text,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "claude_session_env_context_report.py"
)
spec = importlib.util.spec_from_file_location("claude_session_env_context_report_script", SCRIPT_PATH)
claude_session_env_context_report_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_env_context_report_script)


def test_iterable_rows_flag_cwd_and_git_branch_changes_independently():
    rows = [
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"environment": {"cwd": "/repo", "git_branch": "main", "model": "claude"}},
        },
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:05:00+00:00",
            "metadata": {"environment": {"cwd": "/repo/app", "git_branch": "main"}},
        },
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:10:00+00:00",
            "metadata": {"environment": {"cwd": "/repo", "git_branch": "main"}},
        },
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:15:00+00:00",
            "metadata": {"environment": {"cwd": "/repo", "git_branch": "feature"}},
        },
        {"session_id": "sess-empty", "timestamp": "2026-05-01T10:20:00+00:00"},
    ]

    report = build_claude_session_env_context_report(rows, days=7, now=NOW)
    payload = json.loads(format_claude_session_env_context_json(report))

    assert payload["artifact_type"] == "claude_session_env_context_report"
    assert list(payload) == sorted(payload)
    assert [row.session_id for row in report.rows] == ["sess-a", "sess-b"]
    assert report.rows[0].cwd_changed is True
    assert report.rows[0].git_branch_changed is False
    assert report.rows[1].cwd_changed is False
    assert report.rows[1].git_branch_changed is True


def test_database_missing_table_and_malformed_metadata_are_tolerated():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    missing = build_claude_session_env_context_report(conn, days=7, now=NOW)
    assert missing.rows == ()
    assert missing.missing_tables

    conn.execute(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            metadata TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO claude_session_events (session_id, timestamp, metadata) VALUES (?, ?, ?)",
        ("sess-bad", "2026-05-01T10:00:00+00:00", "{bad json"),
    )
    report = build_claude_session_env_context_report(conn, days=7, now=NOW)
    assert report.totals["malformed_metadata_count"] == 1
    assert report.rows == ()


def test_text_formatter_includes_totals_tables_and_session_rows():
    rows = [
        {
            "_source_table": "claude_session_events",
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"environment": {"cwd": "/repo", "git_branch": "main"}},
        },
        {
            "_source_table": "claude_session_events",
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:05:00+00:00",
            "metadata": {"environment": {"cwd": "/repo/app", "git_branch": "feature"}},
        },
    ]

    report = build_claude_session_env_context_report(rows, days=7, now=NOW)
    text = format_claude_session_env_context_text(report)

    assert "Claude Session Environment Context" in text
    assert "Filters: days=7 lookback_start=2026-04-26T12:00:00+00:00" in text
    assert "Totals: rows=2 context_events=2 sessions=1 malformed_metadata=0" in text
    assert "Source tables: claude_session_events" in text
    assert "session=sess-a events=2 cwd_changed=True git_branch_changed=True" in text
    assert "cwd=/repo,/repo/app" in text


def test_deterministic_sorting_and_cli_json(capsys, tmp_path):
    db_path = tmp_path / "events.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            cwd TEXT,
            git_branch TEXT,
            metadata TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO claude_session_events (session_id, timestamp, cwd, git_branch, metadata) VALUES (?, ?, ?, ?, ?)",
        ("sess-z", "2026-05-01T10:00:00+00:00", "/repo/z", "main", "{}"),
    )
    conn.execute(
        "INSERT INTO claude_session_events (session_id, timestamp, cwd, git_branch, metadata) VALUES (?, ?, ?, ?, ?)",
        ("sess-a", "2026-05-01T09:00:00+00:00", "/repo/a", "main", "{}"),
    )
    conn.commit()
    conn.close()

    assert claude_session_env_context_report_script.main(["--db", str(db_path), "--days", "7"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert [row["session_id"] for row in payload["rows"]] == ["sess-a", "sess-z"]
    assert (
        claude_session_env_context_report_script.main(
            ["--db", str(db_path), "--days", "7", "--format", "text"]
        )
        == 0
    )
    output = capsys.readouterr().out
    assert "Totals: rows=2 context_events=2 sessions=2 malformed_metadata=0" in output
    assert "session=sess-a events=1 cwd_changed=False git_branch_changed=False" in output
    assert claude_session_env_context_report_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
