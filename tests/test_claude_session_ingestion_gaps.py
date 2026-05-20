"""Tests for Claude session ingestion gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.claude_session_ingestion_gaps import (
    build_claude_session_ingestion_gaps_report,
    format_claude_session_ingestion_gaps_json,
    format_claude_session_ingestion_gaps_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_session_ingestion_gaps.py"
spec = importlib.util.spec_from_file_location("claude_session_ingestion_gaps_script", SCRIPT_PATH)
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
        """CREATE TABLE ingestion_metadata (
               id INTEGER PRIMARY KEY,
               session_id TEXT,
               source_log TEXT,
               project_path TEXT,
               session_timestamp TEXT,
               ingested_at TEXT
           );
           CREATE TABLE claude_messages (
               id INTEGER PRIMARY KEY,
               session_id TEXT,
               message_uuid TEXT,
               project_path TEXT,
               timestamp TEXT
           );
           CREATE TABLE generated_content (
               id INTEGER PRIMARY KEY,
               source_messages TEXT,
               content TEXT,
               created_at TEXT
           );"""
    )
    return conn


def _metadata(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    source_log: str | None = None,
    project_path: str | None = "/repo/presence",
    session_timestamp: str | None = "2026-05-19T10:00:00+00:00",
) -> None:
    conn.execute(
        """INSERT INTO ingestion_metadata
           (session_id, source_log, project_path, session_timestamp, ingested_at)
           VALUES (?, ?, ?, ?, ?)""",
        (session_id, source_log or f"/logs/{session_id}.jsonl", project_path, session_timestamp, "2026-05-19T11:00:00+00:00"),
    )
    conn.commit()


def _message(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    message_uuid: str | None = None,
    project_path: str | None = "/repo/presence",
    timestamp: str | None = "2026-05-19T10:10:00+00:00",
) -> None:
    conn.execute(
        "INSERT INTO claude_messages (session_id, message_uuid, project_path, timestamp) VALUES (?, ?, ?, ?)",
        (session_id, message_uuid or f"msg-{session_id}", project_path, timestamp),
    )
    conn.commit()


def _content(conn: sqlite3.Connection, source_messages: list[str]) -> None:
    conn.execute(
        "INSERT INTO generated_content (source_messages, content, created_at) VALUES (?, ?, ?)",
        (json.dumps(source_messages), "artifact", "2026-05-19T12:00:00+00:00"),
    )
    conn.commit()


def test_clean_data_has_no_findings():
    conn = _conn()
    _metadata(conn, "sess-clean")
    _message(conn, "sess-clean", message_uuid="msg-clean")
    _content(conn, ["msg-clean"])

    report = build_claude_session_ingestion_gaps_report(conn, now=NOW)

    assert report["findings"] == []
    assert report["summary"]["finding_count"] == 0
    assert "No Claude session ingestion gaps" in format_claude_session_ingestion_gaps_text(report)


def test_each_gap_type_is_classified():
    conn = _conn()
    _metadata(conn, "sess-skipped")
    _metadata(conn, "sess-no-project", project_path=None)
    _message(conn, "sess-no-project", project_path=None)
    _content(conn, ["msg-sess-no-project"])
    _metadata(conn, "sess-bad-time", session_timestamp="not-a-date")
    _message(conn, "sess-bad-time", timestamp="")
    _content(conn, ["msg-sess-bad-time"])
    _metadata(conn, "sess-no-artifact", source_log="/logs/no-artifact.jsonl")
    _message(conn, "sess-no-artifact", message_uuid="msg-no-artifact")

    report = build_claude_session_ingestion_gaps_report(conn, now=NOW)
    reasons = {row["session_id"]: row["reason_codes"] for row in report["findings"]}

    assert reasons["sess-skipped"] == ["missing_parsed_messages", "source_log_without_content_artifact"]
    assert reasons["sess-no-project"] == ["missing_project_path_metadata"]
    assert reasons["sess-bad-time"] == ["invalid_or_absent_session_timestamp"]
    assert reasons["sess-no-artifact"] == ["source_log_without_content_artifact"]
    assert report["summary"]["by_reason"]["source_log_without_content_artifact"] == 2


def test_project_filter_limit_and_json_are_deterministic():
    conn = _conn()
    _metadata(conn, "sess-a", project_path="/repo/a")
    _message(conn, "sess-a", project_path="/repo/a")
    _metadata(conn, "sess-b", project_path="/repo/b")
    _message(conn, "sess-b", project_path="/repo/b")

    report = build_claude_session_ingestion_gaps_report(conn, project_path="/repo/b", limit=1, now=NOW)
    payload = json.loads(format_claude_session_ingestion_gaps_json(report))

    assert list(payload) == sorted(payload)
    assert [row["session_id"] for row in payload["findings"]] == ["sess-b"]
    assert payload["filters"]["project_path"] == "/repo/b"
    assert payload["summary"]["finding_count"] == 1


def test_schema_gaps_are_reported_without_crashing():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE claude_messages (id INTEGER PRIMARY KEY, project_path TEXT)")

    report = build_claude_session_ingestion_gaps_report(conn, now=NOW)

    assert report["findings"] == []
    assert report["schema_gaps"]["missing_tables"] == ["ingestion_metadata", "generated_content"]
    assert report["schema_gaps"]["missing_columns"] == {"claude_messages": ["session_id", "timestamp"]}
    assert "Missing optional tables: ingestion_metadata, generated_content" in format_claude_session_ingestion_gaps_text(report)


def test_cli_json_text_and_validation(monkeypatch, capsys):
    conn = _conn()
    _metadata(conn, "sess-cli")
    _message(conn, "sess-cli")
    monkeypatch.setattr(script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        script,
        "build_claude_session_ingestion_gaps_report",
        lambda db, **kwargs: build_claude_session_ingestion_gaps_report(db, now=NOW, **kwargs),
    )

    assert script.main(["--lookback-days", "7", "--limit", "5", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["findings"][0]["session_id"] == "sess-cli"

    assert script.main(["--table"]) == 0
    assert "session=sess-cli" in capsys.readouterr().out

    with pytest.raises(SystemExit):
        script.parse_args(["--lookback-days", "0"])
