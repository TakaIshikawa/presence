"""Tests for Claude prompt repetition reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from ingestion.claude_prompt_repetition import (
    build_claude_prompt_repetition_report,
    format_claude_prompt_repetition_json,
    format_claude_prompt_repetition_text,
    normalize_prompt_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_prompt_repetition.py"
spec = importlib.util.spec_from_file_location("claude_prompt_repetition_script", SCRIPT_PATH)
claude_prompt_repetition_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_prompt_repetition_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _message_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_messages (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            message_uuid TEXT,
            project_path TEXT,
            timestamp TEXT,
            prompt_text TEXT
        )"""
    )
    return conn


def _insert_message(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    message_uuid: str,
    timestamp: str,
    prompt_text: str,
    project_path: str = "/repo/presence",
) -> None:
    conn.execute(
        """INSERT INTO claude_messages
           (session_id, message_uuid, project_path, timestamp, prompt_text)
           VALUES (?, ?, ?, ?, ?)""",
        (session_id, message_uuid, project_path, timestamp, prompt_text),
    )
    conn.commit()


def test_normalization_collapses_obvious_near_repeated_prompts():
    first = "Please fix tests/test_claude_prompt_repetition.py now!"
    second = "- fix ./tests/test_claude_prompt_repetition.py."

    assert normalize_prompt_text(first) == normalize_prompt_text(second)
    assert normalize_prompt_text(first) == "fix path"


def test_thresholding_flags_repeated_signatures_by_session_only():
    rows = [
        {
            "session_id": "sess-a",
            "message_uuid": "uuid-1",
            "project_path": "/repo/presence",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "prompt_text": "Please rerun pytest for tests/test_cli.py",
        },
        {
            "session_id": "sess-a",
            "message_uuid": "uuid-2",
            "project_path": "/repo/presence",
            "timestamp": "2026-05-01T10:04:00+00:00",
            "prompt_text": "rerun pytest for ./tests/test_cli.py!",
        },
        {
            "session_id": "sess-b",
            "message_uuid": "uuid-3",
            "project_path": "/repo/presence",
            "timestamp": "2026-05-01T10:05:00+00:00",
            "prompt_text": "Please rerun pytest for tests/test_cli.py",
        },
    ]

    report = build_claude_prompt_repetition_report(rows, threshold=2, now=NOW)

    assert report["totals"]["sessions_scanned"] == 2
    assert report["totals"]["sessions_flagged"] == 1
    session = report["sessions"][0]
    assert session["session_id"] == "sess-a"
    assert session["repeated_prompt_count"] == 2
    assert session["suggested_action"] == "investigate_loop"
    repeat = session["repeated_prompts"][0]
    assert repeat["count"] == 2
    assert repeat["first_timestamp"] == "2026-05-01T10:00:00+00:00"
    assert repeat["latest_timestamp"] == "2026-05-01T10:04:00+00:00"
    assert [example["message_uuid"] for example in repeat["examples"]] == ["uuid-1", "uuid-2"]


def test_project_path_filtering_applies_to_rows_and_database():
    rows = [
        {
            "session_id": "sess-a",
            "project_path": "/repo/presence",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "prompt_text": "Summarize the release notes.",
        },
        {
            "session_id": "sess-a",
            "project_path": "/repo/presence",
            "timestamp": "2026-05-01T10:10:00+00:00",
            "prompt_text": "summarize release notes",
        },
        {
            "session_id": "sess-b",
            "project_path": "/repo/other",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "prompt_text": "Summarize the release notes.",
        },
        {
            "session_id": "sess-b",
            "project_path": "/repo/other",
            "timestamp": "2026-05-01T10:10:00+00:00",
            "prompt_text": "summarize release notes",
        },
    ]

    row_report = build_claude_prompt_repetition_report(
        rows,
        project_path="/repo/presence",
        threshold=2,
        now=NOW,
    )

    conn = _message_db()
    for index, row in enumerate(rows, start=1):
        _insert_message(
            conn,
            session_id=str(row["session_id"]),
            message_uuid=f"uuid-{index}",
            project_path=str(row["project_path"]),
            timestamp=str(row["timestamp"]),
            prompt_text=str(row["prompt_text"]),
        )
    db_report = build_claude_prompt_repetition_report(
        conn,
        project_path="/repo/other",
        threshold=2,
        now=NOW,
    )

    assert row_report["filters"]["project_path_filter_applied"] is True
    assert [session["session_id"] for session in row_report["sessions"]] == ["sess-a"]
    assert db_report["filters"]["project_path_filter_applied"] is True
    assert [session["session_id"] for session in db_report["sessions"]] == ["sess-b"]


def test_json_output_is_deterministic_and_ordered_by_repeat_count():
    rows = [
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "prompt_text": "Review the changelog.",
        },
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:03:00+00:00",
            "prompt_text": "review changelog",
        },
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T09:00:00+00:00",
            "prompt_text": "Fix the deploy script.",
        },
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T09:01:00+00:00",
            "prompt_text": "fix deploy script",
        },
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T09:02:00+00:00",
            "prompt_text": "please fix deploy scripts",
        },
    ]

    first = build_claude_prompt_repetition_report(rows, threshold=2, now=NOW)
    second = build_claude_prompt_repetition_report(reversed(rows), threshold=2, now=NOW)
    payload = json.loads(format_claude_prompt_repetition_json(first))

    assert first == second
    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "claude_prompt_repetition"
    assert [session["session_id"] for session in payload["sessions"]] == ["sess-a", "sess-b"]
    assert payload["totals"]["repeated_prompt_instances"] == 5


def test_missing_claude_messages_table_returns_schema_gap():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_claude_prompt_repetition_report(conn, now=NOW)
    text = format_claude_prompt_repetition_text(report)

    assert report["schema_gaps"]["missing_tables"] == ["claude_messages"]
    assert report["totals"]["messages_scanned"] == 0
    assert "Missing tables: claude_messages" in text
    assert "No Claude prompt repetition found." in text


def test_cli_validates_positive_arguments_and_emits_json(db, monkeypatch, capsys):
    monkeypatch.setattr(
        claude_prompt_repetition_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        claude_prompt_repetition_script,
        "build_claude_prompt_repetition_report",
        lambda db, **kwargs: build_claude_prompt_repetition_report(
            [
                {
                    "session_id": "sess-cli",
                    "timestamp": "2026-05-01T10:00:00+00:00",
                    "project_path": "/repo/presence",
                    "prompt_text": "retry the failing pytest run",
                },
                {
                    "session_id": "sess-cli",
                    "timestamp": "2026-05-01T10:05:00+00:00",
                    "project_path": "/repo/presence",
                    "prompt_text": "please retry failing pytest runs",
                },
            ],
            now=NOW,
            **kwargs,
        ),
    )

    assert claude_prompt_repetition_script.main(["--threshold", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    exit_code = claude_prompt_repetition_script.main(
        [
            "--days",
            "7",
            "--threshold",
            "2",
            "--limit",
            "1",
            "--project-path",
            "/repo/presence",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["artifact_type"] == "claude_prompt_repetition"
    assert payload["filters"]["project_path"] == "/repo/presence"
    assert payload["sessions"][0]["session_id"] == "sess-cli"


def test_cli_text_output_includes_required_session_fields(db, monkeypatch, capsys):
    monkeypatch.setattr(
        claude_prompt_repetition_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        claude_prompt_repetition_script,
        "build_claude_prompt_repetition_report",
        lambda db, **kwargs: build_claude_prompt_repetition_report(
            [
                {
                    "session_id": "sess-text",
                    "timestamp": "2026-05-01T10:00:00+00:00",
                    "prompt_text": "Consolidate the session notes.",
                },
                {
                    "session_id": "sess-text",
                    "timestamp": "2026-05-01T11:00:00+00:00",
                    "prompt_text": "consolidate session notes",
                },
            ],
            now=NOW,
            **kwargs,
        ),
    )

    assert claude_prompt_repetition_script.main(["--format", "text"]) == 0
    output = capsys.readouterr().out

    assert "session=sess-text" in output
    assert "count=2" in output
    assert "first=2026-05-01T10:00:00+00:00" in output
    assert "latest=2026-05-01T11:00:00+00:00" in output
    assert "example: Consolidate the session notes." in output
    assert "action=consolidate_session_notes" in output
