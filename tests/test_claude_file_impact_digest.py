"""Tests for Claude file impact digest reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from ingestion.claude_file_impact_digest import (
    build_claude_file_impact_digest,
    format_claude_file_impact_digest_json,
    format_claude_file_impact_digest_markdown,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
REPO_ROOT = Path("/Users/taka/project/presence")
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_file_impact_digest.py"
spec = importlib.util.spec_from_file_location("claude_file_impact_digest_script", SCRIPT_PATH)
claude_file_impact_digest_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_file_impact_digest_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _event_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            project_path TEXT,
            tool_name TEXT,
            status TEXT,
            content TEXT,
            error_message TEXT,
            metadata TEXT
        )"""
    )
    return conn


def _insert_event(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    timestamp: str,
    project_path: str = str(REPO_ROOT),
    tool_name: str | None = None,
    status: str = "ok",
    content: str | None = None,
    error_message: str | None = None,
    metadata: str | dict | None = None,
) -> None:
    metadata_value = json.dumps(metadata, sort_keys=True) if isinstance(metadata, dict) else metadata
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, timestamp, project_path, tool_name, status, content, error_message, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (session_id, timestamp, project_path, tool_name, status, content, error_message, metadata_value),
    )
    conn.commit()


def test_digest_groups_parsed_tool_events_by_normalized_file_path():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-a",
        timestamp="2026-05-01T10:00:00+00:00",
        tool_name="Read",
        metadata={"file_path": str(REPO_ROOT / "src/ingestion/claude_logs.py")},
    )
    _insert_event(
        conn,
        session_id="sess-b",
        timestamp="2026-05-01T11:00:00+00:00",
        tool_name="Edit",
        metadata={"path": "./src/ingestion/claude_logs.py:42"},
    )
    _insert_event(
        conn,
        session_id="sess-c",
        timestamp="2026-05-01T12:00:00+00:00",
        tool_name="Bash",
        status="failed",
        error_message=f"pytest failed for {REPO_ROOT / 'src/ingestion/claude_logs.py'}",
    )

    report = build_claude_file_impact_digest(conn, days=7, repo_root=REPO_ROOT, now=NOW)

    first = report["files"][0]
    assert first["path"] == "src/ingestion/claude_logs.py"
    assert first["mention_count"] == 3
    assert first["tool_count"] == 3
    assert first["error_count"] == 1
    assert first["latest_seen"] == "2026-05-01T12:00:00+00:00"
    assert first["recommendation"].startswith("Stabilize recent failures")


def test_digest_extracts_paths_from_raw_transcript_snippets_and_ranks_impact():
    rows = [
        {
            "session_id": "sess-raw",
            "timestamp": "2026-05-01T09:00:00+00:00",
            "message": "Need tests/test_claude_file_impact_digest.py for scripts/claude_file_impact_digest.py",
        },
        {
            "session_id": "sess-raw",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "content": "Review scripts/claude_file_impact_digest.py and README.md.",
        },
    ]

    report = build_claude_file_impact_digest(rows, days=7, repo_root=REPO_ROOT, now=NOW)

    paths = [item["path"] for item in report["files"]]
    assert paths[:2] == ["scripts/claude_file_impact_digest.py", "README.md"]
    script = report["files"][0]
    assert script["mention_count"] == 2
    assert script["tool_count"] == 0
    assert "Document ownership" in script["recommendation"]


def test_path_normalization_uses_row_project_path_and_skips_external_absolute_paths():
    rows = [
        {
            "session_id": "sess-path",
            "timestamp": "2026-05-01T09:00:00+00:00",
            "project_path": "/tmp/work/project",
            "tool_name": "Write",
            "path": "/tmp/work/project/docs/guide.md:12",
        },
        {
            "session_id": "sess-path",
            "timestamp": "2026-05-01T09:30:00+00:00",
            "tool_name": "Read",
            "path": "/tmp/outside/secret.md",
        },
    ]

    report = build_claude_file_impact_digest(rows, days=7, repo_root=REPO_ROOT, now=NOW)

    assert [item["path"] for item in report["files"]] == ["docs/guide.md"]
    assert report["files"][0]["tool_count"] == 1


def test_json_output_and_markdown_include_artifact_totals_and_recommendations():
    rows = [
        {
            "session_id": "sess-json",
            "timestamp": "2026-05-01T09:00:00+00:00",
            "tool_name": "Edit",
            "path": "tests/test_claude_file_impact_digest.py",
        }
    ]

    report = build_claude_file_impact_digest(rows, days=7, repo_root=REPO_ROOT, now=NOW)
    payload = json.loads(format_claude_file_impact_digest_json(report))
    markdown = format_claude_file_impact_digest_markdown(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "claude_file_impact_digest"
    assert payload["filters"]["days"] == 7
    assert payload["totals"]["file_count"] == 1
    assert payload["files"][0]["path"] == "tests/test_claude_file_impact_digest.py"
    assert "# Claude File Impact Digest" in markdown
    assert "## Highest Impact Files" in markdown
    assert "Recommendation:" in markdown


def test_falls_back_to_claude_messages_table_when_event_tables_are_missing():
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
    conn.execute(
        """INSERT INTO claude_messages
           (session_id, message_uuid, project_path, timestamp, prompt_text)
           VALUES (?, ?, ?, ?, ?)""",
        (
            "sess-message",
            "uuid-1",
            str(REPO_ROOT),
            "2026-05-01T10:00:00+00:00",
            "Open src/ingestion/claude_logs.py and docs/claude.md",
        ),
    )
    conn.commit()

    report = build_claude_file_impact_digest(conn, days=7, repo_root=REPO_ROOT, now=NOW)

    assert report["source_table"] == "claude_messages"
    assert [item["path"] for item in report["files"]] == [
        "docs/claude.md",
        "src/ingestion/claude_logs.py",
    ]


def test_cli_argument_validation_and_script_context_json(db, monkeypatch, capsys):
    monkeypatch.setattr(
        claude_file_impact_digest_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        claude_file_impact_digest_script,
        "build_claude_file_impact_digest",
        lambda db, **kwargs: build_claude_file_impact_digest(
            [
                {
                    "session_id": "sess-cli",
                    "timestamp": "2026-05-01T10:00:00+00:00",
                    "path": "scripts/claude_file_impact_digest.py",
                }
            ],
            now=NOW,
            **kwargs,
        ),
    )

    assert claude_file_impact_digest_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    exit_code = claude_file_impact_digest_script.main(
        ["--limit", "1", "--repo-root", str(REPO_ROOT), "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["artifact_type"] == "claude_file_impact_digest"
    assert payload["files"][0]["path"] == "scripts/claude_file_impact_digest.py"
