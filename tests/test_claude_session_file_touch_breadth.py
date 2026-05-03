"""Tests for Claude session file-touch breadth reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from ingestion.claude_session_file_touch_breadth import (
    build_claude_session_file_touch_breadth_report,
    format_claude_session_file_touch_breadth_json,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
REPO_ROOT = Path("/Users/taka/project/presence")
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "claude_session_file_touch_breadth.py"
)
spec = importlib.util.spec_from_file_location(
    "claude_session_file_touch_breadth_script",
    SCRIPT_PATH,
)
claude_session_file_touch_breadth_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_file_touch_breadth_script)


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
            project_path TEXT,
            timestamp TEXT,
            tool_name TEXT,
            command TEXT,
            content TEXT,
            metadata TEXT
        )"""
    )
    return conn


def _insert_event(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    timestamp: str = "2026-05-01T10:00:00+00:00",
    project_path: str = str(REPO_ROOT),
    tool_name: str | None = "Read",
    command: str | None = None,
    content: str | None = None,
    metadata: str | dict | None = None,
) -> None:
    metadata_value = json.dumps(metadata, sort_keys=True) if isinstance(metadata, dict) else metadata
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, project_path, timestamp, tool_name, command, content, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (session_id, project_path, timestamp, tool_name, command, content, metadata_value),
    )
    conn.commit()


def test_narrow_session_counts_unique_files_extensions_and_commands():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-narrow",
        timestamp="2026-05-01T09:00:00+00:00",
        command="sed -n '1,120p' src/ingestion/claude_logs.py",
        metadata={"file_path": str(REPO_ROOT / "src/ingestion/claude_logs.py")},
    )
    _insert_event(
        conn,
        session_id="sess-narrow",
        timestamp="2026-05-01T10:00:00+00:00",
        command="uv run pytest tests/test_claude_logs.py",
        content="Update tests/test_claude_logs.py and src/ingestion/claude_logs.py",
    )

    report = build_claude_session_file_touch_breadth_report(
        conn,
        limit=10,
        repo_root=REPO_ROOT,
        now=NOW,
    )
    row = report.rows[0]

    assert row.session_id == "sess-narrow"
    assert row.breadth_classification == "narrow"
    assert row.unique_file_count == 2
    assert row.extension_breakdown == {".py": 2}
    assert row.command_count == 2
    assert row.top_directories == {"src": 1, "tests": 1}
    assert row.first_seen_at == "2026-05-01T09:00:00+00:00"
    assert row.last_seen_at == "2026-05-01T10:00:00+00:00"


def test_broad_session_detects_many_files_and_extension_breakdown_from_rows():
    rows = [
        {
            "session_id": "sess-broad",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "command": "edit broad files",
            "metadata": {
                "files": [
                    "src/a.py",
                    "src/b.py",
                    "tests/test_a.py",
                    "docs/a.md",
                    "scripts/a.py",
                    "config/app.toml",
                    "schemas/app.json",
                    "app/main.tsx",
                ]
            },
        }
    ]

    report = build_claude_session_file_touch_breadth_report(
        rows,
        limit=10,
        repo_root=REPO_ROOT,
        now=NOW,
    )
    row = report.rows[0]

    assert row.breadth_classification == "broad"
    assert row.unique_file_count == 8
    assert row.extension_breakdown == {
        ".json": 1,
        ".md": 1,
        ".py": 4,
        ".toml": 1,
        ".tsx": 1,
    }
    assert row.command_count == 1
    assert report.totals["broad_session_count"] == 1


def test_empty_no_file_session_is_reported_with_zero_counts():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-empty",
        timestamp="2026-05-01T11:00:00+00:00",
        tool_name="Bash",
        command="pwd",
        content="No file paths here.",
    )

    report = build_claude_session_file_touch_breadth_report(
        conn,
        limit=10,
        repo_root=REPO_ROOT,
        now=NOW,
    )
    row = report.rows[0]

    assert row.session_id == "sess-empty"
    assert row.breadth_classification == "no_files"
    assert row.unique_file_count == 0
    assert row.extension_breakdown == {}
    assert row.command_count == 1
    assert row.top_files == ()
    assert report.totals["no_file_session_count"] == 1


def test_json_output_is_deterministic_and_limit_applies_after_sorting():
    rows = [
        {
            "session_id": "sess-narrow",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "path": "src/one.py",
        },
        {
            "session_id": "sess-broad",
            "timestamp": "2026-05-01T11:00:00+00:00",
            "metadata": {"files": [f"src/file_{index}.py" for index in range(8)]},
        },
    ]

    report = build_claude_session_file_touch_breadth_report(
        rows,
        limit=1,
        repo_root=REPO_ROOT,
        now=NOW,
    )
    payload = json.loads(format_claude_session_file_touch_breadth_json(report))

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "claude_session_file_touch_breadth"
    assert payload["totals"]["session_count"] == 2
    assert [row["session_id"] for row in payload["rows"]] == ["sess-broad"]


def test_cli_argument_validation_and_script_context_json(db, monkeypatch, capsys):
    monkeypatch.setattr(
        claude_session_file_touch_breadth_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        claude_session_file_touch_breadth_script,
        "build_claude_session_file_touch_breadth_report",
        lambda db, **kwargs: build_claude_session_file_touch_breadth_report(
            [
                {
                    "session_id": "sess-cli",
                    "timestamp": "2026-05-01T10:00:00+00:00",
                    "path": "scripts/claude_session_file_touch_breadth.py",
                }
            ],
            now=NOW,
            **kwargs,
        ),
    )

    assert claude_session_file_touch_breadth_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    exit_code = claude_session_file_touch_breadth_script.main(
        ["--limit", "1", "--repo-root", str(REPO_ROOT)]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["artifact_type"] == "claude_session_file_touch_breadth"
    assert payload["rows"][0]["session_id"] == "sess-cli"
