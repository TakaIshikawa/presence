"""Tests for exporting Claude Code session interruption markers."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from synthesis.claude_session_interruptions import (
    export_claude_session_interruptions,
    format_claude_session_interruptions_json,
    format_claude_session_interruptions_markdown,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "export_claude_session_interruptions.py"
spec = importlib.util.spec_from_file_location("export_claude_session_interruptions_script", SCRIPT_PATH)
export_claude_session_interruptions_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(export_claude_session_interruptions_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _add_message(
    db,
    *,
    message_uuid: str,
    prompt_text: str,
    session_id: str = "sess-1",
    project_path: str = "/repo/presence",
    timestamp: str = "2026-05-01T12:00:00+00:00",
) -> int:
    return db.insert_claude_message(
        session_id=session_id,
        message_uuid=message_uuid,
        project_path=project_path,
        timestamp=timestamp,
        prompt_text=prompt_text,
    )


def test_detects_representative_interruption_marker_types_from_session_text():
    records = export_claude_session_interruptions(
        [
            {
                "session_id": "sess-markers",
                "message_uuid": "uuid-markers",
                "project_path": "/repo/presence",
                "timestamp": "2026-05-01T12:00:00+00:00",
                "transcript": """
                Tool call Bash was aborted before pytest finished.
                User cancelled the run after the migration warning.
                TODO: add regression tests for the interruption exporter.
                Remaining work: wire the markdown formatter into the CLI.
                """,
            }
        ],
        now=NOW,
    )

    by_type = {record.marker_type: record for record in records}

    assert set(by_type) == {
        "aborted_tool_call",
        "user_cancellation",
        "todo_handoff",
        "unfinished_plan",
    }
    assert by_type["todo_handoff"].session_id == "sess-markers"
    assert by_type["aborted_tool_call"].project_path == "/repo/presence"
    assert by_type["unfinished_plan"].excerpt


def test_duplicate_markers_from_same_session_are_collapsed_deterministically():
    text = "TODO: add JSON export for Claude interruptions."
    rows = [
        {
            "session_id": "sess-a",
            "message_uuid": "uuid-1",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "prompt_text": text,
        },
        {
            "session_id": "sess-a",
            "message_uuid": "uuid-2",
            "timestamp": "2026-05-01T11:00:00+00:00",
            "prompt_text": text,
        },
        {
            "session_id": "sess-b",
            "message_uuid": "uuid-3",
            "timestamp": "2026-05-01T11:00:00+00:00",
            "prompt_text": text,
        },
    ]

    first = export_claude_session_interruptions(rows, now=NOW)
    second = export_claude_session_interruptions(reversed(rows), now=NOW)

    assert len(first) == 2
    assert [record.to_dict() for record in first] == [record.to_dict() for record in second]
    assert {record.session_id for record in first} == {"sess-a", "sess-b"}


def test_priority_increases_for_todos_and_recent_unfinished_plans():
    records = export_claude_session_interruptions(
        [
            {
                "session_id": "sess-old",
                "timestamp": "2026-04-20T12:00:00+00:00",
                "prompt_text": "Remaining work: finish the source loader.",
            },
            {
                "session_id": "sess-recent",
                "timestamp": "2026-05-01T12:00:00+00:00",
                "prompt_text": "Remaining work: finish the source loader.",
            },
            {
                "session_id": "sess-todo",
                "timestamp": "2026-05-01T12:00:00+00:00",
                "prompt_text": "TODO: implement tests for cancelled tool calls.",
            },
        ],
        since="2026-04-01",
        now=NOW,
    )
    by_session = {record.session_id: record for record in records}

    assert by_session["sess-recent"].priority > by_session["sess-old"].priority
    assert by_session["sess-todo"].priority > by_session["sess-recent"].priority
    assert by_session["sess-todo"].suggested_follow_up_priority == by_session["sess-todo"].priority


def test_since_filters_database_rows(db):
    _add_message(
        db,
        message_uuid="uuid-old",
        timestamp="2026-04-01T12:00:00+00:00",
        prompt_text="TODO: add an old follow-up.",
    )
    _add_message(
        db,
        message_uuid="uuid-new",
        timestamp="2026-05-01T12:00:00+00:00",
        prompt_text="User cancelled the export run.",
    )

    records = export_claude_session_interruptions(db, since="2026-05-01T00:00:00+00:00", now=NOW)

    assert len(records) == 1
    assert records[0].message_uuid == "uuid-new"


def test_formatters_and_cli_emit_json_by_default_and_markdown_when_requested(db, monkeypatch, capsys):
    _add_message(
        db,
        message_uuid="uuid-cli",
        prompt_text="TODO: document Claude interruption export.",
    )
    monkeypatch.setattr(
        export_claude_session_interruptions_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        export_claude_session_interruptions_script,
        "export_claude_session_interruptions",
        lambda db, **kwargs: export_claude_session_interruptions(db, now=NOW, **kwargs),
    )

    records = export_claude_session_interruptions(db, now=NOW)
    payload = json.loads(format_claude_session_interruptions_json(records))
    markdown = format_claude_session_interruptions_markdown(records)

    assert payload[0]["marker_type"] == "todo_handoff"
    assert "# Claude Session Interruptions" in markdown

    assert export_claude_session_interruptions_script.main(["--since", "2026-04-01"]) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload[0]["message_uuid"] == "uuid-cli"

    assert export_claude_session_interruptions_script.main(["--format", "markdown"]) == 0
    cli_markdown = capsys.readouterr().out
    assert "## todo_handoff - priority" in cli_markdown
