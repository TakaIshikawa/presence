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


def test_json_export_format_contains_all_required_fields():
    """Regression test: JSON export must include all interruption record fields."""
    records = export_claude_session_interruptions(
        [
            {
                "session_id": "sess-json",
                "message_uuid": "uuid-json",
                "project_path": "/repo/test",
                "timestamp": "2026-05-01T14:00:00+00:00",
                "transcript": "TODO: add JSON export for Claude interruptions.",
            }
        ],
        now=NOW,
    )

    json_output = format_claude_session_interruptions_json(records)
    payload = json.loads(json_output)

    assert len(payload) == 1
    record = payload[0]
    required_fields = {
        "interruption_id",
        "session_id",
        "timestamp",
        "project_path",
        "excerpt",
        "marker_type",
        "priority",
        "suggested_follow_up_priority",
        "reason",
        "message_id",
        "message_uuid",
    }
    assert set(record.keys()) == required_fields
    assert record["session_id"] == "sess-json"
    assert record["message_uuid"] == "uuid-json"
    assert record["project_path"] == "/repo/test"
    assert record["marker_type"] == "todo_handoff"
    assert record["priority"] >= 1
    assert "JSON export" in record["excerpt"]


def test_json_export_is_deterministic_and_sorted():
    """Regression test: JSON export must be deterministic with consistent sorting."""
    records = export_claude_session_interruptions(
        [
            {
                "session_id": "sess-b",
                "message_uuid": "uuid-b",
                "timestamp": "2026-05-01T15:00:00+00:00",
                "transcript": "TODO: second task.",
            },
            {
                "session_id": "sess-a",
                "message_uuid": "uuid-a",
                "timestamp": "2026-05-01T14:00:00+00:00",
                "transcript": "User cancelled the build.",
            },
        ],
        now=NOW,
    )

    first_export = format_claude_session_interruptions_json(records)
    second_export = format_claude_session_interruptions_json(records)

    assert first_export == second_export
    payload = json.loads(first_export)
    assert len(payload) == 2
    # JSON keys should be sorted alphabetically
    for record_dict in payload:
        keys = list(record_dict.keys())
        assert keys == sorted(keys)


def test_cancelled_tool_call_detection_with_various_patterns():
    """Regression test: Detect cancelled tool calls with different phrasing patterns."""
    test_cases = [
        ("Tool call Bash was aborted before completion.", "aborted_tool_call"),
        ("The Edit tool use was interrupted by user.", "aborted_tool_call"),
        ("Command execution terminated early.", "aborted_tool_call"),
        ("MCP tool_use was aborted mid-run.", "aborted_tool_call"),
        ("Read tool call interrupted before finishing.", "aborted_tool_call"),
    ]

    for text, expected_type in test_cases:
        records = export_claude_session_interruptions(
            [
                {
                    "session_id": "sess-tool",
                    "message_uuid": f"uuid-{hash(text)}",
                    "timestamp": "2026-05-01T12:00:00+00:00",
                    "transcript": text,
                }
            ],
            now=NOW,
        )
        assert len(records) == 1, f"Failed to detect: {text}"
        assert records[0].marker_type == expected_type, f"Wrong type for: {text}"
        assert records[0].priority >= 3, f"Priority too low for: {text}"


def test_cancelled_tool_call_with_implementation_context_increases_priority():
    """Regression test: Cancelled tool calls with work context get priority boost."""
    without_context = export_claude_session_interruptions(
        [
            {
                "session_id": "sess-1",
                "message_uuid": "uuid-1",
                "timestamp": "2026-05-01T12:00:00+00:00",
                "transcript": "Read was aborted during processing.",
            }
        ],
        now=NOW,
    )[0]

    with_context = export_claude_session_interruptions(
        [
            {
                "session_id": "sess-2",
                "message_uuid": "uuid-2",
                "timestamp": "2026-05-01T12:00:00+00:00",
                "transcript": "Read was aborted while attempting to run tests.",
            }
        ],
        now=NOW,
    )[0]

    assert with_context.priority > without_context.priority
    assert "work context" in with_context.reason
    assert "work context" not in without_context.reason


def test_old_follow_up_tracking_with_timestamp_decay():
    """Regression test: Old follow-ups are tracked with proper timestamp ordering."""
    very_old = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    old = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    recent = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)

    # Use different excerpts to avoid deduplication, and disable since filter with empty string
    records = export_claude_session_interruptions(
        [
            {
                "session_id": "sess-very-old",
                "message_uuid": "uuid-very-old",
                "timestamp": very_old.isoformat(),
                "transcript": "TODO: add very old feature tracking.",
            },
            {
                "session_id": "sess-old",
                "message_uuid": "uuid-old",
                "timestamp": old.isoformat(),
                "transcript": "TODO: add old feature implementation.",
            },
            {
                "session_id": "sess-recent",
                "message_uuid": "uuid-recent",
                "timestamp": recent.isoformat(),
                "transcript": "TODO: add recent feature support.",
            },
        ],
        since="",
        now=NOW,
    )

    assert len(records) == 3

    # All should be detected as todo_handoff
    for record in records:
        assert record.marker_type == "todo_handoff"

    # All have same priority, sorted by timestamp ascending
    assert all(r.priority == records[0].priority for r in records)
    # Verify chronological ordering (earliest first)
    timestamps = [r.timestamp for r in records]
    assert timestamps == sorted(timestamps)


def test_old_follow_ups_are_included_when_since_filter_permits():
    """Regression test: Old follow-ups included if within since filter range."""
    records_with_filter = export_claude_session_interruptions(
        [
            {
                "session_id": "sess-old",
                "message_uuid": "uuid-old",
                "timestamp": "2026-03-01T12:00:00+00:00",
                "transcript": "TODO: implement old feature.",
            },
            {
                "session_id": "sess-new",
                "message_uuid": "uuid-new",
                "timestamp": "2026-05-01T12:00:00+00:00",
                "transcript": "TODO: implement new feature.",
            },
        ],
        since="2026-04-01T00:00:00+00:00",
        now=NOW,
    )

    assert len(records_with_filter) == 1
    assert records_with_filter[0].session_id == "sess-new"

    records_without_filter = export_claude_session_interruptions(
        [
            {
                "session_id": "sess-old",
                "message_uuid": "uuid-old",
                "timestamp": "2026-03-01T12:00:00+00:00",
                "transcript": "TODO: implement old feature.",
            },
            {
                "session_id": "sess-new",
                "message_uuid": "uuid-new",
                "timestamp": "2026-05-01T12:00:00+00:00",
                "transcript": "TODO: implement new feature.",
            },
        ],
        since="2026-02-01T00:00:00+00:00",
        now=NOW,
    )

    assert len(records_without_filter) == 2


def test_export_consistency_across_database_and_dict_formats():
    """Regression test: Export produces identical results from db and dict rows."""
    db_data = [
        {
            "session_id": "sess-1",
            "message_uuid": "uuid-1",
            "project_path": "/repo",
            "timestamp": "2026-05-01T12:00:00+00:00",
            "prompt_text": "TODO: test database format.",
        }
    ]

    dict_data = [
        {
            "session_id": "sess-1",
            "message_uuid": "uuid-1",
            "project_path": "/repo",
            "timestamp": "2026-05-01T12:00:00+00:00",
            "transcript": "TODO: test database format.",
        }
    ]

    records_from_dicts = export_claude_session_interruptions(dict_data, now=NOW)
    # Simulate what would come from db by using prompt_text key
    records_from_db = export_claude_session_interruptions(db_data, now=NOW)

    assert len(records_from_dicts) == 1
    assert len(records_from_db) == 1
    assert records_from_dicts[0].marker_type == records_from_db[0].marker_type
    assert records_from_dicts[0].excerpt == records_from_db[0].excerpt
    assert records_from_dicts[0].priority == records_from_db[0].priority


def test_export_handles_multiple_content_field_formats():
    """Regression test: Export handles transcript, prompt_text, content, etc."""
    test_cases = [
        {"transcript": "TODO: test transcript field."},
        {"prompt_text": "TODO: test prompt_text field."},
        {"content": "TODO: test content field."},
        {"text": "TODO: test text field."},
        {"message": "TODO: test message field."},
    ]

    for row_data in test_cases:
        full_row = {
            "session_id": "sess-test",
            "message_uuid": "uuid-test",
            "timestamp": "2026-05-01T12:00:00+00:00",
            **row_data,
        }
        records = export_claude_session_interruptions([full_row], now=NOW)
        assert len(records) == 1, f"Failed to parse {row_data}"
        assert records[0].marker_type == "todo_handoff", f"Wrong marker for {row_data}"


def test_regression_all_marker_types_are_exported():
    """Regression test: All four marker types are detected and exported."""
    records = export_claude_session_interruptions(
        [
            {
                "session_id": "sess-all",
                "message_uuid": "uuid-all",
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

    marker_types = {r.marker_type for r in records}
    expected_types = {"aborted_tool_call", "user_cancellation", "todo_handoff", "unfinished_plan"}
    assert marker_types == expected_types, f"Missing types: {expected_types - marker_types}"
