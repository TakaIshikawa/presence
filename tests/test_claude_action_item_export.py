"""Tests for exporting Claude Code action items as content idea seeds."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from synthesis.claude_action_item_export import (
    SOURCE_NAME,
    build_claude_action_item_exports,
    export_claude_action_items_json,
    extract_claude_action_items_from_text,
    format_claude_action_item_exports_json,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "export_claude_action_items.py"
spec = importlib.util.spec_from_file_location("export_claude_action_items_script", SCRIPT_PATH)
export_claude_action_items_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(export_claude_action_items_script)


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
    timestamp: str = "2026-04-30T12:00:00+00:00",
) -> int:
    return db.insert_claude_message(
        session_id=session_id,
        message_uuid=message_uuid,
        project_path=project_path,
        timestamp=timestamp,
        prompt_text=prompt_text,
    )


def test_extracts_action_items_with_session_reference_confidence_and_angle():
    exports = extract_claude_action_items_from_text(
        """
        Open question: should we expose this as a command?
        TODO: add JSON export for Claude action items.
        Decision: keep this separate from unresolved-question mining.
        """,
        session_metadata={
            "session_id": "sess-plain",
            "session_path": "/tmp/claude/sess-plain.jsonl",
            "project_path": "/repo/presence",
            "message_uuid": "uuid-plain",
            "timestamp": "2026-04-30T12:00:00+00:00",
        },
        min_confidence=0.65,
    )

    assert len(exports) == 2
    first = exports[0]
    assert first.action_item_id.startswith("claude_action_")
    assert first.session_id == "sess-plain"
    assert first.session_path == "/tmp/claude/sess-plain.jsonl"
    assert first.message_uuid == "uuid-plain"
    assert "JSON export" in first.action_item
    assert first.confidence >= 0.9
    assert "content workflow" in first.suggested_content_angle
    assert first.source_metadata["source"] == SOURCE_NAME
    assert "Open question" not in {export.action_item for export in exports}


def test_excludes_questions_only_and_uncertain_unresolved_entries(db):
    kept_id = _add_message(
        db,
        message_uuid="uuid-1",
        prompt_text="Next step: implement a CLI wrapper for action item export.",
    )
    _add_message(
        db,
        message_uuid="uuid-2",
        prompt_text="Should the exporter include unresolved questions?",
    )
    _add_message(
        db,
        message_uuid="uuid-3",
        prompt_text="TODO: investigate whether the schema cache can be invalidated per tenant.",
    )

    exports = build_claude_action_item_exports(
        db,
        days=7,
        min_confidence=0.68,
        now=NOW,
    )

    assert len(exports) == 1
    assert exports[0].message_id == kept_id
    assert "CLI wrapper" in exports[0].action_item


def test_duplicate_action_items_from_same_session_are_collapsed(db):
    text = "TODO: add JSON export for Claude action items."
    _add_message(db, message_uuid="uuid-1", session_id="sess-a", prompt_text=text)
    _add_message(db, message_uuid="uuid-2", session_id="sess-a", prompt_text=text)
    _add_message(db, message_uuid="uuid-3", session_id="sess-b", prompt_text=text)

    exports = build_claude_action_item_exports(
        db,
        days=7,
        min_confidence=0.65,
        now=NOW,
    )

    assert len(exports) == 2
    assert {export.session_id for export in exports} == {"sess-a", "sess-b"}
    assert len({export.action_item_id for export in exports}) == 2


def test_json_formatter_includes_required_export_fields():
    exports = extract_claude_action_items_from_text(
        "Follow-up: verify publication retry tests before shipping.",
        session_metadata={"session_id": "sess-json", "project_path": "/repo/presence"},
        min_confidence=0.65,
    )

    payload = json.loads(format_claude_action_item_exports_json(exports))

    assert payload[0]["session_id"] == "sess-json"
    assert payload[0]["project_path"] == "/repo/presence"
    assert payload[0]["excerpt"]
    assert payload[0]["confidence"] >= 0.65
    assert "quality-gate" in payload[0]["suggested_content_angle"]


def test_export_claude_action_items_json_uses_schema_fields():
    payload = json.loads(
        export_claude_action_items_json(
            "TODO: add JSON export for Claude action items.",
            session_metadata={
                "session_id": "sess-json-export",
                "timestamp": "2026-04-30T12:00:00+00:00",
            },
            min_confidence=0.65,
        )
    )

    assert set(payload[0]) == {
        "action_id",
        "prompt_text",
        "confidence_score",
        "status",
        "created_at",
        "resolved_at",
    }
    assert payload[0]["action_id"].startswith("claude_action_")
    assert "JSON export" in payload[0]["prompt_text"]
    assert payload[0]["confidence_score"] >= 0.65
    assert payload[0]["status"] == "open"
    assert payload[0]["created_at"] == "2026-04-30T12:00:00+00:00"
    assert payload[0]["resolved_at"] is None


def test_export_claude_action_items_json_filters_by_min_confidence_and_edge_cases():
    payload = json.loads(
        export_claude_action_items_json(
            """
            TODO: add JSON export for Claude action items.
            Should the exporter include unresolved questions?
            """,
            min_confidence=0.9,
        )
    )

    assert len(payload) == 1
    assert "unresolved questions" not in payload[0]["prompt_text"]
    assert json.loads(export_claude_action_items_json("", min_confidence=0.65)) == []


def test_export_claude_action_items_json_validates_status():
    with pytest.raises(ValueError, match="status"):
        export_claude_action_items_json(
            "TODO: add tests.",
            min_confidence=0.65,
            status="unknown",
        )


def test_cli_supports_json_output_and_min_confidence(db, monkeypatch, capsys):
    _add_message(
        db,
        message_uuid="uuid-1",
        prompt_text="Action item: document the Claude session action item exporter.",
    )
    monkeypatch.setattr(
        export_claude_action_items_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        export_claude_action_items_script,
        "build_claude_action_item_exports",
        lambda db, **kwargs: build_claude_action_item_exports(db, now=NOW, **kwargs),
    )

    exit_code = export_claude_action_items_script.main(
        ["--days", "7", "--limit", "5", "--min-confidence", "0.7", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload[0]["source_metadata"]["source"] == SOURCE_NAME
    assert payload[0]["confidence"] >= 0.7


def test_invalid_min_confidence_is_rejected():
    with pytest.raises(ValueError, match="min_confidence"):
        extract_claude_action_items_from_text("TODO: add tests.", min_confidence=1.2)
