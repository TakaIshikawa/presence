"""Tests for Claude unresolved-reference reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from ingestion.claude_unresolved_references import (
    build_claude_session_unresolved_reference_report,
    format_claude_unresolved_references_json,
    format_claude_unresolved_references_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_unresolved_references.py"
spec = importlib.util.spec_from_file_location("claude_unresolved_references_script", SCRIPT_PATH)
claude_unresolved_references_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_unresolved_references_script)


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
    timestamp: datetime | None = None,
) -> int:
    return db.insert_claude_message(
        session_id=session_id,
        message_uuid=message_uuid,
        project_path=project_path,
        timestamp=(timestamp or NOW).isoformat(),
        prompt_text=prompt_text,
    )


def test_report_flags_later_vague_references_without_nearby_anchors(db):
    _add_message(
        db,
        message_uuid="uuid-a1",
        session_id="sess-a",
        timestamp=NOW - timedelta(hours=4),
        prompt_text="Review the ingestion quality report implementation.",
    )
    _add_message(
        db,
        message_uuid="uuid-a2",
        session_id="sess-a",
        timestamp=NOW - timedelta(hours=3),
        prompt_text="That error is still happening. Fix it and check the same issue.",
    )
    _add_message(
        db,
        message_uuid="uuid-b1",
        session_id="sess-b",
        timestamp=NOW - timedelta(hours=2),
        prompt_text="Run `uv run pytest tests/test_report.py::test_handles_errors`.",
    )
    _add_message(
        db,
        message_uuid="uuid-b2",
        session_id="sess-b",
        timestamp=NOW - timedelta(hours=1),
        prompt_text="The failing test still fails after the fixture change.",
    )

    report = build_claude_session_unresolved_reference_report(db, days=7, now=NOW)
    payload = json.loads(format_claude_unresolved_references_json(report))
    text = format_claude_unresolved_references_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "claude_unresolved_references"
    assert payload["totals"] == {
        "messages_scanned": 4,
        "sessions_flagged": 1,
        "sessions_scanned": 2,
        "unresolved_references": 1,
        "vague_references": 4,
    }
    assert [session["session_id"] for session in payload["sessions"]] == ["sess-a"]
    flagged = payload["sessions"][0]
    assert flagged["recommendation"] == "add_context_anchor"
    assert flagged["has_unresolved_references"] is True
    assert flagged["vague_reference_density"] == 0.5
    finding = flagged["findings"][0]
    assert finding["message_uuid"] == "uuid-a2"
    assert finding["recommendation"] == "add_context_anchor"
    assert finding["anchor_evidence"] == []
    assert finding["unresolved_score"] > 0.7
    assert finding["vague_references"] == ["fix_it", "same_issue", "that_error"]
    assert "That error is still happening" in finding["excerpt"]
    assert "Claude Unresolved References" in text
    assert "session=sess-a" in text
    assert "score=" in text
    assert "recommendation=add_context_anchor" in text
    assert "excerpt: That error is still happening" in text


def test_rows_input_project_filter_limit_and_sorting_are_deterministic():
    rows = [
        {
            "id": 3,
            "session_id": "sess-low",
            "message_uuid": "uuid-low-2",
            "project_path": "/repo/presence",
            "timestamp": (NOW - timedelta(hours=2)).isoformat(),
            "prompt_text": "The previous failure needs another look.",
        },
        {
            "id": 1,
            "session_id": "sess-low",
            "message_uuid": "uuid-low-1",
            "project_path": "/repo/presence",
            "timestamp": (NOW - timedelta(hours=3)).isoformat(),
            "prompt_text": "Inspect the newsletter report behavior.",
        },
        {
            "id": 5,
            "session_id": "sess-high",
            "message_uuid": "uuid-high-2",
            "project_path": "/repo/presence",
            "timestamp": NOW.isoformat(),
            "prompt_text": "That error and the previous failure are the same issue.",
        },
        {
            "id": 4,
            "session_id": "sess-high",
            "message_uuid": "uuid-high-1",
            "project_path": "/repo/presence",
            "timestamp": (NOW - timedelta(hours=1)).isoformat(),
            "prompt_text": "Review ingestion report behavior.",
        },
        {
            "id": 6,
            "session_id": "sess-other",
            "message_uuid": "uuid-other",
            "project_path": "/repo/other",
            "timestamp": NOW.isoformat(),
            "prompt_text": "That error is back.",
        },
    ]

    report = build_claude_session_unresolved_reference_report(
        rows,
        days=7,
        project_path="/repo/presence",
        limit=1,
        now=NOW,
    )
    payload = report.to_dict()

    assert payload["filters"]["project_path_filter_applied"] is True
    assert payload["totals"]["sessions_scanned"] == 2
    assert payload["totals"]["sessions_flagged"] == 2
    assert len(payload["sessions"]) == 1
    assert payload["sessions"][0]["session_id"] == "sess-high"
    assert payload["sessions"][0]["findings"][0]["vague_references"] == [
        "previous_failure",
        "same_issue",
        "that_error",
    ]


def test_missing_schema_gaps_return_valid_reports():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_claude_session_unresolved_reference_report(conn, now=NOW)

    assert report.sessions == ()
    assert report.missing_tables == ("claude_messages",)
    assert report.totals["sessions_scanned"] == 0
    assert "Missing tables: claude_messages" in format_claude_unresolved_references_text(report)

    conn.execute(
        """CREATE TABLE claude_messages (
               id INTEGER PRIMARY KEY,
               session_id TEXT,
               timestamp TEXT,
               prompt_text TEXT
           )"""
    )
    conn.execute(
        """INSERT INTO claude_messages (session_id, timestamp, prompt_text)
           VALUES ('sess-legacy', '2026-05-02T10:00:00+00:00', 'That error is back')"""
    )
    legacy_report = build_claude_session_unresolved_reference_report(
        conn,
        project_path="/repo/presence",
        now=NOW,
    )

    assert legacy_report.missing_columns == {
        "claude_messages": ("message_uuid", "project_path")
    }
    assert legacy_report.filters["project_path_filter_applied"] is False
    assert legacy_report.totals["messages_scanned"] == 1
    assert legacy_report.totals["sessions_flagged"] == 0


def test_cli_json_text_and_invalid_args_are_stable(db, monkeypatch, capsys):
    _add_message(
        db,
        message_uuid="uuid-cli-1",
        session_id="sess-cli",
        timestamp=NOW - timedelta(minutes=2),
        prompt_text="Review the report output.",
    )
    _add_message(
        db,
        message_uuid="uuid-cli-2",
        session_id="sess-cli",
        timestamp=NOW - timedelta(minutes=1),
        prompt_text="The previous failure is still the same issue.",
    )
    monkeypatch.setattr(
        claude_unresolved_references_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        claude_unresolved_references_script,
        "build_claude_session_unresolved_reference_report",
        lambda db, **kwargs: build_claude_session_unresolved_reference_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    assert claude_unresolved_references_script.main(
        ["--days", "7", "--limit", "5", "--format", "json"]
    ) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["sessions"][0]["session_id"] == "sess-cli"
    assert cli_payload["totals"]["sessions_flagged"] == 1

    assert claude_unresolved_references_script.main(["--format", "text"]) == 0
    assert "session=sess-cli" in capsys.readouterr().out

    assert claude_unresolved_references_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert claude_unresolved_references_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
