"""Tests for Claude prompt correction loop reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

from ingestion.claude_session_prompt_correction_loops import (
    build_claude_session_prompt_correction_loops_report,
    format_claude_session_prompt_correction_loops_json,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "claude_session_prompt_correction_loops.py"
)
spec = importlib.util.spec_from_file_location(
    "claude_session_prompt_correction_loops_script",
    SCRIPT_PATH,
)
claude_session_prompt_correction_loops_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_prompt_correction_loops_script)


def test_tool_error_correction_loop_is_grouped_by_session_and_trigger():
    rows = [
        {
            "session_id": "sess-tool",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Bash",
            "status": "failed",
            "content": "Command failed with exit code 1",
        },
        {
            "session_id": "sess-tool",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "role": "user",
            "content": "Try again with the package test command.",
        },
        {
            "session_id": "sess-tool",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "role": "assistant",
            "content": "I will try again with npm test.",
        },
    ]

    report = build_claude_session_prompt_correction_loops_report(rows, now=NOW)
    payload = json.loads(format_claude_session_prompt_correction_loops_json(report))
    row = report.rows[0]

    assert payload["artifact_type"] == "claude_session_prompt_correction_loops"
    assert list(payload) == sorted(payload)
    assert row.session_id == "sess-tool"
    assert row.trigger_type == "tool_error"
    assert row.correction_kind == "retry_request"
    assert row.correction_count == 2
    assert row.trigger_snippets == ("Command failed with exit code 1",)


def test_user_clarification_loop_does_not_require_prior_tool_error():
    rows = [
        {
            "session_id": "sess-user",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "role": "user",
            "content": "I meant the ingestion report, not the newsletter report.",
        },
        {
            "session_id": "sess-user",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "role": "user",
            "content": "To clarify, use the ingestion files instead.",
        },
    ]

    report = build_claude_session_prompt_correction_loops_report(rows, now=NOW)

    assert len(report.rows) == 1
    assert report.rows[0].trigger_type == "user_clarification"
    assert report.rows[0].correction_count == 2


def test_non_loop_sessions_are_not_reported():
    rows = [
        {
            "session_id": "sess-single",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Bash",
            "status": "failed",
            "content": "Command failed with exit code 1",
        },
        {
            "session_id": "sess-single",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "role": "user",
            "content": "Try again with the right command.",
        },
    ]

    report = build_claude_session_prompt_correction_loops_report(rows, now=NOW)

    assert report.rows == ()
    assert report.totals["correction_loop_count"] == 0


def test_snippet_truncation_is_deterministic_and_configurable():
    rows = [
        {
            "session_id": "sess-truncate",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Read",
            "status": "failed",
            "content": "No such file: " + "very-long-path/" * 8,
        },
        {
            "session_id": "sess-truncate",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "role": "user",
            "content": "Actually use the existing source file under src/ingestion instead.",
        },
        {
            "session_id": "sess-truncate",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "role": "assistant",
            "content": "Actually use the existing source file under src/ingestion instead.",
        },
    ]

    report = build_claude_session_prompt_correction_loops_report(
        rows,
        max_snippet_chars=32,
        now=NOW,
    )

    assert report.rows[0].trigger_type == "missing_context"
    assert report.rows[0].trigger_snippets[0].endswith("...")
    assert len(report.rows[0].trigger_snippets[0]) <= 32
    assert report.rows[0].correction_snippets[0].endswith("...")
    assert len(report.rows[0].correction_snippets[0]) <= 32


def test_cli_reads_jsonl_and_emits_json(capsys, tmp_path):
    input_path = tmp_path / "session.jsonl"
    rows = [
        {
            "session_id": "sess-cli",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Bash",
            "status": "failed",
            "content": "SyntaxError: invalid syntax",
        },
        {
            "session_id": "sess-cli",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "role": "user",
            "content": "Try again after fixing the parse error.",
        },
        {
            "session_id": "sess-cli",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "role": "assistant",
            "content": "I will try again with valid syntax.",
        },
    ]
    input_path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows), encoding="utf-8")

    assert (
        claude_session_prompt_correction_loops_script.main(
            [str(input_path), "--max-snippet-chars", "80"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["rows"][0]["session_id"] == "sess-cli"
    assert payload["rows"][0]["trigger_type"] == "parse_error"
    assert claude_session_prompt_correction_loops_script.main([str(input_path), "--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
