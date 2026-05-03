"""Tests for Claude session tool result schema drift reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

from ingestion.claude_session_tool_result_drift import (
    build_claude_session_tool_result_drift_report,
    format_claude_session_tool_result_drift_json,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "claude_session_tool_result_drift.py"
)
spec = importlib.util.spec_from_file_location("claude_session_tool_result_drift_script", SCRIPT_PATH)
claude_session_tool_result_drift_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_tool_result_drift_script)


def test_row_list_reports_drifted_result_key_sets_only():
    rows = [
        {
            "session_id": "sess-drift",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Bash",
            "result": {"stdout": "ok", "exit_code": 0},
        },
        {
            "session_id": "sess-drift",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "tool_name": "Bash",
            "result": {"stdout": "ok", "stderr": "warn", "exit_code": 0},
        },
        {
            "session_id": "sess-stable",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "tool_name": "Read",
            "result": {"content": "a"},
        },
        {
            "session_id": "sess-stable",
            "timestamp": "2026-05-01T10:03:00+00:00",
            "tool_name": "Read",
            "result": {"content": "b"},
        },
    ]

    report = build_claude_session_tool_result_drift_report(rows, days=7, now=NOW)
    payload = json.loads(format_claude_session_tool_result_drift_json(report))

    assert payload["artifact_type"] == "claude_session_tool_result_drift"
    assert list(payload) == sorted(payload)
    assert len(report.rows) == 1
    assert report.rows[0].session_id == "sess-drift"
    assert report.rows[0].tool_name == "bash"
    assert report.rows[0].distinct_result_key_sets == (
        ("exit_code", "stderr", "stdout"),
        ("exit_code", "stdout"),
    )
    assert report.rows[0].representative_result_keys == ("exit_code", "stderr", "stdout")
    assert report.totals["tool_result_count"] == 4
    assert report.totals["drift_count"] == 1


def test_malformed_metadata_tool_filter_and_no_drift_suppression():
    rows = [
        {
            "session_id": "sess-bad",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Bash",
            "metadata": "{bad json",
        },
        {
            "session_id": "sess-filtered",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "tool_name": "Bash",
            "output": '{"stdout": "ok"}',
        },
        {
            "session_id": "sess-filtered",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "tool_name": "Bash",
            "output": '{"stdout": "ok", "exit_code": 0}',
        },
        {
            "session_id": "sess-other",
            "timestamp": "2026-05-01T10:03:00+00:00",
            "tool_name": "Read",
            "result": {"content": "same"},
        },
    ]

    report = build_claude_session_tool_result_drift_report(
        rows,
        days=7,
        tool="Bash",
        now=NOW,
    )

    assert report.filters["tool"] == "bash"
    assert report.totals["malformed_metadata_count"] == 1
    assert report.totals["tool_result_count"] == 2
    assert [row.session_id for row in report.rows] == ["sess-filtered"]

    stable = build_claude_session_tool_result_drift_report(
        rows[-1:],
        days=7,
        now=NOW,
    )
    assert stable.rows == ()
    assert stable.totals["drift_count"] == 0


def test_limit_validation_and_cli_parse_errors(capsys):
    assert claude_session_tool_result_drift_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert claude_session_tool_result_drift_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
