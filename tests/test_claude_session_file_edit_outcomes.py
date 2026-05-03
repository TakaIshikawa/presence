"""Tests for Claude session file-edit outcome reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

from ingestion.claude_session_file_edit_outcomes import (
    build_claude_session_file_edit_outcomes_report,
    format_claude_session_file_edit_outcomes_json,
    load_claude_session_log_rows,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "claude_session_file_edit_outcomes.py"
)
spec = importlib.util.spec_from_file_location(
    "claude_session_file_edit_outcomes_script",
    SCRIPT_PATH,
)
claude_session_file_edit_outcomes_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_file_edit_outcomes_script)


def _assistant_tool_use(
    *,
    tool_id: str,
    name: str,
    input_payload: dict,
    timestamp: str = "2026-05-01T10:00:00+00:00",
) -> dict:
    return {
        "type": "assistant",
        "sessionId": "sess-a",
        "cwd": "/repo",
        "timestamp": timestamp,
        "message": {
            "content": [
                {
                    "type": "tool_use",
                    "id": tool_id,
                    "name": name,
                    "input": input_payload,
                }
            ]
        },
    }


def _user_tool_result(
    *,
    tool_id: str,
    content: str,
    is_error: bool = False,
    timestamp: str = "2026-05-01T10:01:00+00:00",
) -> dict:
    return {
        "type": "user",
        "sessionId": "sess-a",
        "cwd": "/repo",
        "timestamp": timestamp,
        "message": {
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": tool_id,
                    "content": content,
                    "is_error": is_error,
                }
            ]
        },
    }


def test_pairs_file_edit_tool_uses_with_results_and_groups_by_extension():
    rows = [
        _assistant_tool_use(
            tool_id="toolu-write",
            name="Write",
            input_payload={"file_path": "src/new_module.py", "content": "print('ok')"},
        ),
        _user_tool_result(tool_id="toolu-write", content="File written successfully"),
        _assistant_tool_use(
            tool_id="toolu-edit",
            name="Edit",
            input_payload={"file_path": "README.md", "old_string": "old", "new_string": "new"},
            timestamp="2026-05-01T10:02:00+00:00",
        ),
        _user_tool_result(
            tool_id="toolu-edit",
            content="old_string not found in file",
            is_error=True,
            timestamp="2026-05-01T10:03:00+00:00",
        ),
        _assistant_tool_use(
            tool_id="toolu-bash",
            name="Bash",
            input_payload={"command": "echo ignored"},
            timestamp="2026-05-01T10:04:00+00:00",
        ),
        _user_tool_result(tool_id="toolu-bash", content="ignored"),
    ]

    report = build_claude_session_file_edit_outcomes_report(rows, now=NOW)
    payload = json.loads(format_claude_session_file_edit_outcomes_json(report))

    assert payload["artifact_type"] == "claude_session_file_edit_outcomes"
    assert list(payload) == sorted(payload)
    assert report.totals["attempt_count"] == 2
    assert report.totals["success_count"] == 1
    assert report.totals["failure_count"] == 1
    assert [
        (row.tool_name, row.path_category, row.attempts, row.successes, row.failures)
        for row in report.rows
    ] == [
        ("Edit", ".md", 1, 0, 1),
        ("Write", ".py", 1, 1, 0),
    ]
    assert report.rows[0].example_targets == ("README.md",)
    assert payload["rows"][0]["target_extension_counts"] == {".md": 1}
    assert payload["rows"][1]["target_extension_counts"] == {".py": 1}


def test_missing_results_no_ops_and_no_extension_targets_are_separate_outcomes():
    rows = [
        _assistant_tool_use(
            tool_id="toolu-missing",
            name="MultiEdit",
            input_payload={"file_path": "Makefile", "edits": []},
        ),
        _assistant_tool_use(
            tool_id="toolu-noop",
            name="NotebookEdit",
            input_payload={"notebook_path": "notebooks/demo.ipynb"},
            timestamp="2026-05-01T10:02:00+00:00",
        ),
        _user_tool_result(
            tool_id="toolu-noop",
            content="No changes made; notebook already matches.",
            timestamp="2026-05-01T10:03:00+00:00",
        ),
    ]

    report = build_claude_session_file_edit_outcomes_report(rows, now=NOW)

    by_tool = {row.tool_name: row for row in report.rows}
    assert by_tool["MultiEdit"].path_category == "[no extension]"
    assert by_tool["MultiEdit"].missing_results == 1
    assert by_tool["MultiEdit"].target_extension_counts == {"[none]": 1}
    assert by_tool["NotebookEdit"].path_category == ".ipynb"
    assert by_tool["NotebookEdit"].no_ops == 1
    assert report.totals["missing_result_count"] == 1
    assert report.totals["no_op_count"] == 1


def test_target_extension_counts_cover_mixed_and_missing_targets():
    rows = [
        {
            "session_id": "sess-meta",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {
                "tool_use": {"name": "Edit", "input": {"file_path": "src/app.PY"}},
                "tool_result": {"content": "updated"},
            },
        },
        {
            "session_id": "sess-meta",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "metadata": {
                "tool_use": {"name": "Edit", "input": {"file_path": "README.md"}},
                "tool_result": {"content": "updated"},
            },
        },
        {
            "session_id": "sess-meta",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "metadata": {
                "tool_use": {"name": "Edit", "input": {"file_path": "Makefile"}},
                "tool_result": {"content": "updated"},
            },
        },
        {
            "session_id": "sess-meta",
            "timestamp": "2026-05-01T10:03:00+00:00",
            "metadata": {
                "tool_use": {"name": "Edit", "input": {"old_string": "a", "new_string": "b"}},
                "tool_result": {"content": "updated"},
            },
        },
    ]

    report = build_claude_session_file_edit_outcomes_report(rows, now=NOW)
    counts = {}
    for row in report.rows:
        counts.update(row.target_extension_counts)

    assert counts == {".md": 1, ".py": 1, "[none]": 1, "[unknown]": 1}
    payload = json.loads(format_claude_session_file_edit_outcomes_json(report))
    serialized_counts = {}
    for row in payload["rows"]:
        serialized_counts.update(row["target_extension_counts"])
        assert list(row["target_extension_counts"]) == sorted(row["target_extension_counts"])
    assert serialized_counts == counts


def test_row_style_metadata_with_embedded_result_is_supported():
    rows = [
        {
            "session_id": "sess-meta",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {
                "tool_use": {
                    "name": "Edit",
                    "input": {"file_path": "scripts/report.py"},
                },
                "tool_result": {"content": "No changes needed; file unchanged."},
            },
        }
    ]

    report = build_claude_session_file_edit_outcomes_report(rows, now=NOW)

    assert report.rows[0].tool_name == "Edit"
    assert report.rows[0].path_category == ".py"
    assert report.rows[0].no_ops == 1
    assert report.events[0].outcome == "no_op"


def test_log_loader_and_cli_print_text_while_writing_json(tmp_path, capsys):
    log_path = tmp_path / "session.jsonl"
    output_path = tmp_path / "report.json"
    rows = [
        _assistant_tool_use(
            tool_id="toolu-write",
            name="Write",
            input_payload={"file_path": "docs/guide.md"},
        ),
        _user_tool_result(tool_id="toolu-write", content="Wrote docs/guide.md"),
    ]
    log_path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    loaded = load_claude_session_log_rows(log_path)
    assert len(loaded) == 2

    exit_code = claude_session_file_edit_outcomes_script.main(
        ["--log", str(log_path), "--output", str(output_path)]
    )
    stdout = capsys.readouterr().out
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert "Claude Session File Edit Outcomes" in stdout
    assert "Write .md" in stdout
    assert payload["artifact_type"] == "claude_session_file_edit_outcomes"
    assert payload["totals"]["success_count"] == 1
