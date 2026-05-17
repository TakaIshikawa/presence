"""Tests for Claude session content yield reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from evaluation.claude_session_content_yield import build_claude_session_content_yield_report, build_claude_session_content_yield_report_from_db


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_session_content_yield.py"
spec = importlib.util.spec_from_file_location("claude_session_content_yield_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_matches_content_by_explicit_session_ids_and_mentions():
    report = build_claude_session_content_yield_report(
        [
            {"session_id": "s1", "cwd": "/repo/app", "timestamp": "2026-04-30T00:00:00+00:00"},
            {"session_id": "s2", "cwd": "/repo/other", "timestamp": "2026-03-01T00:00:00+00:00"},
        ],
        [
            {"id": "c1", "source_session_ids": '["s1"]'},
            {"id": "c2", "content": "Built from session s2 notes."},
        ],
        now=NOW,
    )

    by_id = {row["session_id"]: row for row in report["sessions"]}
    assert by_id["s1"]["matched_content_ids"] == ["c1"]
    assert by_id["s2"]["matched_content_ids"] == ["c2"]
    assert report["totals"]["conversion_rate"] == 1.0


def test_reports_unconverted_sessions_and_project_breakdown():
    report = build_claude_session_content_yield_report(
        [{"session_id": "s1", "cwd": "/repo/app", "timestamp": "2026-04-01T00:00:00+00:00"}],
        [],
        now=NOW,
    )

    assert report["sessions"][0]["conversion_status"] == "unconverted"
    assert report["sessions"][0]["age_bucket"] == "over_30_days"
    assert report["totals"]["by_project"][0]["project"] == "app"


def test_db_adapter_tolerates_missing_tables_and_optional_columns():
    conn = sqlite3.connect(":memory:")
    empty = build_claude_session_content_yield_report_from_db(conn, now=NOW)
    assert empty["empty_state"]["is_empty"] is True

    conn.execute("CREATE TABLE claude_session_events (session_id TEXT, timestamp TEXT)")
    conn.execute("CREATE TABLE generated_content (id TEXT, content TEXT)")
    conn.execute("INSERT INTO claude_session_events VALUES ('s1', '2026-04-30T00:00:00+00:00')")
    conn.execute("INSERT INTO generated_content VALUES ('c1', 'from s1')")
    report = build_claude_session_content_yield_report_from_db(conn, now=NOW)
    assert report["sessions"][0]["conversion_status"] == "converted"


def test_cli_json_and_table(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_claude_session_content_yield_report_from_db",
        lambda _db, **kwargs: build_claude_session_content_yield_report([{"session_id": "s1"}], [], now=NOW, **kwargs),
    )

    assert script.main(["--limit", "1", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "claude_session_content_yield"
    assert script.main(["--table"]) == 0
    assert "session_id | project" in capsys.readouterr().out
