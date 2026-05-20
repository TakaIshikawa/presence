"""Tests for proactive action relationship context gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from engagement.proactive_action_relationship_context_gaps import (
    build_proactive_action_relationship_context_gaps_report,
    format_proactive_action_relationship_context_gaps_json,
    format_proactive_action_relationship_context_gaps_text,
)


NOW = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "proactive_action_relationship_context_gaps.py"
spec = importlib.util.spec_from_file_location("proactive_action_relationship_context_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE proactive_actions (
            id INTEGER PRIMARY KEY,
            status TEXT,
            action_type TEXT,
            target_author_handle TEXT,
            created_at TEXT,
            relationship_context TEXT
        );
        """
    )
    return conn


def test_report_flags_missing_malformed_empty_and_low_signal_context():
    conn = _conn()
    conn.executescript(
        """
        INSERT INTO proactive_actions VALUES (1, 'pending', 'reply', '@a', '2026-05-01T08:00:00+00:00', NULL);
        INSERT INTO proactive_actions VALUES (2, 'queued', 'reply', '@b', '2026-05-01T08:01:00+00:00', '{bad');
        INSERT INTO proactive_actions VALUES (3, 'draft', 'quote', '@c', '2026-05-01T08:02:00+00:00', '{}');
        INSERT INTO proactive_actions VALUES (4, 'needs_approval', 'reply', '@d', '2026-05-01T08:03:00+00:00', '{"summary":"met once"}');
        INSERT INTO proactive_actions VALUES (5, 'pending', 'reply', '@e', '2026-05-01T08:04:00+00:00', '{"stage":"warm"}');
        INSERT INTO proactive_actions VALUES (6, 'approved', 'reply', '@f', '2026-05-01T08:05:00+00:00', NULL);
        """
    )

    report = build_proactive_action_relationship_context_gaps_report(conn, now=NOW)

    assert report["artifact_type"] == "proactive_action_relationship_context_gaps"
    assert report["summary"]["actions_scanned"] == 5
    assert [item["gap_type"] for item in report["items"]] == [
        "missing_context",
        "malformed_context",
        "empty_context",
        "low_signal_context",
    ]
    assert {item["action_id"] for item in report["items"]} == {1, 2, 3, 4}


def test_schema_gaps_limit_formatters_and_cli(tmp_path, monkeypatch, capsys):
    conn = _conn()
    conn.execute("INSERT INTO proactive_actions VALUES (1, 'pending', 'reply', '@a', ?, '')", (NOW.isoformat(),))
    report = build_proactive_action_relationship_context_gaps_report(conn, limit=1, now=NOW)

    assert len(report["items"]) == 1
    assert json.loads(format_proactive_action_relationship_context_gaps_json(report))["artifact_type"] == "proactive_action_relationship_context_gaps"
    assert "action_id | status | action_type" in format_proactive_action_relationship_context_gaps_text(report)

    missing = build_proactive_action_relationship_context_gaps_report(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["proactive_actions"]
    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE proactive_actions (id INTEGER, status TEXT)")
    schema_report = build_proactive_action_relationship_context_gaps_report(bad, now=NOW)
    assert schema_report["missing_columns"] == {"proactive_actions": ["relationship_context"]}

    db_path = tmp_path / "actions.sqlite"
    disk = sqlite3.connect(db_path)
    disk.executescript(
        """
        CREATE TABLE proactive_actions (id INTEGER PRIMARY KEY, status TEXT, relationship_context TEXT);
        INSERT INTO proactive_actions VALUES (1, 'pending', '');
        """
    )
    disk.close()
    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["items"][0]["gap_type"] == "missing_context"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Proactive Action Relationship Context Gaps" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_tables"] == ["proactive_actions"]
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
