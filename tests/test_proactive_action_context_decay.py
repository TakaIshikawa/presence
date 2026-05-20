"""Tests for proactive action context decay reporting."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.proactive_action_context_decay import build_proactive_action_context_decay_report_from_db


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "proactive_action_context_decay.py"
spec = importlib.util.spec_from_file_location("proactive_action_context_decay_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE proactive_actions (
               id INTEGER PRIMARY KEY,
               status TEXT,
               action_type TEXT,
               target_author_handle TEXT,
               relationship_context TEXT,
               platform_metadata TEXT,
               created_at TEXT,
               reviewed_at TEXT
           )"""
    )
    return conn


def _insert(
    conn: sqlite3.Connection,
    action_id: int,
    *,
    status: str = "pending",
    action_type: str = "reply",
    relationship_context: str | None = None,
    platform_metadata: str | None = None,
    created_at: str = "2026-05-20T00:00:00+00:00",
    reviewed_at: str | None = None,
) -> None:
    conn.execute(
        "INSERT INTO proactive_actions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (action_id, status, action_type, f"author{action_id}", relationship_context, platform_metadata, created_at, reviewed_at),
    )


def test_report_flags_missing_stale_and_invalid_json_context():
    conn = _conn()
    _insert(conn, 1, relationship_context=None, platform_metadata='{"updated_at":"2026-05-19T00:00:00+00:00"}')
    _insert(
        conn,
        2,
        relationship_context='{"fetched_at":"2026-04-01T00:00:00+00:00"}',
        platform_metadata='{"observed_at":"2026-05-18T00:00:00+00:00"}',
        created_at="2026-05-20T00:00:00+00:00",
    )
    _insert(conn, 3, relationship_context="{bad json", platform_metadata='{"observed_at":"2026-05-18T00:00:00+00:00"}')
    _insert(
        conn,
        4,
        relationship_context='{"fetched_at":"2026-05-19T00:00:00+00:00"}',
        platform_metadata='{"observed_at":"2026-05-18T00:00:00+00:00"}',
    )

    report = build_proactive_action_context_decay_report_from_db(conn, max_context_age_days=14)

    by_id = {item["proactive_action_id"]: item for item in report["decayed_actions"]}
    assert by_id[1]["missing_context"] is True
    assert by_id[1]["severity"] == "critical"
    assert by_id[2]["context_age_days"] == 49
    assert by_id[2]["metadata_age_days"] == 2
    assert by_id[2]["severity"] == "medium"
    assert by_id[3]["issue_types"] == ["invalid_json"]
    assert 4 not in by_id


def test_cli_filters_and_validation(tmp_path, capsys):
    conn = _conn()
    _insert(conn, 1, action_type="reply", relationship_context=None)
    _insert(conn, 2, action_type="follow", relationship_context=None)
    conn.commit()
    db_path = tmp_path / "proactive.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path), "--action-type", "reply"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["proactive_action_id"] for item in payload["decayed_actions"]] == [1]

    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Proactive Action Context Decay" in capsys.readouterr().out

    assert script.main(["--db", str(db_path), "--max-context-age-days", "-1"]) == 2
    assert "value must be non-negative" in capsys.readouterr().err


def test_status_filter_missing_table_and_reviewed_at_reference():
    conn = _conn()
    _insert(
        conn,
        1,
        status="approved",
        relationship_context='{"updated_at":"2026-05-01T00:00:00+00:00"}',
        reviewed_at="2026-05-20T00:00:00+00:00",
    )

    report = build_proactive_action_context_decay_report_from_db(conn, status="approved", max_context_age_days=14)
    assert report["decayed_actions"][0]["context_age_days"] == 19

    missing = build_proactive_action_context_decay_report_from_db(sqlite3.connect(":memory:"))
    assert missing["missing_tables"] == ["proactive_actions"]
