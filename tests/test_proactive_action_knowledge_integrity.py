"""Tests for proactive action knowledge integrity reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from engagement.proactive_action_knowledge_integrity import (
    build_proactive_action_knowledge_integrity_report,
    build_proactive_action_knowledge_integrity_report_from_db,
    format_proactive_action_knowledge_integrity_json,
    format_proactive_action_knowledge_integrity_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "proactive_action_knowledge_integrity.py"
spec = importlib.util.spec_from_file_location("proactive_action_knowledge_integrity_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE proactive_actions (
               id INTEGER PRIMARY KEY,
               status TEXT,
               action_type TEXT,
               knowledge_ids TEXT
           );
           CREATE TABLE knowledge (
               id INTEGER PRIMARY KEY
           );"""
    )
    return conn


def test_builder_detects_malformed_wrong_shape_missing_duplicate_and_low_relevance():
    report = build_proactive_action_knowledge_integrity_report(
        [
            {"id": 1, "status": "active", "action_type": "reply", "knowledge_ids": "["},
            {"id": 2, "status": "active", "action_type": "reply", "knowledge_ids": "{}"},
            {"id": 3, "status": "active", "action_type": "reply", "knowledge_ids": json.dumps([{"bad": 1}, {"id": 99, "relevance": 0.2}, {"id": 99, "relevance": 0.9}])},
        ],
        known_knowledge_ids={"1"},
        min_relevance=0.5,
        now=NOW,
    )
    assert report["artifact_type"] == "proactive_action_knowledge_integrity"
    assert report["summary"]["by_issue_type"] == {
        "duplicate_knowledge_id": 1,
        "invalid_reference_shape": 1,
        "low_relevance": 1,
        "malformed_json": 1,
        "missing_knowledge": 2,
        "non_list_payload": 1,
    }


def test_db_loader_parses_known_knowledge_and_filters():
    conn = _conn()
    conn.execute("INSERT INTO knowledge VALUES (1)")
    conn.executemany(
        "INSERT INTO proactive_actions VALUES (?, ?, ?, ?)",
        [
            (1, "active", "reply", json.dumps([{"id": 1, "relevance": 0.9}])),
            (2, "active", "reply", json.dumps([[2, 0.2], [2, 0.8]])),
            (3, "done", "seed", json.dumps(["missing"])),
        ],
    )
    report = build_proactive_action_knowledge_integrity_report_from_db(conn, status="active", action_type="reply", min_relevance=0.5, now=NOW)
    assert report["summary"]["by_issue_type"] == {"duplicate_knowledge_id": 1, "low_relevance": 1, "missing_knowledge": 2}


def test_cli_json_text_and_validation(tmp_path, capsys):
    conn = _conn()
    conn.execute("INSERT INTO proactive_actions VALUES (1, 'active', 'reply', '[')")
    conn.commit()
    db_path = tmp_path / "knowledge.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)
    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Proactive Action Knowledge Integrity" in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_formatters_and_invalid_thresholds():
    report = build_proactive_action_knowledge_integrity_report([], now=NOW)
    assert json.loads(format_proactive_action_knowledge_integrity_json(report))["artifact_type"] == "proactive_action_knowledge_integrity"
    assert "No proactive action knowledge integrity gaps found" in format_proactive_action_knowledge_integrity_text(report)
    with pytest.raises(ValueError, match="min_relevance must be non-negative"):
        build_proactive_action_knowledge_integrity_report([], min_relevance=-0.1)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_proactive_action_knowledge_integrity_report([], limit=0)
