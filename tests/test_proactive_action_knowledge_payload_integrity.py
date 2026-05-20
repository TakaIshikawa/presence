"""Tests for proactive action knowledge payload integrity reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from engagement.proactive_action_knowledge_payload_integrity import (
    build_proactive_action_knowledge_payload_integrity_report,
    build_proactive_action_knowledge_payload_integrity_report_from_db,
    format_proactive_action_knowledge_payload_integrity_json,
    format_proactive_action_knowledge_payload_integrity_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "proactive_action_knowledge_payload_integrity.py"
spec = importlib.util.spec_from_file_location("proactive_action_knowledge_payload_integrity_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE proactive_actions (
               id INTEGER PRIMARY KEY,
               status TEXT,
               action_type TEXT,
               knowledge_ids TEXT
           );
           CREATE TABLE knowledge (
               id INTEGER PRIMARY KEY,
               license TEXT
           );"""
    )
    return conn


def test_builder_flags_payload_shape_missing_refs_relevance_and_restricted_use():
    rows = [
        {"id": 1, "status": "pending", "action_type": "reply", "knowledge_ids": "["},
        {"id": 2, "status": "pending", "action_type": "reply", "knowledge_ids": json.dumps([[None, 0.4], [10], {"id": 11}, {"id": 12, "relevance": 1.2}, {"id": 99, "relevance": 0.8}])},
        {"id": 3, "status": "approved", "action_type": "reply", "knowledge_ids": json.dumps([{"id": 13, "score": 0.7}])},
        {"id": 4, "status": "posted", "action_type": "quote_tweet", "knowledge_ids": json.dumps([])},
    ]
    report = build_proactive_action_knowledge_payload_integrity_report(
        rows,
        knowledge_rows=[{"id": 10, "license": "open"}, {"id": 11, "license": "open"}, {"id": 12, "license": "open"}, {"id": 13, "license": "restricted"}],
        now=NOW,
    )
    payload = json.loads(format_proactive_action_knowledge_payload_integrity_json(report))

    assert payload["artifact_type"] == "proactive_action_knowledge_payload_integrity"
    assert [item["reason"] for item in payload["findings"]] == [
        "malformed_knowledge_ids_json",
        "missing_knowledge_id",
        "missing_relevance_score",
        "missing_relevance_score",
        "invalid_relevance_score",
        "missing_knowledge_reference",
        "restricted_knowledge_resolved_action",
        "posted_action_empty_knowledge_support",
    ]
    assert payload["summary"]["by_reason"] == {
        "invalid_relevance_score": 1,
        "malformed_knowledge_ids_json": 1,
        "missing_knowledge_id": 1,
        "missing_knowledge_reference": 1,
        "missing_relevance_score": 2,
        "posted_action_empty_knowledge_support": 1,
        "restricted_knowledge_resolved_action": 1,
    }
    assert {"status", "action_type", "reason", "count"}.issubset(payload["groups"][0])


def test_db_loader_cli_and_schema_diagnostics(tmp_path, capsys):
    conn = _conn()
    conn.executemany("INSERT INTO knowledge VALUES (?, ?)", [(1, "open"), (2, "restricted")])
    conn.executemany(
        "INSERT INTO proactive_actions VALUES (?, ?, ?, ?)",
        [
            (1, "posted", "reply", json.dumps([1])),
            (2, "posted", "reply", json.dumps([])),
            (3, "approved", "quote_tweet", json.dumps([[2, 0.9], [99, 0.8]])),
        ],
    )
    report = build_proactive_action_knowledge_payload_integrity_report_from_db(conn, now=NOW)
    assert report["summary"]["finding_count"] == 3
    assert report["summary"]["by_reason"]["posted_action_empty_knowledge_support"] == 1
    assert report["summary"]["by_reason"]["restricted_knowledge_resolved_action"] == 1
    assert "Proactive Action Knowledge Payload Integrity" in format_proactive_action_knowledge_payload_integrity_text(report)

    db_path = tmp_path / "proactive.sqlite"
    conn.commit()
    conn.backup(sqlite3.connect(db_path))
    assert script.main(["--db", str(db_path), "--format", "json", "--now", NOW.isoformat(), "--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["limit"] == 5
    assert payload["summary"]["finding_count"] == 3
    assert script.main(["--db", str(db_path), "--format", "text", "--now", NOW.isoformat()]) == 0
    assert "restricted_knowledge_resolved_action" in capsys.readouterr().out

    missing = build_proactive_action_knowledge_payload_integrity_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["knowledge", "proactive_actions"]
    partial = sqlite3.connect(":memory:")
    partial.executescript(
        """CREATE TABLE proactive_actions (id INTEGER, knowledge_ids TEXT);
           CREATE TABLE knowledge (license TEXT);"""
    )
    gaps = build_proactive_action_knowledge_payload_integrity_report_from_db(partial, now=NOW)
    assert gaps["missing_columns"] == {
        "knowledge": ["id"],
        "proactive_actions": ["action_type", "status"],
    }
    assert "No proactive action knowledge payload integrity gaps" in format_proactive_action_knowledge_payload_integrity_text(gaps)


def test_validation_errors():
    with pytest.raises(ValueError, match="limit must be positive"):
        build_proactive_action_knowledge_payload_integrity_report([], limit=0)
    assert script.main(["--limit", "0"]) == 2
