from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from output.knowledge_source_review_queue_export import build_knowledge_source_review_queue_export_from_db, format_knowledge_source_review_queue_export_csv, format_knowledge_source_review_queue_export_json

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "export_knowledge_source_review_queue.py"
spec = importlib.util.spec_from_file_location("export_knowledge_source_review_queue_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_review_queue_export_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE knowledge_sources (id INTEGER PRIMARY KEY, url TEXT, title TEXT, license TEXT, confidence REAL, last_seen_at TEXT, reuse_count INTEGER, extraction_error TEXT)")
    conn.execute("INSERT INTO knowledge_sources VALUES (1, 'https://e.test', 'E', NULL, 0.2, '2025-01-01T00:00:00+00:00', 11, 'parse')")
    export = build_knowledge_source_review_queue_export_from_db(conn, now=NOW)
    assert export["rows"][0]["review_priority"] >= 10
    assert "missing_license" in export["rows"][0]["reasons"]
    assert json.loads(format_knowledge_source_review_queue_export_json(export))["artifact_type"] == "knowledge_source_review_queue_export"
    assert "source_id,url" in format_knowledge_source_review_queue_export_csv(export)
    conn.commit()
    dest = sqlite3.connect(tmp_path / "knowledge.sqlite")
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(tmp_path / "knowledge.sqlite"), "--reason", "missing_license"]) == 0
    assert json.loads(capsys.readouterr().out)["rows"]


def test_missing_empty_and_validation():
    assert build_knowledge_source_review_queue_export_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"]
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
