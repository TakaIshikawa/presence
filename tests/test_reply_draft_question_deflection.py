"""Tests for reply draft question deflection reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from engagement.reply_draft_question_deflection import (
    build_reply_draft_question_deflection_report,
    build_reply_draft_question_deflection_report_from_db,
    format_reply_draft_question_deflection_json,
    format_reply_draft_question_deflection_text,
)


NOW = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_draft_question_deflection.py"
spec = importlib.util.spec_from_file_location("reply_draft_question_deflection_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_question_targets_with_generic_drafts_are_flagged_but_direct_answers_are_not():
    rows = [
        {"id": 1, "platform": "x", "status": "queued", "target_text": "How do I rotate keys?", "draft_text": "Great question, thanks for asking.", "created_at": NOW.isoformat()},
        {"id": 2, "platform": "x", "status": "queued", "target_text": "How do I rotate keys?", "draft_text": "You can rotate keys from Settings, then create a replacement token.", "created_at": NOW.isoformat()},
        {"id": 3, "platform": "x", "status": "queued", "target_text": "Nice launch", "draft_text": "Thanks!", "created_at": NOW.isoformat()},
    ]
    report = build_reply_draft_question_deflection_report(rows, now=NOW)
    assert report["artifact_type"] == "reply_draft_question_deflection"
    assert [f["reply_draft_id"] for f in report["findings"]] == [1]
    assert report["findings"][0]["deflection_reason"] == "generic_acknowledgement_without_answer"


def test_db_adapter_column_variants_formatters_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE reply_queue (id INTEGER PRIMARY KEY, status TEXT, platform TEXT, raw_target_metadata TEXT, content TEXT, created_at TEXT)")
    conn.execute("INSERT INTO reply_queue VALUES (1, 'pending', 'x', ?, 'Interesting, I will look into it.', ?)", (json.dumps({"target_text": "Why did this fail?"}), NOW.isoformat()))
    conn.execute("INSERT INTO reply_queue VALUES (2, 'pending', 'x', ?, 'Because the token expired; create a new one.', ?)", (json.dumps({"text": "Why did this fail?"}), NOW.isoformat()))
    report = build_reply_draft_question_deflection_report_from_db(conn, now=NOW)
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["target_text"] == "Why did this fail?"
    assert json.loads(format_reply_draft_question_deflection_json(report))["artifact_type"] == "reply_draft_question_deflection"
    assert "Reply Draft Question Deflection" in format_reply_draft_question_deflection_text(report)

    db_path = tmp_path / "reply.sqlite"
    disk = sqlite3.connect(db_path)
    conn.commit()
    conn.backup(disk)
    disk.close()
    assert script.main(["--db", str(db_path), "--min-question-score", "1", "--window-days", "60"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Reply Draft Question Deflection" in capsys.readouterr().out


def test_missing_schema_and_cli_validation():
    assert build_reply_draft_question_deflection_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"] == ["reply_drafts|reply_queue"]
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE reply_drafts (id INTEGER PRIMARY KEY, target_text TEXT)")
    assert build_reply_draft_question_deflection_report_from_db(conn, now=NOW)["missing_columns"] == {"reply_drafts": ["draft_text|content|body|reply_text"]}
    with pytest.raises(SystemExit):
        script.parse_args(["--window-days", "0"])
