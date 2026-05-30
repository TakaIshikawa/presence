from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.reply_draft_question_balance import (
    build_reply_draft_question_balance_report,
    build_reply_draft_question_balance_report_from_db,
    format_reply_draft_question_balance_json,
    format_reply_draft_question_balance_text,
)


NOW = datetime(2026, 5, 24, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_draft_question_balance.py"
spec = importlib.util.spec_from_file_location("reply_draft_question_balance_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_flags_question_only_excessive_and_missing_answer_before_question():
    report = build_reply_draft_question_balance_report(
        [
            {"id": 1, "draft_text": "Could you share the error?", "target_author": "ada", "target_id": "p1"},
            {"id": 2, "draft_text": "Does it fail locally? What version are you on? Can you paste logs?"},
            {"id": 3, "draft_text": "The token expired. Create a new token, then retry. Does that fix it?"},
        ],
        max_questions=1,
        now=NOW,
    )

    assert [row["draft_id"] for row in report["findings"]] == [2, 1]
    assert report["findings"][0]["issue_codes"] == ["question_only", "excessive_questions", "no_answer_before_question"]
    assert report["findings"][1]["issue_codes"] == ["question_only", "no_answer_before_question"]
    assert report["findings"][1]["target_author"] == "ada"
    assert report["findings"][1]["target_id"] == "p1"
    assert report["summary"]["issue_counts"]["question_only"] == 2


def test_limit_db_adapter_formatters_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE reply_drafts (id INTEGER, content TEXT, target_author TEXT, target_post_id TEXT)")
    conn.executemany(
        "INSERT INTO reply_drafts VALUES (?, ?, ?, ?)",
        [
            (1, "Can you clarify?", "ada", "p1"),
            (2, "Here is the fix. Rotate the key, then retry. Is it green now?", "ben", "p2"),
        ],
    )
    report = build_reply_draft_question_balance_report_from_db(conn, now=NOW)
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["question_count"] == 1
    assert json.loads(format_reply_draft_question_balance_json(report))["artifact_type"] == "reply_draft_question_balance"
    assert "Reply Draft Question Balance" in format_reply_draft_question_balance_text(report)

    db_path = tmp_path / "reply.sqlite"
    conn.commit()
    out = sqlite3.connect(db_path)
    conn.backup(out)
    out.close()
    assert script.main(["--db", str(db_path), "--max-questions", "1", "--limit", "1"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Reply Draft Question Balance" in capsys.readouterr().out


def test_missing_schema_and_validation():
    assert build_reply_draft_question_balance_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"] == ["reply_drafts|reply_queue"]
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE reply_drafts (id INTEGER)")
    assert build_reply_draft_question_balance_report_from_db(conn, now=NOW)["missing_columns"] == {"reply_drafts": ["draft_text|content|body|reply_text"]}
    with pytest.raises(ValueError):
        build_reply_draft_question_balance_report([], max_questions=0)
