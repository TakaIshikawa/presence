from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.reply_draft_unanswered_question_gaps import (
    build_reply_draft_unanswered_question_gaps_report,
    build_reply_draft_unanswered_question_gaps_report_from_db,
    format_reply_draft_unanswered_question_gaps_json,
    format_reply_draft_unanswered_question_gaps_text,
)


SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "reply_draft_unanswered_question_gaps.py"
spec = importlib.util.spec_from_file_location("reply_draft_unanswered_question_gaps_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_answered_ignored_multiple_link_and_non_question_cases():
    rows = [
        {"id": "answered", "mention_id": "m1", "inbound_text": "How do I rotate keys?", "reply_text": "Rotate keys from Settings, then create a new token."},
        {"id": "ignored", "mention_id": "m2", "inbound_text": "Why did this fail?", "reply_text": "Thanks for asking."},
        {"id": "multi", "mention_id": "m3", "inbound_text": "What changed? Where is the guide?", "reply_text": "Good question."},
        {"id": "link", "mention_id": "m4", "inbound_text": "Can you share the source link?", "reply_text": "The source is in the dashboard details."},
        {"id": "plain", "mention_id": "m5", "inbound_text": "Nice launch", "reply_text": "Thanks!"},
    ]
    report = build_reply_draft_unanswered_question_gaps_report(rows)
    codes = {(f["reply_id"], f["issue_code"]) for f in report["findings"]}
    assert ("answered", "question_appears_unanswered") not in codes
    assert ("ignored", "question_appears_unanswered") in codes
    assert ("multi", "multiple_questions_thin_reply") in codes
    assert ("link", "link_or_detail_request_missing") in codes
    assert all(f["mention_id"] != "m5" for f in report["findings"])


def test_db_adapter_formatters_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE reply_drafts (id TEXT, mention_id TEXT, inbound_text TEXT, draft_text TEXT)")
    conn.execute("INSERT INTO reply_drafts VALUES ('r1', 'm1', 'What is the URL?', 'I will check.')")
    conn.commit()
    report = build_reply_draft_unanswered_question_gaps_report_from_db(conn)
    assert report["findings"][0]["issue_code"] == "link_or_detail_request_missing"
    assert json.loads(format_reply_draft_unanswered_question_gaps_json(report))["artifact_type"] == "reply_draft_unanswered_question_gaps"
    assert "Reply Draft Unanswered Question Gaps" in format_reply_draft_unanswered_question_gaps_text(report)

    db_path = tmp_path / "reply.sqlite"
    disk = sqlite3.connect(db_path)
    conn.backup(disk)
    disk.close()
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "link_or_detail_request_missing" in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--limit", "0"]) == 2
