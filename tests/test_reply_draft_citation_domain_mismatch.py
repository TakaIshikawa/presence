"""Tests for reply draft citation domain mismatch reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from engagement.reply_draft_citation_domain_mismatch import (
    build_reply_draft_citation_domain_mismatch_report,
    build_reply_draft_citation_domain_mismatch_report_from_db,
    format_reply_draft_citation_domain_mismatch_json,
    format_reply_draft_citation_domain_mismatch_text,
)


NOW = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_draft_citation_domain_mismatch.py"
spec = importlib.util.spec_from_file_location("reply_draft_citation_domain_mismatch_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_claimed_source_aliases_are_normalized_and_mismatches_flagged():
    rows = [
        {"id": 1, "platform": "x", "status": "queued", "text": "GitHub docs say this: https://marketing.example/guide", "created_at": NOW.isoformat()},
        {"id": 2, "platform": "x", "status": "queued", "text": "OpenAI docs cover this: https://platform.openai.com/docs", "created_at": NOW.isoformat()},
    ]
    report = build_reply_draft_citation_domain_mismatch_report(rows, now=NOW)
    assert report["artifact_type"] == "reply_draft_citation_domain_mismatch"
    assert [f["reply_draft_id"] for f in report["findings"]] == [1]
    assert report["findings"][0]["claimed_source"] == "github docs"
    assert report["findings"][0]["linked_domain"] == "marketing.example"


def test_db_adapter_metadata_urls_formatters_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE reply_drafts (id INTEGER PRIMARY KEY, platform TEXT, status TEXT, body TEXT, metadata TEXT, created_at TEXT)")
    conn.execute("INSERT INTO reply_drafts VALUES (1, 'x', 'pending', 'Per GitHub docs', ?, ?)", (json.dumps({"urls": ["https://unrelated.example/a"]}), NOW.isoformat()))
    report = build_reply_draft_citation_domain_mismatch_report_from_db(conn, now=NOW)
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["url"] == "https://unrelated.example/a"
    assert json.loads(format_reply_draft_citation_domain_mismatch_json(report))["artifact_type"] == "reply_draft_citation_domain_mismatch"
    assert "Reply Draft Citation Domain Mismatch" in format_reply_draft_citation_domain_mismatch_text(report)

    db_path = tmp_path / "reply.sqlite"
    disk = sqlite3.connect(db_path)
    conn.commit()
    conn.backup(disk)
    disk.close()
    assert script.main(["--db", str(db_path), "--window-days", "60"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Reply Draft Citation Domain Mismatch" in capsys.readouterr().out


def test_missing_schema_and_cli_validation():
    assert build_reply_draft_citation_domain_mismatch_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"] == ["reply_drafts|reply_queue"]
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE reply_drafts (id INTEGER PRIMARY KEY, status TEXT)")
    assert build_reply_draft_citation_domain_mismatch_report_from_db(conn, now=NOW)["missing_columns"] == {"reply_drafts": ["draft_text|content|body|reply_text|text"]}
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
