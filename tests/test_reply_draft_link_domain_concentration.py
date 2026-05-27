"""Tests for reply draft link domain concentration reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from engagement.reply_draft_link_domain_concentration import (
    build_reply_draft_link_domain_concentration_report,
    build_reply_draft_link_domain_concentration_report_from_db,
    format_reply_draft_link_domain_concentration_json,
    format_reply_draft_link_domain_concentration_text,
)


NOW = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_draft_link_domain_concentration.py"
spec = importlib.util.spec_from_file_location("reply_draft_link_domain_concentration_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_threshold_filtering_and_summary_by_domain():
    rows = [
        {"id": 1, "platform": "x", "status": "queued", "text": "https://a.test/1 https://a.test/2 https://b.test/1", "created_at": NOW.isoformat()},
        {"id": 2, "platform": "x", "status": "queued", "text": "https://c.test/1 https://d.test/1", "created_at": NOW.isoformat()},
        {"id": 3, "platform": "x", "status": "sent", "text": "https://a.test/3 https://a.test/4", "created_at": NOW.isoformat()},
    ]
    report = build_reply_draft_link_domain_concentration_report(rows, status="queued", max_domain_share=0.6, now=NOW)
    assert report["artifact_type"] == "reply_draft_link_domain_concentration"
    assert [item["reply_draft_id"] for item in report["findings"]] == [1]
    assert report["findings"][0]["domain"] == "a.test"
    assert report["summary"]["by_domain"][0]["affected_draft_ids"] == [1]


def test_domain_extraction_from_body_metadata_permalink_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_drafts (
            id INTEGER PRIMARY KEY, platform TEXT, status TEXT, body TEXT, metadata TEXT, permalink TEXT, created_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO reply_drafts VALUES (1, 'x', 'pending', ?, ?, ?, ?)",
        (
            "See https://docs.example/a and https://docs.example/b",
            json.dumps({"urls": ["https://docs.example/c", "https://other.example/z"]}),
            "https://docs.example/permalink",
            NOW.isoformat(),
        ),
    )
    conn.execute("INSERT INTO reply_drafts VALUES (2, 'x', 'queued', 'https://one.example/a https://two.example/b', NULL, NULL, ?)", (NOW.isoformat(),))
    report = build_reply_draft_link_domain_concentration_report_from_db(conn, max_domain_share=0.6, now=NOW)
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["link_count"] == 4
    assert json.loads(format_reply_draft_link_domain_concentration_json(report))["artifact_type"] == "reply_draft_link_domain_concentration"
    assert "Summary by domain" in format_reply_draft_link_domain_concentration_text(report)

    db_path = tmp_path / "reply.sqlite"
    disk = sqlite3.connect(db_path)
    conn.commit()
    conn.backup(disk)
    disk.close()
    assert script.main(["--db", str(db_path), "--max-domain-share", "0.6", "--window-days", "60"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Reply Draft Link Domain Concentration" in capsys.readouterr().out


def test_missing_schema_metadata_and_argument_validation():
    missing = build_reply_draft_link_domain_concentration_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["reply_drafts|reply_queue"]
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE reply_queue (status TEXT)")
    assert build_reply_draft_link_domain_concentration_report_from_db(conn, now=NOW)["missing_columns"] == {"reply_queue": ["id"]}
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
    with pytest.raises(SystemExit):
        script.parse_args(["--max-domain-share", "1.2"])
