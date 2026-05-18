"""Tests for reply knowledge link orphan reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from engagement.reply_knowledge_link_orphans import (
    build_reply_knowledge_link_orphans_report,
    build_reply_knowledge_link_orphans_report_from_db,
    format_reply_knowledge_link_orphans_json,
    format_reply_knowledge_link_orphans_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_knowledge_link_orphans.py"
spec = importlib.util.spec_from_file_location("reply_knowledge_link_orphans_script", SCRIPT_PATH)
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
        CREATE TABLE reply_queue (id INTEGER PRIMARY KEY, status TEXT);
        CREATE TABLE knowledge (id INTEGER PRIMARY KEY);
        CREATE TABLE reply_knowledge_links (
            reply_queue_id INTEGER,
            knowledge_id INTEGER,
            relevance_score REAL
        );
        """
    )
    return conn


def test_builder_flags_missing_reply_missing_knowledge_invalid_score_and_dismissed_reply():
    report = build_reply_knowledge_link_orphans_report(
        [
            {
                "link_id": 1,
                "reply_queue_id": 100,
                "resolved_reply_queue_id": None,
                "knowledge_id": 200,
                "resolved_knowledge_id": 200,
                "relevance_score": None,
            },
            {
                "link_id": 2,
                "reply_queue_id": 101,
                "resolved_reply_queue_id": 101,
                "reply_status": "dismissed",
                "knowledge_id": 201,
                "resolved_knowledge_id": None,
                "relevance_score": 1.4,
            },
        ],
        now=NOW,
    )

    counts = report["summary"]["by_issue_type"]
    assert report["artifact_type"] == "reply_knowledge_link_orphans"
    assert counts["missing_reply"] == 1
    assert counts["missing_knowledge"] == 1
    assert counts["invalid_relevance_score"] == 2
    assert counts["dismissed_reply_attached"] == 1


def test_status_filter_and_limit_are_applied():
    report = build_reply_knowledge_link_orphans_report(
        [
            {"link_id": 1, "reply_status": "pending", "resolved_reply_queue_id": 1, "resolved_knowledge_id": None},
            {"link_id": 2, "reply_status": "dismissed", "resolved_reply_queue_id": 2, "resolved_knowledge_id": None},
        ],
        status="dismissed",
        limit=1,
        now=NOW,
    )

    assert report["summary"]["link_count"] == 1
    assert report["summary"]["finding_count"] == 1
    assert report["summary"]["shown_count"] == 1
    assert report["findings"][0]["link_id"] == 2


def test_db_adapter_reads_joined_links_and_handles_missing_tables():
    conn = _conn()
    conn.execute("INSERT INTO reply_queue VALUES (1, 'dismissed')")
    conn.execute("INSERT INTO knowledge VALUES (2)")
    conn.execute("INSERT INTO reply_knowledge_links VALUES (1, 2, 0.9)")
    conn.execute("INSERT INTO reply_knowledge_links VALUES (999, 888, -0.1)")

    report = build_reply_knowledge_link_orphans_report_from_db(conn, now=NOW)

    assert report["summary"]["link_count"] == 2
    assert report["summary"]["finding_count"] == 2
    assert report["summary"]["by_issue_type"]["missing_reply"] == 1
    assert report["summary"]["by_issue_type"]["missing_knowledge"] == 1
    assert report["summary"]["by_issue_type"]["dismissed_reply_attached"] == 1

    empty = build_reply_knowledge_link_orphans_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert empty["missing_tables"] == ["knowledge", "reply_knowledge_links", "reply_queue"]
    assert empty["findings"] == []


def test_json_and_text_formatters_are_stable():
    report = build_reply_knowledge_link_orphans_report(
        [{"link_id": 1, "reply_queue_id": 1, "knowledge_id": 2, "relevance_score": "bad"}],
        now=NOW,
    )

    payload = json.loads(format_reply_knowledge_link_orphans_json(report))
    assert payload["artifact_type"] == "reply_knowledge_link_orphans"
    assert list(payload) == sorted(payload)
    text = format_reply_knowledge_link_orphans_text(report)
    assert "Reply Knowledge Link Orphans" in text
    assert "link_id | reply_queue_id | knowledge_id" in text


def test_cli_supports_db_status_json_text_and_invalid_limit(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "reply-links.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE reply_queue (id INTEGER PRIMARY KEY, status TEXT);
        CREATE TABLE knowledge (id INTEGER PRIMARY KEY);
        CREATE TABLE reply_knowledge_links (reply_queue_id INTEGER, knowledge_id INTEGER, relevance_score REAL);
        INSERT INTO reply_queue VALUES (1, 'dismissed');
        INSERT INTO knowledge VALUES (2);
        INSERT INTO reply_knowledge_links VALUES (1, 2, NULL);
        """
    )
    conn.close()

    assert script.main(["--db", str(db_path), "--status", "dismissed", "--limit", "5", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "reply_knowledge_link_orphans"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "dismissed_reply_attached" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["findings"] == []
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
