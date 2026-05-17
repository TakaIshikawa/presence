"""Tests for reply draft knowledge freshness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from engagement.reply_draft_knowledge_freshness import (
    build_reply_draft_knowledge_freshness_report,
    format_reply_draft_knowledge_freshness_json,
    format_reply_draft_knowledge_freshness_text,
)


NOW = datetime(2026, 5, 18, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_draft_knowledge_freshness.py"
spec = importlib.util.spec_from_file_location("reply_draft_knowledge_freshness_script", SCRIPT_PATH)
reply_draft_knowledge_freshness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_draft_knowledge_freshness_script)


@contextmanager
def _script_context(conn):
    yield SimpleNamespace(), conn


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE reply_queue (id INTEGER PRIMARY KEY, inbound_author TEXT, detected_at TEXT);
        CREATE TABLE reply_knowledge_links (reply_queue_id INTEGER, knowledge_id INTEGER);
        CREATE TABLE knowledge (id INTEGER PRIMARY KEY, source_type TEXT, title TEXT, published_at TEXT, ingested_at TEXT);"""
    )
    return conn


def test_stale_knowledge_uses_published_then_ingested_and_breakdowns():
    conn = _conn()
    conn.execute("INSERT INTO reply_queue VALUES (1, 'alice', ?)", ((NOW - timedelta(days=1)).isoformat(),))
    conn.execute("INSERT INTO reply_queue VALUES (2, 'bob', ?)", ((NOW - timedelta(days=1)).isoformat(),))
    conn.execute("INSERT INTO knowledge VALUES (1, 'blog', 'Old', ?, ?)", ((NOW - timedelta(days=400)).isoformat(), (NOW - timedelta(days=5)).isoformat()))
    conn.execute("INSERT INTO knowledge VALUES (2, 'doc', 'Fresh', NULL, ?)", ((NOW - timedelta(days=10)).isoformat(),))
    conn.execute("INSERT INTO reply_knowledge_links VALUES (1, 1)")
    conn.execute("INSERT INTO reply_knowledge_links VALUES (2, 2)")
    conn.commit()

    report = build_reply_draft_knowledge_freshness_report(conn, now=NOW, stale_days=180)

    assert report["totals"]["linked_source_count"] == 2
    assert report["stale_draft_examples"][0]["knowledge_id"] == 1
    assert report["author_breakdowns"] == {"alice": 1}
    assert report["source_type_breakdowns"] == {"blog": 1}


def test_json_text_cli_and_schema_gaps(monkeypatch, capsys):
    conn = _conn()
    report = build_reply_draft_knowledge_freshness_report(conn, now=NOW)
    assert json.loads(format_reply_draft_knowledge_freshness_json(report))["artifact_type"] == "reply_draft_knowledge_freshness"
    assert "Reply Draft Knowledge Freshness" in format_reply_draft_knowledge_freshness_text(report)
    monkeypatch.setattr(reply_draft_knowledge_freshness_script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        reply_draft_knowledge_freshness_script,
        "build_reply_draft_knowledge_freshness_report",
        lambda db, **kwargs: build_reply_draft_knowledge_freshness_report(db, now=NOW, **kwargs),
    )
    assert reply_draft_knowledge_freshness_script.main(["--format", "text"]) == 0
    assert "Totals: linked=0" in capsys.readouterr().out

    missing = build_reply_draft_knowledge_freshness_report(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["reply_queue", "reply_knowledge_links", "knowledge"]
