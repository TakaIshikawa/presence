"""Tests for reply draft context staleness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from engagement.reply_draft_context_staleness import (
    build_reply_draft_context_staleness_report,
    format_reply_draft_context_staleness_json,
    format_reply_draft_context_staleness_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_draft_context_staleness.py"
spec = importlib.util.spec_from_file_location("reply_draft_context_staleness_script", SCRIPT_PATH)
reply_draft_context_staleness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_draft_context_staleness_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _reply(db, mention: str, *, context_at: datetime | None, mention_at: datetime | None, status: str = "pending") -> int:
    metadata = {} if mention_at is None else {"mention_fetched_at": mention_at.isoformat()}
    context = None if context_at is None else json.dumps({"updated_at": context_at.isoformat()})
    reply_id = db.insert_reply_draft(
        inbound_tweet_id=mention,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text="Can you clarify?",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text="Thanks for asking.",
        relationship_context=context,
        platform_metadata=json.dumps(metadata),
        status=status,
    )
    db.conn.execute("UPDATE reply_queue SET detected_at = ? WHERE id = ?", ((NOW - timedelta(days=1)).isoformat(), reply_id))
    db.conn.commit()
    return int(reply_id)


def _knowledge(db, reply_id: int, ingested_at: datetime) -> None:
    row = db.conn.execute(
        """INSERT INTO knowledge (source_type, source_id, content, ingested_at, created_at)
           VALUES ('curated_article', ?, 'Evidence', ?, ?)""",
        (f"k-{reply_id}", ingested_at.isoformat(), ingested_at.isoformat()),
    )
    db.insert_reply_knowledge_links(reply_id, [(int(row.lastrowid), 0.9)])


def test_flags_stale_and_missing_context_fields(db):
    stale = _reply(
        db,
        "mention-1",
        context_at=NOW - timedelta(days=20),
        mention_at=NOW - timedelta(days=1),
    )
    _knowledge(db, stale, NOW - timedelta(days=25))
    missing = _reply(db, "mention-2", context_at=None, mention_at=None)

    report = build_reply_draft_context_staleness_report(db, context_max_age=14, now=NOW)

    assert [finding.draft_id for finding in report.findings] == [missing, stale]
    assert report.findings[0].severity == "high"
    assert "missing_relationship_context" in report.findings[0].stale_fields
    assert report.findings[1].stale_fields == ("relationship_context", "cited_knowledge")
    assert report.findings[1].age_days == 25
    assert report.totals["draft_count"] == 2


def test_window_limit_formatters_and_json_are_stable(db):
    recent = _reply(db, "recent", context_at=NOW - timedelta(days=30), mention_at=NOW - timedelta(days=30))
    old = _reply(db, "old", context_at=NOW - timedelta(days=30), mention_at=NOW - timedelta(days=30))
    db.conn.execute("UPDATE reply_queue SET detected_at = ? WHERE id = ?", ((NOW - timedelta(days=90)).isoformat(), old))
    db.conn.commit()

    report = build_reply_draft_context_staleness_report(db, days=7, limit=1, context_max_age=14, now=NOW)
    payload = json.loads(format_reply_draft_context_staleness_json(report))
    text = format_reply_draft_context_staleness_text(report)

    assert payload["artifact_type"] == "reply_draft_context_staleness"
    assert len(payload["findings"]) == 1
    assert payload["findings"][0]["draft_id"] == recent
    assert "Reply Draft Context Staleness" in text
    assert "mention=recent" in text


def test_missing_optional_knowledge_tables_warns_without_crashing():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            status TEXT,
            inbound_tweet_id TEXT,
            inbound_author_handle TEXT,
            relationship_context TEXT,
            platform_metadata TEXT,
            detected_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO reply_queue VALUES (1, 'pending', 'm1', 'bob', NULL, '{}', ?)",
        (NOW.isoformat(),),
    )

    report = build_reply_draft_context_staleness_report(conn, now=NOW)

    assert report.findings[0].draft_id == 1
    assert report.schema_warnings == (
        "missing optional table: reply_knowledge_links",
        "missing optional table: knowledge",
    )


def test_cli_outputs_json(db, monkeypatch, capsys):
    _reply(db, "cli", context_at=None, mention_at=None)
    monkeypatch.setattr(reply_draft_context_staleness_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        reply_draft_context_staleness_script,
        "build_reply_draft_context_staleness_report",
        lambda db, **kwargs: build_reply_draft_context_staleness_report(db, now=NOW, **kwargs),
    )

    assert reply_draft_context_staleness_script.main(["--days", "7", "--context-max-age", "3", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["findings"][0]["mention_id"] == "cli"
