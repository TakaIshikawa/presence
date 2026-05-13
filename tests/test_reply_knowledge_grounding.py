"""Tests for reply knowledge grounding reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import sqlite3

import pytest

from engagement.reply_knowledge_grounding import (
    build_reply_knowledge_grounding_report,
    format_reply_knowledge_grounding_json,
    format_reply_knowledge_grounding_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_knowledge_grounding.py"
spec = importlib.util.spec_from_file_location("reply_knowledge_grounding_script", SCRIPT_PATH)
reply_knowledge_grounding_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_knowledge_grounding_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _reply(db, inbound_id: str, handle: str, *, status: str = "pending") -> int:
    reply_id = db.insert_reply_draft(
        inbound_tweet_id=inbound_id,
        inbound_author_handle=handle,
        inbound_author_id=f"{handle}-id",
        inbound_text="Question",
        our_tweet_id="ours",
        our_content_id=None,
        our_post_text="Post",
        draft_text="Reply draft",
        platform="x",
        status=status,
        inbound_url=None,
    )
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        ("2026-05-02T10:00:00+00:00", reply_id),
    )
    db.conn.commit()
    return reply_id


def _knowledge(db, knowledge_id: int) -> None:
    db.conn.execute(
        """INSERT INTO knowledge (id, source_type, source_id, content)
           VALUES (?, 'own_post', ?, 'Useful context')""",
        (knowledge_id, f"k-{knowledge_id}"),
    )
    db.conn.commit()


def _link(db, reply_id: int, knowledge_id: int, relevance: float) -> None:
    _knowledge(db, knowledge_id)
    db.conn.execute(
        """INSERT INTO reply_knowledge_links
           (reply_queue_id, knowledge_id, relevance_score)
           VALUES (?, ?, ?)""",
        (reply_id, knowledge_id, relevance),
    )
    db.conn.commit()


def test_classifies_grounding_statuses_and_counts_by_reply_status(db):
    grounded = _reply(db, "grounded", "alice", status="approved")
    weak = _reply(db, "weak", "bob", status="pending")
    ungrounded = _reply(db, "none", "cam", status="pending")
    posted = _reply(db, "posted", "dee", status="posted")
    _link(db, grounded, 1, 0.9)
    _link(db, weak, 2, 0.4)

    report = build_reply_knowledge_grounding_report(db, min_relevance=0.7, now=NOW)

    by_id = {item["reply_queue_id"]: item for item in report["items"]}
    assert by_id[grounded]["grounding_status"] == "grounded"
    assert by_id[weak]["grounding_status"] == "weakly_grounded"
    assert by_id[ungrounded]["grounding_status"] == "ungrounded"
    assert by_id[posted]["grounding_status"] == "posted_without_grounding"
    assert report["totals"]["by_grounding_status"] == {
        "grounded": 1,
        "posted_without_grounding": 1,
        "ungrounded": 1,
        "weakly_grounded": 1,
    }
    assert report["totals"]["by_reply_status"] == {
        "approved": 1,
        "pending": 2,
        "posted": 1,
    }


def test_items_include_required_grounding_fields_and_limit(db):
    first = _reply(db, "first", "alice")
    second = _reply(db, "second", "bob")
    _link(db, first, 10, 0.8)
    _link(db, first, 11, 0.7)

    report = build_reply_knowledge_grounding_report(db, limit=1, now=NOW)

    assert len(report["items"]) == 1
    item = {row["reply_queue_id"]: row for row in build_reply_knowledge_grounding_report(db, now=NOW)["items"]}[first]
    assert item["author_handle"] == "alice"
    assert item["status"] == "pending"
    assert item["knowledge_link_count"] == 2
    assert item["max_relevance"] == 0.8
    assert item["representative_knowledge_ids"] == [10, 11]
    assert second in {
        row["reply_queue_id"] for row in build_reply_knowledge_grounding_report(db, now=NOW)["items"]
    }


def test_rows_without_reply_knowledge_links_do_not_crash():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            inbound_author_handle TEXT,
            status TEXT,
            draft_text TEXT,
            detected_at TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO reply_queue
           VALUES (1, 'alice', 'pending', 'Draft', '2026-05-02T09:00:00+00:00')"""
    )

    report = build_reply_knowledge_grounding_report(conn, now=NOW)

    assert report["missing_optional_tables"] == ["reply_knowledge_links", "knowledge"]
    assert report["items"][0]["grounding_status"] == "ungrounded"
    assert report["items"][0]["knowledge_link_count"] == 0
    conn.close()


def test_json_text_and_cli_formatting_are_stable(db, monkeypatch, capsys):
    reply_id = _reply(db, "cli", "alice")
    _link(db, reply_id, 1, 0.9)

    report = build_reply_knowledge_grounding_report(db, min_relevance=0.8, limit=5, now=NOW)
    payload = json.loads(format_reply_knowledge_grounding_json(report))
    text = format_reply_knowledge_grounding_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert "Reply Knowledge Grounding" in text
    assert "grounded=1" in text

    monkeypatch.setattr(
        reply_knowledge_grounding_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        reply_knowledge_grounding_script,
        "build_reply_knowledge_grounding_report",
        lambda db, **kwargs: build_reply_knowledge_grounding_report(db, now=NOW, **kwargs),
    )
    assert reply_knowledge_grounding_script.main(["--min-relevance", "0.8", "--limit", "5", "--format", "json"]) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["min_relevance"] == 0.8
    assert reply_knowledge_grounding_script.main(["--min-relevance", "2"]) == 2
    assert "value must be between 0 and 1" in capsys.readouterr().err


def test_missing_schema_and_invalid_args_are_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_reply_knowledge_grounding_report(conn, now=NOW)
    assert report["missing_tables"] == ["reply_queue"]

    with pytest.raises(ValueError, match="lookback_days must be positive"):
        build_reply_knowledge_grounding_report(conn, lookback_days=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_reply_knowledge_grounding_report(conn, limit=0, now=NOW)
    with pytest.raises(ValueError, match="min_relevance must be between 0 and 1"):
        build_reply_knowledge_grounding_report(conn, min_relevance=1.1, now=NOW)
    conn.close()
