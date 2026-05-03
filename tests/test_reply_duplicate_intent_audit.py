"""Tests for reply duplicate-intent auditing."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_duplicate_intent_audit import (
    build_reply_duplicate_intent_audit,
    format_reply_duplicate_intent_audit_json,
    format_reply_duplicate_intent_audit_markdown,
    normalize_reply_intent_text,
    normalize_reply_recipient,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "audit_reply_duplicate_intents.py"
spec = importlib.util.spec_from_file_location("audit_reply_duplicate_intents_script", SCRIPT_PATH)
audit_reply_duplicate_intents_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(audit_reply_duplicate_intents_script)


@contextmanager
def _script_context(conn):
    yield SimpleNamespace(), conn


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            status TEXT,
            platform TEXT,
            inbound_tweet_id TEXT,
            inbound_cid TEXT,
            inbound_url TEXT,
            inbound_author_handle TEXT,
            inbound_author_id TEXT,
            draft_text TEXT,
            intent TEXT,
            relationship_context TEXT,
            quality_score REAL,
            detected_at TEXT
        )"""
    )
    return conn


def _insert(conn: sqlite3.Connection, **kwargs) -> int:
    defaults = {
        "status": "pending",
        "platform": "x",
        "inbound_tweet_id": "mention-1",
        "inbound_cid": None,
        "inbound_url": None,
        "inbound_author_handle": "Alice",
        "inbound_author_id": "author-a",
        "draft_text": "Thanks for asking. I would start with the retry budget.",
        "intent": "question",
        "relationship_context": None,
        "quality_score": None,
        "detected_at": "2026-05-02 10:00:00",
    }
    defaults.update(kwargs)
    columns = tuple(defaults)
    placeholders = ", ".join("?" for _ in columns)
    cursor = conn.execute(
        f"INSERT INTO reply_queue ({', '.join(columns)}) VALUES ({placeholders})",
        tuple(defaults[column] for column in columns),
    )
    conn.commit()
    return int(cursor.lastrowid)


def test_groups_drafts_with_same_target_mention_and_selects_highest_quality_canonical():
    conn = _conn()
    older = _insert(
        conn,
        inbound_tweet_id="tweet-123",
        draft_text="First answer.",
        quality_score=6.0,
        detected_at="2026-05-02 08:00:00",
    )
    better = _insert(
        conn,
        inbound_tweet_id="tweet-123",
        draft_text="Second answer with more context.",
        quality_score=8.5,
        detected_at="2026-05-02 11:00:00",
    )

    report = build_reply_duplicate_intent_audit(conn, now=NOW)
    payload = json.loads(format_reply_duplicate_intent_audit_json(report))

    assert report.ok is False
    assert payload["duplicate_group_count"] == 1
    assert payload["duplicate_draft_count"] == 1
    assert payload["groups"][0]["canonical_draft_id"] == better
    assert payload["groups"][0]["duplicate_draft_ids"] == [older]
    assert payload["groups"][0]["confidence"] == 1.0
    assert payload["groups"][0]["reasons"] == ["same_target_mention"]
    assert "target:x:tweet-123" in payload["groups"][0]["normalized_intent_keys"]


def test_groups_equivalent_normalized_text_and_recipient_when_ids_differ():
    conn = _conn()
    first = _insert(
        conn,
        inbound_tweet_id="mention-a",
        inbound_author_handle="@Casey",
        draft_text="Thanks @Casey! Here's the write-up: https://example.com/post",
        detected_at="2026-05-02 08:00:00",
    )
    second = _insert(
        conn,
        inbound_tweet_id="mention-b",
        inbound_author_handle="casey",
        draft_text="thanks here's the write up",
        detected_at="2026-05-02 10:00:00",
    )

    report = build_reply_duplicate_intent_audit(conn, now=NOW)
    group = report.groups[0]

    assert group.canonical_draft_id == first
    assert group.duplicate_draft_ids == (second,)
    assert group.confidence == 0.94
    assert group.reasons == ("same_recipient_normalized_text",)
    assert group.normalized_intent_keys == ("recipient_text:casey:thanks heres the write up",)
    assert normalize_reply_recipient("@Casey") == "casey"
    assert normalize_reply_intent_text("Thanks @Casey! Here's the write-up: https://x.y") == (
        "thanks heres the write up"
    )


def test_distinct_replies_are_not_grouped_and_filters_apply():
    conn = _conn()
    _insert(
        conn,
        inbound_tweet_id="mention-a",
        inbound_author_handle="alice",
        draft_text="Here is the rollout plan.",
        platform="x",
    )
    _insert(
        conn,
        inbound_tweet_id="mention-b",
        inbound_author_handle="bob",
        draft_text="Here is the rollout plan.",
        platform="x",
    )
    _insert(
        conn,
        inbound_tweet_id="mention-c",
        inbound_author_handle="alice",
        draft_text="Here is the rollout plan.",
        platform="bluesky",
        detected_at="2026-04-01 10:00:00",
    )

    report = build_reply_duplicate_intent_audit(conn, days=7, platform="x", now=NOW)

    assert report.ok is True
    assert report.audited_count == 2
    assert report.groups == ()


def test_oldest_is_canonical_when_quality_metadata_is_absent():
    conn = _conn()
    oldest = _insert(
        conn,
        inbound_tweet_id="mention-a",
        inbound_author_handle="devon",
        draft_text="I would split the migration into batches.",
        detected_at="2026-05-02 08:00:00",
    )
    newer = _insert(
        conn,
        inbound_tweet_id="mention-b",
        inbound_author_handle="Devon",
        draft_text="I would split the migration into batches!",
        detected_at="2026-05-02 09:00:00",
    )

    report = build_reply_duplicate_intent_audit(conn, now=NOW)

    assert report.groups[0].canonical_draft_id == oldest
    assert report.groups[0].duplicate_draft_ids == (newer,)


def test_missing_reply_queue_or_required_columns_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    missing_table = build_reply_duplicate_intent_audit(conn, now=NOW)

    conn.execute("CREATE TABLE reply_queue (id INTEGER PRIMARY KEY)")
    conn.execute("INSERT INTO reply_queue (id) VALUES (1)")
    conn.commit()
    missing_columns = build_reply_duplicate_intent_audit(conn, now=NOW)

    assert missing_table.ok is True
    assert missing_table.missing_tables == ("reply_queue",)
    assert missing_columns.ok is True
    assert missing_columns.missing_columns == {"reply_queue": ("draft_text",)}


def test_cli_outputs_json_and_markdown_and_returns_issue_status(monkeypatch, capsys):
    conn = _conn()
    _insert(conn, inbound_tweet_id="same-target", draft_text="One")
    _insert(conn, inbound_tweet_id="same-target", draft_text="Two")
    monkeypatch.setattr(
        audit_reply_duplicate_intents_script,
        "script_context",
        lambda: _script_context(conn),
    )

    json_exit = audit_reply_duplicate_intents_script.main(["--format", "json", "--limit", "5"])
    payload = json.loads(capsys.readouterr().out)

    assert json_exit == 1
    assert payload["artifact_type"] == "reply_duplicate_intent_audit"
    assert payload["groups"][0]["reasons"] == ["same_target_mention"]

    markdown_exit = audit_reply_duplicate_intents_script.main(["--format", "markdown"])
    markdown = capsys.readouterr().out
    assert markdown_exit == 1
    assert "# Reply Duplicate Intent Audit" in markdown
    assert "reply_queue:" in markdown

    invalid = audit_reply_duplicate_intents_script.main(["--days", "0"])
    captured = capsys.readouterr()
    assert invalid == 2
    assert "value must be positive" in captured.err
