"""Tests for knowledge source author recency balance reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3

from knowledge.source_author_recency import (
    build_knowledge_source_author_recency_report,
    format_knowledge_source_author_recency_json,
    format_knowledge_source_author_recency_text,
    normalize_author,
    normalize_domain,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def _add_knowledge(
    db,
    *,
    author: str | None,
    source_url: str,
    days_ago: int | None,
    source_id: str,
) -> int:
    timestamp = None if days_ago is None else (NOW - timedelta(days=days_ago)).isoformat()
    return int(
        db.conn.execute(
            """INSERT INTO knowledge
               (source_type, source_id, source_url, author, content, insight,
                approved, published_at, ingested_at, created_at)
               VALUES ('curated_article', ?, ?, ?, ?, ?, 1, ?, ?, ?)""",
            (
                source_id,
                source_url,
                author,
                f"content {source_id}",
                f"insight {source_id}",
                timestamp,
                timestamp,
                timestamp,
            ),
        ).lastrowid
    )


def _content_link(db, knowledge_id: int) -> None:
    content_id = db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published, created_at)
           VALUES (?, 'x_post', 8.0, 0, ?)""",
        (f"post using {knowledge_id}", NOW.isoformat()),
    ).lastrowid
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])


def _reply_link(db, knowledge_id: int) -> None:
    db.conn.execute(
        """INSERT INTO reply_knowledge_links
           (reply_queue_id, knowledge_id, relevance_score, created_at)
           VALUES (1, ?, 0.8, ?)""",
        (knowledge_id, NOW.isoformat()),
    )
    db.conn.commit()


def test_groups_by_normalized_author_domain_and_recency_with_usage_counts(db):
    alice_ids = [
        _add_knowledge(
            db,
            author="@Alice",
            source_url="https://www.example.com/a",
            days_ago=130,
            source_id="alice-old-1",
        ),
        _add_knowledge(
            db,
            author="https://x.com/alice/",
            source_url="example.com/b",
            days_ago=120,
            source_id="alice-old-2",
        ),
    ]
    bob_id = _add_knowledge(
        db,
        author="Bob",
        source_url="https://news.example/bob",
        days_ago=5,
        source_id="bob-fresh",
    )
    quiet_id = _add_knowledge(
        db,
        author=None,
        source_url="",
        days_ago=None,
        source_id="quiet-undated",
    )

    for _ in range(2):
        _content_link(db, alice_ids[0])
    _reply_link(db, alice_ids[1])
    for _ in range(3):
        _content_link(db, bob_id)

    report = build_knowledge_source_author_recency_report(
        db,
        freshness_window_days=90,
        heavy_usage_count=3,
        dominance_threshold=0.5,
        now=NOW,
    )

    rows = {(row.author, row.domain, row.recency_bucket): row for row in report.rows}
    alice = rows[("alice", "example.com", "stale")]
    bob = rows[("bob", "news.example", "fresh")]
    quiet = rows[("(unknown author)", "(unknown domain)", "undated")]

    assert normalize_author("https://twitter.com/Alice/") == "alice"
    assert normalize_domain("https://www.Example.com/path") == "example.com"
    assert alice.item_count == 2
    assert alice.usage_count == 3
    assert alice.knowledge_ids == tuple(alice_ids)
    assert alice.recommended_action == "refresh_author_sources"
    assert bob.usage_count == 3
    assert bob.usage_share == 0.5
    assert bob.recommended_action == "diversify_author"
    assert quiet.usage_count == 0
    assert quiet.recommended_action == "ok"
    assert report.totals["usage_count"] == 6
    assert report.totals["action_counts"] == {
        "diversify_author": 1,
        "ok": 1,
        "refresh_author_sources": 1,
    }


def test_min_items_filters_small_groups_and_formats_json_text(db):
    for index in range(2):
        _add_knowledge(
            db,
            author="Stable",
            source_url="https://stable.example/source",
            days_ago=index + 1,
            source_id=f"stable-{index}",
        )
    _add_knowledge(
        db,
        author="Small",
        source_url="https://small.example/source",
        days_ago=1,
        source_id="small-1",
    )

    report = build_knowledge_source_author_recency_report(
        db,
        min_items=2,
        now=NOW,
    )
    payload = json.loads(format_knowledge_source_author_recency_json(report))
    text = format_knowledge_source_author_recency_text(report)

    assert [row.author for row in report.rows] == ["stable"]
    assert payload["artifact_type"] == "knowledge_source_author_recency"
    assert list(payload) == sorted(payload)
    assert "Knowledge Source Author Recency" in text
    assert "Stable @ stable.example" in text


def test_missing_schema_and_optional_link_availability_are_reported():
    empty = sqlite3.connect(":memory:")
    empty.row_factory = sqlite3.Row
    try:
        missing = build_knowledge_source_author_recency_report(empty, now=NOW)
    finally:
        empty.close()

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY,
            author TEXT,
            source_url TEXT,
            created_at TEXT
        );
        INSERT INTO knowledge (id, author, source_url, created_at)
        VALUES (1, 'Analyst', 'https://example.com/a', '2026-05-01T12:00:00+00:00');
        CREATE TABLE content_knowledge_links (
            id INTEGER PRIMARY KEY,
            knowledge_id INTEGER
        );
        """
    )
    try:
        report = build_knowledge_source_author_recency_report(conn, now=NOW)
    finally:
        conn.close()

    assert missing.missing_tables == ("knowledge",)
    assert report.availability["content_knowledge_links"] is True
    assert report.availability["reply_knowledge_links"] is False
    assert report.missing_columns["knowledge"] == ("published_at", "ingested_at")
    assert report.rows[0].author == "analyst"
