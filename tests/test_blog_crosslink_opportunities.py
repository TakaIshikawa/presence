"""Tests for blog crosslink opportunity reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.blog_crosslink_opportunities import (
    build_blog_crosslink_opportunities_report,
    format_blog_crosslink_opportunities_json,
    format_blog_crosslink_opportunities_text,
)


NOW = datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_crosslink_opportunities.py"
spec = importlib.util.spec_from_file_location("blog_crosslink_opportunities_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
               id INTEGER PRIMARY KEY,
               content_type TEXT NOT NULL,
               content TEXT NOT NULL,
               metadata TEXT,
               published INTEGER DEFAULT 0,
               published_url TEXT,
               published_at TEXT,
               created_at TEXT
           )"""
    )
    return conn


def _insert(
    conn: sqlite3.Connection,
    content_id: int,
    *,
    content_type: str = "blog_post",
    content: str,
    url: str,
    published_at: datetime,
    metadata: dict | None = None,
    published: int = 1,
) -> None:
    conn.execute(
        """INSERT INTO generated_content
           (id, content_type, content, metadata, published, published_url, published_at, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            content_type,
            content,
            json.dumps(metadata or {}),
            published,
            url,
            published_at.isoformat(),
            published_at.isoformat(),
        ),
    )


def test_shared_topic_metadata_ranks_above_token_overlap_and_reports_candidates():
    conn = _conn()
    _insert(
        conn,
        1,
        content="Evidence hygiene checklist",
        url="https://example.test/blog/evidence-hygiene",
        published_at=NOW - timedelta(days=30),
        metadata={"topics": ["evidence"], "category": "evaluation"},
    )
    _insert(
        conn,
        2,
        content="Claim evidence revalidation playbook",
        url="https://example.test/blog/claim-evidence",
        published_at=NOW,
        metadata={"topic": "evidence", "category": "evaluation"},
    )
    _insert(
        conn,
        3,
        content="Prompt version testing claim evidence reports",
        url="https://example.test/blog/prompt-evidence",
        published_at=NOW - timedelta(days=20),
    )

    report = build_blog_crosslink_opportunities_report(conn, min_shared_tokens=2, now=NOW)

    assert report["totals"]["published_blog_count"] == 3
    assert report["opportunities"][0]["source_content_id"] == 2
    assert report["opportunities"][0]["target_content_id"] == 1
    assert report["opportunities"][0]["reason"] == "shared_topic_metadata"
    token_item = next(item for item in report["opportunities"] if item["reason"] == "token_overlap")
    assert report["opportunities"][0]["confidence"] > token_item["confidence"]


def test_skips_when_target_url_already_appears_in_source_content_or_metadata():
    conn = _conn()
    _insert(
        conn,
        1,
        content="Older evaluation post",
        url="https://example.test/blog/older-evaluation",
        published_at=NOW - timedelta(days=10),
        metadata={"topic": "evaluation"},
    )
    _insert(
        conn,
        2,
        content="Newer evaluation post links https://example.test/blog/older-evaluation",
        url="https://example.test/blog/newer-evaluation",
        published_at=NOW,
        metadata={"topic": "evaluation"},
    )

    report = build_blog_crosslink_opportunities_report(conn, now=NOW)

    assert report["opportunities"] == []
    assert report["totals"]["candidate_count"] == 0


def test_considers_blog_like_published_generated_content_and_ignores_unpublished():
    conn = _conn()
    _insert(
        conn,
        1,
        content_type="article",
        content="Launch cadence evidence guide",
        url="https://example.test/blog/cadence",
        published_at=NOW - timedelta(days=5),
    )
    _insert(
        conn,
        2,
        content_type="technical_post",
        content="Launch cadence evidence checklist",
        url="https://example.test/blog/checklist",
        published_at=NOW,
    )
    _insert(
        conn,
        3,
        content_type="blog_post",
        content="Launch cadence evidence draft",
        url="https://example.test/blog/draft",
        published_at=NOW,
        published=0,
    )

    report = build_blog_crosslink_opportunities_report(conn, min_shared_tokens=2, now=NOW)

    assert report["totals"]["published_blog_count"] == 2
    assert report["opportunities"][0]["source_content_id"] == 2
    assert report["opportunities"][0]["target_content_id"] == 1
    assert report["opportunities"][0]["reason"] == "token_overlap"


def test_missing_generated_content_warns_and_formatters_work():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_blog_crosslink_opportunities_report(conn, now=NOW)
    payload = json.loads(format_blog_crosslink_opportunities_json(report))
    text = format_blog_crosslink_opportunities_text(report)

    assert payload["missing_tables"] == ["generated_content"]
    assert "Missing tables: generated_content" in text


def test_script_outputs_json_with_database_path(file_db, capsys):
    first = file_db.insert_generated_content(
        content_type="blog_post",
        source_commits=[],
        source_messages=[],
        content="Evidence cadence guide",
        eval_score=8,
        eval_feedback="ok",
    )
    second = file_db.insert_generated_content(
        content_type="blog_post",
        source_commits=[],
        source_messages=[],
        content="Evidence cadence checklist",
        eval_score=8,
        eval_feedback="ok",
    )
    file_db.conn.execute(
        "UPDATE generated_content SET published = 1, published_url = ?, published_at = ? WHERE id = ?",
        ("https://example.test/blog/guide", (NOW - timedelta(days=5)).isoformat(), first),
    )
    file_db.conn.execute(
        "UPDATE generated_content SET published = 1, published_url = ?, published_at = ? WHERE id = ?",
        ("https://example.test/blog/checklist", NOW.isoformat(), second),
    )
    file_db.conn.commit()

    assert script.main(["--db", str(file_db.db_path), "--min-shared-tokens", "2"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["artifact_type"] == "blog_crosslink_opportunities"
    assert payload["opportunities"][0]["source_content_id"] == second
    assert payload["opportunities"][0]["target_content_id"] == first
