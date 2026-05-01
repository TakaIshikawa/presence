"""Tests for linked knowledge author and domain diversity reporting."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from knowledge.author_diversity import (
    build_knowledge_author_diversity_report,
    format_knowledge_author_diversity_json,
    format_knowledge_author_diversity_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "knowledge_author_diversity.py"
)
spec = importlib.util.spec_from_file_location("knowledge_author_diversity_script", SCRIPT_PATH)
knowledge_author_diversity_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(knowledge_author_diversity_script)


def _set_created_at(db, table: str, row_id: int, created_at: datetime) -> None:
    db.conn.execute(
        f"UPDATE {table} SET created_at = ? WHERE id = ?",
        (created_at.isoformat(), row_id),
    )
    db.conn.commit()


def _add_content(db, created_at: datetime) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=f"post {created_at.isoformat()}",
        eval_score=8.0,
        eval_feedback="ok",
    )
    _set_created_at(db, "generated_content", content_id, created_at)
    return content_id


def _add_knowledge(
    db,
    *,
    source_id: str,
    author: str | None,
    source_url: str | None,
    source_type: str = "curated_x",
    published_at: datetime | None = None,
    metadata: dict | None = None,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            approved, published_at, metadata)
           VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)""",
        (
            source_type,
            source_id,
            source_url,
            author,
            f"content {source_id}",
            f"insight {source_id}",
            published_at.isoformat() if published_at else None,
            json.dumps(metadata) if metadata else None,
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _link(db, content_id: int, knowledge_id: int, created_at: datetime) -> None:
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])
    db.conn.execute(
        """UPDATE content_knowledge_links
           SET created_at = ?
           WHERE id = (SELECT MAX(id) FROM content_knowledge_links)""",
        (created_at.isoformat(),),
    )
    db.conn.commit()


def test_report_flags_author_domain_concentration_and_stale_usage(db):
    content_ids = [_add_content(db, NOW - timedelta(days=1)) for _ in range(6)]
    alice_ids = [
        _add_knowledge(
            db,
            source_id=f"alice-{index}",
            author="Alice",
            source_url=f"https://example.com/a/{index}",
            published_at=NOW - timedelta(days=15 if index < 2 else 220),
        )
        for index in range(4)
    ]
    bob_id = _add_knowledge(
        db,
        source_id="bob",
        author="Bob",
        source_url="https://other.example/bob",
        published_at=NOW - timedelta(days=20),
    )
    unknown_id = _add_knowledge(
        db,
        source_id="unknown",
        author=None,
        source_url=None,
        published_at=NOW - timedelta(days=10),
    )
    _add_knowledge(
        db,
        source_id="newsletter-unused",
        author="Newsletter",
        source_url="https://newsletter.example/issue",
        source_type="curated_newsletter",
        published_at=NOW - timedelta(days=3),
    )

    for content_id, knowledge_id in zip(content_ids, alice_ids + [bob_id, unknown_id]):
        _link(db, content_id, knowledge_id, NOW - timedelta(hours=6))

    report = build_knowledge_author_diversity_report(
        db,
        days=30,
        top_n=3,
        min_usage=5,
        stale_after_days=180,
        now=NOW,
    )
    text = format_knowledge_author_diversity_text(report)

    assert report.total_usage_count == 6
    assert report.unique_content_count == 6
    assert report.top_authors[0].label == "Alice"
    assert report.top_authors[0].share == 0.667
    assert report.top_domains[0].label == "example.com"
    assert report.unknown_author_count == 1
    assert report.unknown_domain_count == 1
    assert report.stale_usage_count == 2
    assert report.warnings == (
        "author concentration: Alice accounts for 67% of linked usage (4/6)",
        "domain concentration: example.com accounts for 67% of linked usage (4/6)",
        "stale source usage: 2/6 linked uses are older than the stale threshold",
    )
    assert report.recommended_underused_source_types[0].source_type == "curated_newsletter"
    assert "Unknown author share: 17%" in text


def test_unknown_author_and_domain_buckets_are_explicit(db):
    content_ids = [_add_content(db, NOW - timedelta(days=1)) for _ in range(3)]
    knowledge_ids = [
        _add_knowledge(db, source_id=f"missing-{index}", author="", source_url="")
        for index in range(2)
    ]
    knowledge_ids.append(
        _add_knowledge(
            db,
            source_id="metadata-domain",
            author="Analyst",
            source_url=None,
            metadata={"link_metadata": {"canonical_url": "https://meta.example/post"}},
        )
    )

    for content_id, knowledge_id in zip(content_ids, knowledge_ids):
        _link(db, content_id, knowledge_id, NOW - timedelta(hours=2))

    report = build_knowledge_author_diversity_report(
        db,
        days=30,
        top_n=5,
        min_usage=2,
        now=NOW,
    )

    assert report.top_authors[0].label == "(unknown author)"
    assert report.top_domains[0].label == "(unknown domain)"
    assert report.top_domains[1].label == "meta.example"
    assert "unknown author data: 2/3 linked uses lack author metadata" in report.warnings
    assert "unknown domain data: 2/3 linked uses lack source_url/domain metadata" in report.warnings


def test_json_formatter_is_deterministic_and_schema_tolerant():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY,
            source_type TEXT,
            content TEXT NOT NULL
        );
        CREATE TABLE content_knowledge_links (
            id INTEGER PRIMARY KEY,
            knowledge_id INTEGER
        );
        INSERT INTO knowledge (id, source_type, content)
        VALUES (1, 'curated_article', 'source');
        INSERT INTO content_knowledge_links (id, knowledge_id)
        VALUES (1, 1);
        """
    )

    report = build_knowledge_author_diversity_report(
        conn,
        days=30,
        top_n=2,
        min_usage=1,
        now=NOW,
    )
    payload = json.loads(format_knowledge_author_diversity_json(report))

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["missing_columns"]["knowledge"] == [
        "source_url",
        "author",
        "published_at",
        "ingested_at",
        "created_at",
    ]
    assert payload["top_authors"][0]["label"] == "(unknown author)"
    assert payload["total_usage_count"] == 1


def test_cli_outputs_json_and_reports_validation_errors(db, capsys):
    content_id = _add_content(db, NOW - timedelta(days=1))
    knowledge_id = _add_knowledge(
        db,
        source_id="cli",
        author="CLI Author",
        source_url="https://cli.example/source",
    )
    _link(db, content_id, knowledge_id, NOW - timedelta(hours=1))

    @contextmanager
    def fake_context():
        yield None, db

    with patch.object(knowledge_author_diversity_script, "script_context", fake_context):
        result = knowledge_author_diversity_script.main(
            ["--days", "30", "--top-n", "2", "--min-usage", "1", "--json"]
        )

    assert result == 0
    output = json.loads(capsys.readouterr().out)
    assert output["top_authors"][0]["label"] == "CLI Author"

    result = knowledge_author_diversity_script.main(["--min-usage", "0"])
    captured = capsys.readouterr()
    assert result == 1
    assert "min_usage must be at least 1" in captured.err
