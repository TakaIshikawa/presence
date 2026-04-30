"""Tests for curated knowledge source overlap reporting."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_overlap import (
    build_source_overlap_report,
    normalize_author,
    normalize_domain,
)
from knowledge_source_overlap import format_text_report, main
from storage.db import Database


NOW = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    schema_path = Path(__file__).parent.parent / "schema.sql"
    connection.executescript(schema_path.read_text())
    yield connection
    connection.close()


@pytest.fixture
def test_db(tmp_path):
    db = Database(str(tmp_path / "presence.db"))
    db.connect()
    db.init_schema(str(Path(__file__).parent.parent / "schema.sql"))
    yield db
    db.close()


def iso(days_ago: int) -> str:
    return (NOW - timedelta(days=days_ago)).isoformat()


def insert_knowledge(
    conn,
    *,
    source_type: str = "curated_article",
    source_id: str,
    source_url: str | None,
    author: str | None = "Expert",
    content: str = "Teams should use staged rollout gates for safer launches.",
    insight: str | None = "Use staged rollout gates for safer launches.",
    approved: int = 1,
    license_value: str = "attribution_required",
    days_ago: int = 1,
) -> int:
    connection = conn.conn if hasattr(conn, "conn") else conn
    cursor = connection.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight, approved,
            license, published_at, ingested_at, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            source_type,
            source_id,
            source_url,
            author,
            content,
            insight,
            approved,
            license_value,
            iso(days_ago),
            iso(days_ago),
            iso(days_ago),
        ),
    )
    connection.commit()
    return cursor.lastrowid


def test_domain_normalization_groups_www_and_case(conn):
    assert normalize_domain("HTTPS://WWW.Example.com/posts/one") == "example.com"
    assert normalize_domain("example.com/articles/two") == "example.com"

    left_id = insert_knowledge(
        conn,
        source_id="a1",
        source_url="https://www.Example.com/posts/one",
    )
    right_id = insert_knowledge(
        conn,
        source_id="b1",
        source_url="https://another.test/posts/one",
    )
    insert_knowledge(
        conn,
        source_id="a2",
        source_url="https://example.com/posts/two",
        insight="Reliability reviews catch launch risk before incidents.",
    )
    insert_knowledge(
        conn,
        source_id="b2",
        source_url="https://another.test/posts/two",
        insight="Reliability reviews catch launch risk before incidents.",
    )

    report = build_source_overlap_report(conn, min_overlap=2, now=NOW)

    assert report.pair_count == 1
    pair = report.pairs[0]
    assert pair.left_source.identifier == "another.test"
    assert pair.right_source.identifier == "example.com"
    assert set(pair.representative_item_ids[:2]) == {left_id, right_id}


def test_author_fallback_when_source_url_missing(conn):
    assert normalize_author("@ALICE ") == "alice"

    insert_knowledge(conn, source_id="a1", source_url=None, author="@Alice")
    insert_knowledge(conn, source_id="b1", source_url=None, author="Bob")
    insert_knowledge(
        conn,
        source_id="a2",
        source_url=None,
        author="alice",
        insight="Incident reviews should include customer-visible impact.",
    )
    insert_knowledge(
        conn,
        source_id="b2",
        source_url=None,
        author="bob",
        insight="Incident reviews should include customer-visible impact.",
    )

    report = build_source_overlap_report(conn, min_overlap=2, now=NOW)

    assert [(pair.left_source.label, pair.right_source.label) for pair in report.pairs] == [
        ("author:alice", "author:bob")
    ]


def test_restricted_and_unapproved_rows_are_excluded_by_default(conn):
    insert_knowledge(conn, source_id="approved-a", source_url="https://a.test/1")
    insert_knowledge(conn, source_id="approved-b", source_url="https://b.test/1")
    insert_knowledge(
        conn,
        source_id="restricted-a",
        source_url="https://restricted-a.test/1",
        license_value="restricted",
    )
    insert_knowledge(
        conn,
        source_id="restricted-b",
        source_url="https://restricted-b.test/1",
        license_value="restricted",
    )
    insert_knowledge(
        conn,
        source_id="unapproved-a",
        source_url="https://unapproved-a.test/1",
        approved=0,
    )
    insert_knowledge(
        conn,
        source_id="unapproved-b",
        source_url="https://unapproved-b.test/1",
        approved=0,
    )

    report = build_source_overlap_report(conn, min_overlap=1, now=NOW)
    unrestricted_labels = {
        label
        for pair in report.pairs
        for label in (pair.left_source.label, pair.right_source.label)
    }
    assert unrestricted_labels == {"domain:a.test", "domain:b.test"}

    with_restricted = build_source_overlap_report(
        conn,
        min_overlap=1,
        include_restricted=True,
        now=NOW,
    )
    labels = {
        label
        for pair in with_restricted.pairs
        for label in (pair.left_source.label, pair.right_source.label)
    }
    assert "domain:restricted-a.test" in labels
    assert "domain:restricted-b.test" in labels
    assert "domain:unapproved-a.test" not in labels


def test_deterministic_ranking_by_count_similarity_then_source_label(conn):
    for index in range(3):
        insert_knowledge(
            conn,
            source_id=f"a{index}",
            source_url="https://a.test/post",
            insight=f"Shared rollout playbook item {index} needs metrics and rollback.",
        )
        insert_knowledge(
            conn,
            source_id=f"b{index}",
            source_url="https://b.test/post",
            insight=f"Shared rollout playbook item {index} needs metrics and rollback.",
        )
    for index in range(2):
        insert_knowledge(
            conn,
            source_id=f"c{index}",
            source_url="https://c.test/post",
            insight=f"Cache invalidation runbook item {index} needs alerts.",
        )
        insert_knowledge(
            conn,
            source_id=f"d{index}",
            source_url="https://d.test/post",
            insight=f"Cache invalidation runbook item {index} needs alerts plus owner.",
        )

    report = build_source_overlap_report(
        conn,
        min_overlap=2,
        similarity_threshold=0.7,
        now=NOW,
    )

    assert [(pair.left_source.identifier, pair.right_source.identifier) for pair in report.pairs[:2]] == [
        ("a.test", "b.test"),
        ("c.test", "d.test"),
    ]
    assert report.pairs[0].overlap_count > report.pairs[1].overlap_count


def test_json_and_text_output_are_readable(test_db, capsys):
    insert_knowledge(test_db, source_id="a1", source_url="https://a.test/1")
    insert_knowledge(test_db, source_id="b1", source_url="https://b.test/1")

    @contextmanager
    def fake_script_context():
        yield None, test_db

    with patch("knowledge_source_overlap.script_context", fake_script_context):
        assert main(["--days", "30", "--min-overlap", "1", "--format", "json"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["pair_count"] == 1
    assert payload["pairs"][0]["overlap_count"] == 1

    report = build_source_overlap_report(test_db.conn, min_overlap=1, now=NOW)
    text = format_text_report(report)
    assert "Knowledge Source Overlap Report" in text
    assert "domain:a.test <> domain:b.test" in text
    assert "suggested_action=" in text
