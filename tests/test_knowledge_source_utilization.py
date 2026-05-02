"""Tests for knowledge source utilization reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from knowledge.source_utilization import (
    build_knowledge_source_utilization_report,
    format_knowledge_source_utilization_json,
    format_knowledge_source_utilization_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "knowledge_source_utilization.py"
)
spec = importlib.util.spec_from_file_location("knowledge_source_utilization_script", SCRIPT_PATH)
knowledge_source_utilization_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(knowledge_source_utilization_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _add_knowledge(
    db,
    *,
    source_type: str = "curated_article",
    source_id: str,
    source_url: str | None,
    author: str | None,
    days_ago: int,
) -> int:
    ingested_at = (NOW - timedelta(days=days_ago)).isoformat()
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            approved, ingested_at, created_at)
           VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)""",
        (
            source_type,
            source_id,
            source_url,
            author,
            f"content {source_id}",
            f"insight {source_id}",
            ingested_at,
            ingested_at,
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _add_content_link(db, knowledge_id: int) -> None:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=f"post {knowledge_id}",
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])


def _add_reply_link(db, knowledge_id: int) -> None:
    db.conn.execute(
        """INSERT INTO reply_knowledge_links
           (reply_queue_id, knowledge_id, relevance_score, created_at)
           VALUES (NULL, ?, 0.8, ?)""",
        (knowledge_id, NOW.isoformat()),
    )
    db.conn.commit()


def test_report_ranks_high_unused_sources_and_counts_both_link_tables(db):
    alpha_ids = [
        _add_knowledge(
            db,
            source_id=f"alpha-{index}",
            source_url="https://alpha.example/source",
            author="Alpha",
            days_ago=index + 1,
        )
        for index in range(4)
    ]
    beta_ids = [
        _add_knowledge(
            db,
            source_id=f"beta-{index}",
            source_url="https://beta.example/source",
            author="Beta",
            days_ago=index + 1,
        )
        for index in range(3)
    ]
    gamma_ids = [
        _add_knowledge(
            db,
            source_id=f"gamma-{index}",
            source_url="https://gamma.example/source",
            author="Gamma",
            days_ago=index + 1,
        )
        for index in range(2)
    ]
    _add_content_link(db, alpha_ids[0])
    _add_reply_link(db, alpha_ids[1])
    _add_content_link(db, gamma_ids[0])

    report = build_knowledge_source_utilization_report(
        db,
        days=30,
        min_items=2,
        unused_threshold=0.5,
        now=NOW,
    )

    assert [source.author for source in report.sources] == ["Beta", "Alpha", "Gamma"]
    beta = report.sources[0]
    assert beta.knowledge_ids == tuple(beta_ids)
    assert beta.unused_percentage == 1.0
    assert beta.link_count == 0
    alpha = report.sources[1]
    assert alpha.unused_percentage == 0.5
    assert alpha.used_count == 2
    assert alpha.unused_count == 2
    assert alpha.content_link_count == 1
    assert alpha.reply_link_count == 1
    assert report.totals["knowledge_item_count"] == 9
    assert report.totals["unused_item_count"] == 6


def test_author_is_part_of_source_grouping(db):
    alice_ids = [
        _add_knowledge(
            db,
            source_id=f"alice-{index}",
            source_url="https://shared.example/post",
            author="Alice",
            days_ago=index + 1,
        )
        for index in range(2)
    ]
    bob_ids = [
        _add_knowledge(
            db,
            source_id=f"bob-{index}",
            source_url="https://shared.example/post",
            author="Bob",
            days_ago=index + 1,
        )
        for index in range(2)
    ]
    _add_content_link(db, alice_ids[0])

    report = build_knowledge_source_utilization_report(
        db,
        days=30,
        min_items=2,
        unused_threshold=0.0,
        now=NOW,
    )

    groups = {source.author: source for source in report.sources}
    assert set(groups) == {"Alice", "Bob"}
    assert groups["Alice"].knowledge_ids == tuple(alice_ids)
    assert groups["Alice"].unused_percentage == 0.5
    assert groups["Bob"].knowledge_ids == tuple(bob_ids)
    assert groups["Bob"].unused_percentage == 1.0


def test_thresholds_filter_reported_sources(db):
    large_ids = [
        _add_knowledge(
            db,
            source_id=f"large-{index}",
            source_url="https://large.example/source",
            author="Large",
            days_ago=index + 1,
        )
        for index in range(4)
    ]
    small_ids = [
        _add_knowledge(
            db,
            source_id=f"small-{index}",
            source_url="https://small.example/source",
            author="Small",
            days_ago=index + 1,
        )
        for index in range(2)
    ]
    _add_content_link(db, large_ids[0])

    report = build_knowledge_source_utilization_report(
        db,
        days=30,
        min_items=3,
        unused_threshold=0.75,
        now=NOW,
    )

    assert [source.author for source in report.sources] == ["Large"]
    assert report.sources[0].knowledge_ids == tuple(large_ids)
    assert all(source.author != "Small" for source in report.sources)
    assert tuple(small_ids)


def test_missing_optional_columns_and_empty_tables_are_graceful():
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
        VALUES (1, 'curated_article', 'one');
        INSERT INTO content_knowledge_links (id, knowledge_id)
        VALUES (1, 1);
        """
    )

    report = build_knowledge_source_utilization_report(
        conn,
        days=30,
        min_items=1,
        unused_threshold=0.0,
        now=NOW,
    )

    assert report.availability["content_knowledge_links"] is True
    assert report.availability["reply_knowledge_links"] is False
    assert report.missing_columns["knowledge"] == (
        "source_url",
        "author",
        "ingested_at",
        "created_at",
        "approved",
    )
    assert report.sources[0].source_type == "curated_article"
    assert report.sources[0].author is None
    assert report.sources[0].link_count == 1

    empty = sqlite3.connect(":memory:")
    empty.row_factory = sqlite3.Row
    empty.execute("CREATE TABLE knowledge (id INTEGER PRIMARY KEY, source_type TEXT)")
    empty_report = build_knowledge_source_utilization_report(empty, now=NOW)

    assert empty_report.sources == ()
    assert "No underutilized knowledge sources found." in (
        format_knowledge_source_utilization_text(empty_report)
    )


def test_formatters_are_deterministic(db):
    _add_knowledge(
        db,
        source_id="stable-1",
        source_url="https://stable.example/source",
        author="Stable",
        days_ago=1,
    )
    _add_knowledge(
        db,
        source_id="stable-2",
        source_url="https://stable.example/source",
        author="Stable",
        days_ago=2,
    )

    report = build_knowledge_source_utilization_report(
        db,
        days=30,
        min_items=2,
        unused_threshold=0.5,
        now=NOW,
    )
    payload = json.loads(format_knowledge_source_utilization_json(report))
    text = format_knowledge_source_utilization_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["artifact_type"] == "knowledge_source_utilization"
    assert payload["sources"][0]["unused_percentage"] == 1.0
    assert "Knowledge Source Utilization" in text
    assert "Stable" in text
    assert "unused=100%" in text


def test_cli_supports_requested_flags(db, capsys):
    _add_knowledge(
        db,
        source_id="cli-1",
        source_url="https://cli.example/source",
        author="CLI",
        days_ago=1,
    )
    _add_knowledge(
        db,
        source_id="cli-2",
        source_url="https://cli.example/source",
        author="CLI",
        days_ago=2,
    )
    fixed_report = build_knowledge_source_utilization_report(
        db,
        days=30,
        min_items=2,
        unused_threshold=0.5,
        now=NOW,
    )

    with patch.object(
        knowledge_source_utilization_script,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        knowledge_source_utilization_script,
        "build_knowledge_source_utilization_report",
        return_value=fixed_report,
    ):
        result = knowledge_source_utilization_script.main(
            [
                "--days",
                "30",
                "--min-items",
                "2",
                "--unused-threshold",
                "0.5",
                "--format",
                "json",
            ]
        )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"] == {
        "days": 30,
        "min_items": 2,
        "unused_threshold": 0.5,
    }
    assert payload["sources"][0]["author"] == "CLI"
