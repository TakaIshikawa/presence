"""Tests for knowledge source quote opportunity exports."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from knowledge.source_quote_opportunities import (
    build_source_quote_opportunity_report,
    format_source_quote_opportunities_json,
    format_source_quote_opportunities_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "source_quote_opportunities.py"
spec = importlib.util.spec_from_file_location("source_quote_opportunities_script", SCRIPT_PATH)
source_quote_opportunities_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(source_quote_opportunities_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _add_knowledge(
    db,
    *,
    source_id: str,
    content: str | None = None,
    insight: str | None = None,
    source_url: str | None = None,
    metadata: dict | str | None = None,
    days_ago: int = 1,
    approved: int = 1,
) -> int:
    published_at = (NOW - timedelta(days=days_ago)).isoformat()
    if content is None:
        content = (
            f"{source_id} evidence shows that durable quote-backed posts work best "
            "when they carry a complete source claim, a boundary, and enough context."
        )
    if metadata is None:
        metadata_value = json.dumps(
            {"title": source_id.title(), "topic": "quote workflows", "trust_score": 0.5},
            sort_keys=True,
        )
    elif isinstance(metadata, str):
        metadata_value = metadata
    else:
        metadata_value = json.dumps(metadata, sort_keys=True)
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight,
            approved, published_at, ingested_at, metadata)
           VALUES ('curated_article', ?, ?, 'Ada', ?, ?, ?, ?, ?, ?)""",
        (
            source_id,
            source_url or f"https://example.test/{source_id}",
            content,
            insight,
            approved,
            published_at,
            published_at,
            metadata_value,
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _generated_content(db, text: str, *, knowledge_id: int | None = None) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    if knowledge_id is not None:
        db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])
    return int(content_id)


def test_ranks_fresh_trusted_unused_sources_above_older_weaker_sources(db):
    old_id = _add_knowledge(
        db,
        source_id="older",
        days_ago=70,
        metadata={"title": "Older", "topic": "systems", "trust_score": 0.3},
    )
    fresh_id = _add_knowledge(
        db,
        source_id="fresh",
        days_ago=3,
        metadata={
            "title": "Fresh",
            "topic": "agent evaluation",
            "trust_score": 0.95,
            "excerpt": (
                "Fresh expert sources make quote-backed posts stronger when the excerpt "
                "contains one concrete claim that can be attributed and checked."
            ),
        },
    )

    report = build_source_quote_opportunity_report(db, days=90, limit=10, now=NOW)
    payload = json.loads(format_source_quote_opportunities_json(report))

    assert [item.source_id for item in report.opportunities][:2] == ["fresh", "older"]
    assert report.opportunities[0].knowledge_id == fresh_id
    assert report.opportunities[1].knowledge_id == old_id
    assert report.opportunities[0].opportunity_score > report.opportunities[1].opportunity_score
    assert payload["artifact_type"] == "source_quote_opportunities"
    assert list(payload) == sorted(payload)
    assert payload["opportunities"][0]["topic_terms"][0] == "agent evaluation"
    assert "Fresh expert sources" in payload["opportunities"][0]["excerpt"]


def test_generated_content_and_ideas_suppress_used_source_ids_and_urls(db):
    unused_id = _add_knowledge(
        db,
        source_id="unused",
        metadata={"title": "Unused", "topic": "topic-a", "trust_score": 0.8},
    )
    linked_id = _add_knowledge(
        db,
        source_id="linked",
        metadata={"title": "Linked", "topic": "topic-b", "trust_score": 0.8},
    )
    url_id = _add_knowledge(
        db,
        source_id="url-used",
        source_url="https://example.test/url-used",
        metadata={"title": "URL Used", "topic": "topic-c", "trust_score": 0.8},
    )
    idea_id = _add_knowledge(
        db,
        source_id="idea-used",
        metadata={"title": "Idea Used", "topic": "topic-d", "trust_score": 0.8},
    )
    _generated_content(db, "already linked", knowledge_id=linked_id)
    _generated_content(db, "Generated copy cites https://example.test/url-used")
    db.conn.execute(
        """INSERT INTO content_ideas (note, topic, source, source_metadata)
           VALUES (?, ?, ?, ?)""",
        (
            "Seeded from idea source",
            "topic-d",
            "source_quote_opportunities",
            json.dumps({"knowledge_id": idea_id, "source_id": "idea-used"}),
        ),
    )
    db.conn.commit()

    report = build_source_quote_opportunity_report(db, days=30, limit=10, now=NOW)
    by_id = {item.knowledge_id: item for item in report.opportunities}

    assert by_id[unused_id].usage_count == 0
    assert by_id[linked_id].usage_count == 1
    assert by_id[url_id].usage_count == 1
    assert by_id[idea_id].usage_count == 1
    assert by_id[unused_id].opportunity_score > by_id[linked_id].opportunity_score
    assert by_id[unused_id].opportunity_score > by_id[url_id].opportunity_score
    assert by_id[unused_id].opportunity_score > by_id[idea_id].opportunity_score


def test_malformed_metadata_is_reported_as_warning_not_crash(db):
    malformed_id = _add_knowledge(
        db,
        source_id="bad-json",
        metadata="{not valid json",
    )
    db.conn.execute(
        """INSERT INTO content_ideas (note, topic, source_metadata)
           VALUES ('bad metadata idea', 'ops', '{also invalid')"""
    )
    db.conn.commit()

    report = build_source_quote_opportunity_report(db, days=30, now=NOW)

    assert report.opportunities[0].knowledge_id == malformed_id
    assert any("knowledge:" in warning and "malformed JSON" in warning for warning in report.warnings)
    assert any("content_ideas:" in warning and "malformed JSON" in warning for warning in report.warnings)


def test_missing_knowledge_table_and_columns_are_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    missing_table = build_source_quote_opportunity_report(conn, now=NOW)

    conn.execute("CREATE TABLE knowledge (id INTEGER PRIMARY KEY)")
    missing_columns = build_source_quote_opportunity_report(conn, now=NOW)
    text = format_source_quote_opportunities_text(missing_table)

    assert missing_table.missing_tables == ("knowledge",)
    assert "Missing tables: knowledge" in text
    assert missing_columns.missing_columns == {"knowledge": ("content",)}


def test_filters_limit_days_and_min_score(db):
    _add_knowledge(
        db,
        source_id="fresh-high",
        days_ago=1,
        metadata={"topic": "fresh", "trust_score": 1.0},
    )
    _add_knowledge(
        db,
        source_id="old-outside-window",
        days_ago=45,
        metadata={"topic": "old", "trust_score": 1.0},
    )
    _add_knowledge(
        db,
        source_id="fresh-low-filtered",
        days_ago=1,
        metadata={"topic": "fresh low", "trust_score": 0.0},
    )

    all_recent = build_source_quote_opportunity_report(db, days=30, limit=10, now=NOW)
    min_score = all_recent.opportunities[0].opportunity_score - 0.001
    filtered = build_source_quote_opportunity_report(
        db,
        days=30,
        limit=1,
        min_score=min_score,
        now=NOW,
    )

    assert {item.source_id for item in all_recent.opportunities} == {
        "fresh-high",
        "fresh-low-filtered",
    }
    assert [item.source_id for item in filtered.opportunities] == ["fresh-high"]


def test_cli_outputs_json_and_text(db, monkeypatch, capsys):
    _add_knowledge(
        db,
        source_id="cli-source",
        metadata={"title": "CLI Source", "topic": "cli", "trust_score": 0.9},
    )
    monkeypatch.setattr(
        source_quote_opportunities_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        source_quote_opportunities_script,
        "build_source_quote_opportunity_report",
        lambda db, **kwargs: build_source_quote_opportunity_report(db, now=NOW, **kwargs),
    )

    json_exit = source_quote_opportunities_script.main(
        ["--days", "30", "--limit", "5", "--min-score", "1", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)
    text_exit = source_quote_opportunities_script.main(["--format", "text"])
    text = capsys.readouterr().out

    assert json_exit == 0
    assert payload["filters"] == {"days": 30, "limit": 5, "min_score": 1.0}
    assert payload["opportunities"][0]["source_id"] == "cli-source"
    assert text_exit == 0
    assert "Source Quote Opportunities" in text
    assert "cli-source" in text
