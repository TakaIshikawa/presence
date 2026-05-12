"""Tests for content source deadend reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.content_source_deadends import (
    build_content_source_deadends_report,
    format_content_source_deadends_json,
    format_content_source_deadends_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_source_deadends.py"
spec = importlib.util.spec_from_file_location("content_source_deadends_script", SCRIPT_PATH)
content_source_deadends_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_source_deadends_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, content_type: str = "x_post", *, days_ago: int = 1, published_url: str | None = None) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=f"{content_type} content",
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ?, published_url = ? WHERE id = ?",
        ((NOW - timedelta(days=days_ago)).isoformat(), published_url, content_id),
    )
    db.conn.commit()
    return int(content_id)


def _knowledge(db, *, source_type: str = "curated_article", source_id: str = "source.example", author: str = "source.example") -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, approved, ingested_at)
           VALUES (?, ?, ?, ?, 'source chunk', 1, ?)""",
        (
            source_type,
            source_id,
            f"https://{source_id}/post",
            author,
            NOW.isoformat(),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _link(db, content_id: int, knowledge_id: int) -> None:
    db.conn.execute(
        "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, 0.8)",
        (content_id, knowledge_id),
    )
    db.conn.commit()


def _curated_source(db, identifier: str, *, active: int = 1, status: str = "active") -> int:
    cursor = db.conn.execute(
        """INSERT INTO curated_sources
           (source_type, identifier, name, active, status)
           VALUES ('blog', ?, ?, ?, ?)""",
        (identifier, identifier, active, status),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_detects_missing_inactive_and_publication_deadends_grouped_by_content(db):
    missing_source_content = _content(db)
    missing_source_knowledge = _knowledge(db, source_id="missing.example")
    _link(db, missing_source_content, missing_source_knowledge)

    inactive_content = _content(db)
    inactive_knowledge = _knowledge(db, source_id="inactive.example")
    inactive_source_id = _curated_source(db, "inactive.example", active=0, status="paused")
    _link(db, inactive_content, inactive_knowledge)

    missing_chunk_content = _content(db)
    db.conn.execute(
        "INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score) VALUES (?, ?, 0.8)",
        (missing_chunk_content, 9999),
    )
    db.conn.commit()

    publication_content = _content(db)
    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, published_at)
           VALUES (?, 'x', 'published', ?)""",
        (publication_content, NOW.isoformat()),
    )
    db.conn.commit()

    report = build_content_source_deadends_report(db, days=7, limit=10, now=NOW)
    by_reason = {label for group in report.groups for label in group.reason_labels}

    assert by_reason == {
        "broken_publication_source_join",
        "inactive_curated_source",
        "missing_knowledge_chunk",
        "missing_source_row",
        "no_knowledge_chunks",
    }
    inactive_group = next(group for group in report.groups if "inactive_curated_source" in group.reason_labels)
    assert inactive_group.findings[0].curated_source_id == inactive_source_id
    assert report.totals["by_reason"]["missing_knowledge_chunk"] == 1


def test_filters_by_days_content_type_limit_and_formats_json(db):
    included = _content(db, "blog_post")
    _link(db, included, _knowledge(db, source_id="included.example"))
    old = _content(db, "blog_post", days_ago=90)
    _link(db, old, _knowledge(db, source_id="old.example"))
    wrong_type = _content(db, "x_post")
    _link(db, wrong_type, _knowledge(db, source_id="wrong.example"))

    report = build_content_source_deadends_report(
        db,
        days=7,
        limit=1,
        content_type="blog_post",
        now=NOW,
    )
    payload = json.loads(format_content_source_deadends_json(report))
    text = format_content_source_deadends_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "content_source_deadends"
    assert payload["filters"]["content_type"] == "blog_post"
    assert payload["totals"]["content_count"] == 1
    assert payload["groups"][0]["content_id"] == included
    assert "Content type: blog_post" in text


def test_missing_schema_returns_empty_report_with_warnings():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_content_source_deadends_report(conn, now=NOW)

    assert report.groups == ()
    assert report.schema_warnings == (
        "missing table: generated_content",
        "missing table: content_knowledge_links",
        "missing table: knowledge",
        "missing table: curated_sources",
    )


def test_content_type_validation_and_cli(db, monkeypatch, capsys):
    content_id = _content(db, "x_post")
    _link(db, content_id, _knowledge(db, source_id="cli.example"))
    monkeypatch.setattr(
        content_source_deadends_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        content_source_deadends_script,
        "build_content_source_deadends_report",
        lambda db, **kwargs: build_content_source_deadends_report(db, now=NOW, **kwargs),
    )

    with pytest.raises(ValueError, match="content-type must be one of"):
        build_content_source_deadends_report(db, content_type="podcast", now=NOW)

    assert content_source_deadends_script.main(["--days", "7", "--limit", "5", "--content-type", "x_post", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["groups"][0]["source_reference"] == "https://cli.example/post"

    assert content_source_deadends_script.main(["--content-type", "podcast"]) == 1
    assert "content-type must be one of" in capsys.readouterr().err
