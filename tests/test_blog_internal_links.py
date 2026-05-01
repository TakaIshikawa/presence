"""Tests for deterministic blog internal link suggestions."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.blog_internal_links import (
    build_blog_internal_link_suggestions,
    format_blog_internal_links_json,
    format_blog_internal_links_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "suggest_blog_links.py"
spec = importlib.util.spec_from_file_location("suggest_blog_links_cli", SCRIPT_PATH)
suggest_blog_links_cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(suggest_blog_links_cli)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _blog(
    db,
    content: str,
    *,
    content_type: str = "blog_post",
    topics: list[tuple[str, str, float]] | None = None,
    published: bool = True,
    url: str | None = "https://example.com/blog/post",
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="ok",
    )
    if topics:
        db.insert_content_topics(content_id, topics)
    if published:
        db.conn.execute(
            """UPDATE generated_content
               SET published = 1, published_url = ?, published_at = ?
               WHERE id = ?""",
            (url, NOW.isoformat(), content_id),
        )
        db.conn.commit()
    return content_id


def _suggestion_by_id(report: dict) -> dict[int, dict]:
    return {item["target_content_id"]: item for item in report["suggestions"]}


def test_suggestions_include_topic_matches_and_required_fields(db, tmp_path):
    target = _blog(
        db,
        "TITLE: Testing Database Migrations\n\nUse rollback fixtures for schema changes.",
        topics=[("testing", "pytest", 0.9), ("database", "migrations", 0.8)],
        url="https://example.com/blog/testing-migrations",
    )
    _blog(
        db,
        "TITLE: Product Launch Notes\n\nA checklist for launch announcements.",
        topics=[("launch", "", 0.9)],
        url="https://example.com/blog/launch-notes",
    )
    draft = tmp_path / "draft.md"
    draft.write_text(
        """---
title: Migration Test Playbook
topics:
  - testing
---

# Ignore this heading

Database migration rollback tests need fixtures and schema assertions.
""",
        encoding="utf-8",
    )

    report = build_blog_internal_link_suggestions(
        db,
        draft_path=draft,
        min_score=2.0,
        now=NOW,
    )
    suggestion = _suggestion_by_id(report)[target]

    assert suggestion["target_content_id"] == target
    assert suggestion["url"] == "https://example.com/blog/testing-migrations"
    assert suggestion["title"] == "Testing Database Migrations"
    assert suggestion["anchor_text"] == "Testing Database Migrations"
    assert suggestion["matched_topics"] == ["testing"]
    assert suggestion["score"] >= 4.0
    assert "topic match: testing" in suggestion["reason"]


def test_body_term_matches_can_suggest_without_topic_overlap(db, tmp_path):
    target = _blog(
        db,
        "TITLE: Release Guardrails\n\nRollback migration fixture checks catch schema drift before deploy.",
        topics=[("operations", "", 0.7)],
    )
    draft = tmp_path / "body-only.md"
    draft.write_text(
        "# Safer Deploys\n\nSchema migration rollback fixture checks reduce deploy drift.",
        encoding="utf-8",
    )

    report = build_blog_internal_link_suggestions(
        db,
        draft_path=draft,
        min_score=1.0,
        now=NOW,
    )
    suggestion = _suggestion_by_id(report)[target]

    assert suggestion["matched_topics"] == []
    assert {"schema", "migration", "rollback", "fixture"} <= set(
        suggestion["matched_terms"]
    )
    assert "shared terms" in suggestion["reason"]


def test_content_id_source_excludes_self_and_accepts_long_post(db):
    source = _blog(
        db,
        "TITLE: Testing Strategy\n\nDatabase migration rollback fixture tests.",
        topics=[("testing", "", 0.9)],
        published=False,
    )
    related = _blog(
        db,
        "TITLE: Migration Fixtures\n\nTesting rollback behavior for database migrations.",
        content_type="long_post",
        topics=[("testing", "", 0.9)],
    )

    report = build_blog_internal_link_suggestions(
        db,
        content_id=source,
        min_score=1.0,
        now=NOW,
    )

    assert [item["target_content_id"] for item in report["suggestions"]] == [related]
    assert source not in _suggestion_by_id(report)


def test_missing_urls_are_allowed(db, tmp_path):
    target = _blog(
        db,
        "TITLE: URL Missing\n\nTesting migration rollback fixtures.",
        topics=[("testing", "", 0.9)],
        url=None,
    )
    draft = tmp_path / "draft.md"
    draft.write_text(
        "---\ntitle: Draft\ntopics: testing\n---\n\nMigration rollback fixtures.",
        encoding="utf-8",
    )

    report = build_blog_internal_link_suggestions(db, draft_path=draft, now=NOW)
    suggestion = _suggestion_by_id(report)[target]

    assert suggestion["url"] is None
    assert suggestion["anchor_text"] == "URL Missing"


def test_content_publication_url_is_used_when_generated_url_is_missing(db, tmp_path):
    target = _blog(
        db,
        "TITLE: Publication URL\n\nTesting migration rollback fixtures.",
        topics=[("testing", "", 0.9)],
        url=None,
    )
    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, platform_url, published_at)
           VALUES (?, 'blog', 'published', ?, ?)""",
        (target, "https://example.com/blog/publication-url", NOW.isoformat()),
    )
    db.conn.commit()
    draft = tmp_path / "draft.md"
    draft.write_text(
        "---\ntitle: Draft\ntopics: testing\n---\n\nMigration rollback fixtures.",
        encoding="utf-8",
    )

    report = build_blog_internal_link_suggestions(db, draft_path=draft, now=NOW)

    assert _suggestion_by_id(report)[target]["url"] == "https://example.com/blog/publication-url"


def test_formatters_and_cli_text_json_output(db, tmp_path, capsys):
    _blog(
        db,
        "TITLE: CLI Target\n\nTesting migration rollback fixtures.",
        topics=[("testing", "", 0.9)],
    )
    draft = tmp_path / "cli.md"
    draft.write_text(
        "---\ntitle: CLI Draft\ntopics: testing\n---\n\nRollback fixtures.",
        encoding="utf-8",
    )
    report = build_blog_internal_link_suggestions(db, draft_path=draft, now=NOW)

    text = format_blog_internal_links_text(report)
    payload = json.loads(format_blog_internal_links_json(report))
    assert "Blog internal link suggestions" in text
    assert "CLI Target" in text
    assert payload["suggestions"][0]["title"] == "CLI Target"

    @contextmanager
    def fake_script_context():
        yield SimpleNamespace(), db

    with patch.object(suggest_blog_links_cli, "script_context", fake_script_context), patch.object(
        suggest_blog_links_cli,
        "build_blog_internal_link_suggestions",
        wraps=lambda db, **kwargs: build_blog_internal_link_suggestions(
            db,
            now=NOW,
            **kwargs,
        ),
    ):
        assert suggest_blog_links_cli.main(
            ["--draft", str(draft), "--limit", "5", "--min-score", "1", "--format", "json"]
        ) == 0

    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["limit"] == 5
    assert cli_payload["filters"]["min_score"] == 1.0
    assert cli_payload["suggestions"][0]["title"] == "CLI Target"
