"""Tests for blog draft source coverage reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from output.blog_source_coverage import (
    BlogDraftRecord,
    BlogSourceLinkRecord,
    build_blog_source_coverage_report,
    format_blog_source_coverage_json,
    format_blog_source_coverage_markdown,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_source_coverage.py"
spec = importlib.util.spec_from_file_location("blog_source_coverage_script", SCRIPT_PATH)
blog_source_coverage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(blog_source_coverage_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    content: str = "# Draft title\n\nBody with evidence.",
    content_type: str = "blog_post",
    source_commits: list[str] | None = None,
    source_messages: list[str] | None = None,
    source_activity_ids: list[str] | None = None,
) -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=source_commits or [],
        source_messages=source_messages or [],
        source_activity_ids=source_activity_ids or [],
        content=content,
        eval_score=8.0,
        eval_feedback="usable",
    )


def _knowledge(db, source_type: str, source_id: str) -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge (source_type, source_id, content, insight, approved)
           VALUES (?, ?, ?, ?, 1)""",
        (source_type, source_id, f"Knowledge item {source_id}", "Relevant evidence"),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_drafts_are_summarized_with_total_and_type_counts_from_records():
    report = build_blog_source_coverage_report(
        drafts=[
            BlogDraftRecord(draft_id=2, title="Enough evidence"),
            BlogDraftRecord(draft_id=1, title="Needs more"),
        ],
        source_links=[
            BlogSourceLinkRecord(2, "commit", "abc123"),
            BlogSourceLinkRecord(2, "claude_session", "session-1"),
            BlogSourceLinkRecord(2, "curated_article", "article-1"),
            BlogSourceLinkRecord(2, "commit", "abc123"),
            BlogSourceLinkRecord(1, "commit", "def456"),
        ],
        min_sources=3,
        now=NOW,
    )

    first, second = report.drafts
    assert first.draft_id == 1
    assert first.total_source_count == 1
    assert first.source_counts_by_type == {"commit": 1}
    assert second.draft_id == 2
    assert second.total_source_count == 3
    assert second.source_counts_by_type == {
        "claude_session": 1,
        "commit": 1,
        "curated_article": 1,
    }
    assert report.counts == {
        "drafts": 2,
        "passing_drafts": 1,
        "warning_drafts": 1,
        "total_sources": 4,
    }


def test_drafts_below_minimum_get_actionable_missing_source_hints():
    report = build_blog_source_coverage_report(
        drafts=[{"draft_id": 10, "title": "Thin draft"}],
        source_links=[{"draft_id": 10, "source_type": "commit", "source_id": "abc123"}],
        min_sources=3,
        now=NOW,
    )
    draft = report.drafts[0]

    assert draft.ok is False
    assert draft.warnings == ("needs 2 more source artifacts", "needs 1 more source type")
    assert any("Attach source commits" in hint for hint in draft.missing_source_hints)
    assert any("curated_article" in hint for hint in draft.missing_source_hints)


def test_drafts_with_sufficient_varied_sources_pass_without_warnings():
    report = build_blog_source_coverage_report(
        drafts=[{"draft_id": 7, "title": "Grounded draft"}],
        source_links=[
            {"draft_id": 7, "source_type": "commit", "source_id": "abc123"},
            {"draft_id": 7, "source_type": "claude_message", "source_id": "uuid-1"},
            {"draft_id": 7, "source_type": "published_post", "source_id": "post-1"},
        ],
        min_sources=3,
        now=NOW,
    )
    draft = report.drafts[0]

    assert draft.ok is True
    assert draft.warnings == ()
    assert draft.missing_source_hints == ()
    assert draft.source_counts_by_type == {
        "claude_session": 1,
        "commit": 1,
        "published_post": 1,
    }


def test_database_loader_counts_generated_and_knowledge_sources(db):
    blog_id = _content(
        db,
        source_commits=["abc123"],
        source_messages=["uuid-1"],
    )
    article_id = _knowledge(db, "curated_article", "article-1")
    post_id = _knowledge(db, "own_post", "post-1")
    db.insert_content_knowledge_links(blog_id, [(article_id, 0.9), (post_id, 0.8)])

    report = build_blog_source_coverage_report(db, min_sources=4, now=NOW)
    draft = report.drafts[0]

    assert draft.draft_id == blog_id
    assert draft.ok is True
    assert draft.total_source_count == 4
    assert draft.source_counts_by_type == {
        "claude_session": 1,
        "commit": 1,
        "curated_article": 1,
        "published_post": 1,
    }


def test_blog_variant_is_included_as_a_draft(db):
    content_id = _content(db, content_type="x_post", source_commits=["abc123"])
    db.upsert_content_variant(
        content_id,
        platform="blog",
        variant_type="post",
        content="Expanded blog version.",
    )

    report = build_blog_source_coverage_report(db, min_sources=1, min_source_types=1, now=NOW)

    assert [draft.draft_id for draft in report.drafts] == [content_id]


def test_json_and_markdown_output_are_deterministic():
    report = build_blog_source_coverage_report(
        drafts=[{"draft_id": 3, "title": "Grounded"}],
        source_links=[
            {"draft_id": 3, "source_type": "curated_newsletter", "source_id": "news"},
            {"draft_id": 3, "source_type": "commit", "source_id": "abc"},
            {"draft_id": 3, "source_type": "message", "source_id": "uuid"},
        ],
        min_sources=3,
        now=NOW,
    )

    payload = json.loads(format_blog_source_coverage_json(report))
    markdown = format_blog_source_coverage_markdown(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "blog_source_coverage"
    assert payload["drafts"][0]["source_counts_by_type"] == {
        "claude_session": 1,
        "commit": 1,
        "curated_article": 1,
    }
    assert markdown.startswith("# Blog Source Coverage")
    assert "counts: claude_session=1, commit=1, curated_article=1" in markdown


def test_missing_generated_content_table_returns_empty_report():
    import sqlite3

    conn = sqlite3.connect(":memory:")

    report = build_blog_source_coverage_report(conn, now=NOW)

    assert report.drafts == ()
    assert report.missing_tables == ("generated_content",)
    assert report.counts["drafts"] == 0


def test_invalid_minimums_are_rejected():
    with pytest.raises(ValueError, match="min_sources"):
        build_blog_source_coverage_report(drafts=[], min_sources=-1, now=NOW)
    with pytest.raises(ValueError, match="min_source_types"):
        build_blog_source_coverage_report(drafts=[], min_source_types=-1, now=NOW)


def test_cli_supports_json_markdown_and_min_sources(db, capsys, monkeypatch):
    _content(db, source_commits=["abc123"], source_messages=["uuid-1"])
    monkeypatch.setattr(blog_source_coverage_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        blog_source_coverage_script,
        "build_blog_source_coverage_report",
        lambda db, **kwargs: build_blog_source_coverage_report(db, now=NOW, **kwargs),
    )

    assert blog_source_coverage_script.main(["--min-sources", "-1"]) == 2
    assert "value must be non-negative" in capsys.readouterr().err

    assert blog_source_coverage_script.main(["--min-sources", "2", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["min_sources"] == 2
    assert payload["counts"]["passing_drafts"] == 1

    assert blog_source_coverage_script.main(["--min-sources", "3", "--format", "markdown"]) == 1
    markdown = capsys.readouterr().out
    assert "# Blog Source Coverage" in markdown
    assert "needs 1 more source artifact" in markdown
