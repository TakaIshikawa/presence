"""Tests for blog publication metadata gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from output.blog_publication_metadata_gaps import (
    GAP_MISSING_PUBLISHED_AT,
    GAP_MISSING_PUBLISHED_URL,
    GAP_MISSING_TITLE,
    build_blog_publication_metadata_gap_report,
    format_blog_publication_metadata_gaps_json,
    format_blog_publication_metadata_gaps_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_publication_metadata_gaps.py"
spec = importlib.util.spec_from_file_location("blog_publication_metadata_gaps_script", SCRIPT_PATH)
blog_publication_metadata_gaps_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(blog_publication_metadata_gaps_script)

NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_blog(db, content: str) -> int:
    return db.insert_generated_content(
        content_type="blog_post",
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="ok",
    )


def _publish(
    db,
    content_id: int,
    *,
    published_url: str | None = "https://example.com/blog/post",
    published_at: str | None = "2026-05-01T12:00:00+00:00",
) -> None:
    db.conn.execute(
        """UPDATE generated_content
           SET published = 1, published_url = ?, published_at = ?
           WHERE id = ?""",
        (published_url, published_at, content_id),
    )
    db.conn.commit()


def test_published_blog_post_missing_published_url_is_flagged(db):
    content_id = _insert_blog(db, "# Launch Notes\n\nBody.")
    _publish(db, content_id, published_url=None)

    report = build_blog_publication_metadata_gap_report(db, now=NOW)

    assert report.totals == {"gaps_found": 1, "posts_checked": 1}
    assert report.gaps[0].gap_type == GAP_MISSING_PUBLISHED_URL
    assert report.gaps[0].content_id == content_id


def test_published_blog_post_missing_published_at_is_flagged(db):
    content_id = _insert_blog(db, "# Launch Notes\n\nBody.")
    _publish(db, content_id, published_at=None)

    report = build_blog_publication_metadata_gap_report(db, now=NOW)

    assert [gap.gap_type for gap in report.gaps] == [GAP_MISSING_PUBLISHED_AT]
    assert report.gaps[0].content_id == content_id


def test_blog_content_without_frontmatter_title_or_h1_is_flagged(db):
    content_id = _insert_blog(db, "Intro paragraph without a heading.")
    _publish(db, content_id)

    report = build_blog_publication_metadata_gap_report(db, now=NOW)

    assert [gap.gap_type for gap in report.gaps] == [GAP_MISSING_TITLE]
    assert report.gaps[0].title_source is None
    assert report.gaps[0].content_id == content_id


def test_frontmatter_title_and_h1_satisfy_title_requirement(db):
    frontmatter_id = _insert_blog(
        db,
        """---
title: "Frontmatter Title"
---

Body.
""",
    )
    h1_id = _insert_blog(db, "# Markdown Title\n\nBody.")
    _publish(db, frontmatter_id)
    _publish(db, h1_id, published_url="https://example.com/blog/h1")

    report = build_blog_publication_metadata_gap_report(db, now=NOW)

    assert report.gaps == ()
    assert report.totals == {"gaps_found": 0, "posts_checked": 2}


def test_unpublished_and_non_blog_content_are_ignored(db):
    unpublished = _insert_blog(db, "No title or publication metadata.")
    other = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="No title",
        eval_score=8.0,
        eval_feedback="ok",
    )
    _publish(db, other, published_url=None, published_at=None)

    report = build_blog_publication_metadata_gap_report(db, now=NOW)

    assert report.gaps == ()
    assert report.totals == {"gaps_found": 0, "posts_checked": 0}
    assert unpublished


def test_json_and_text_formatting_are_stable(db):
    content_id = _insert_blog(db, "No heading here.")
    _publish(db, content_id, published_url=None, published_at=None)

    report = build_blog_publication_metadata_gap_report(db, days=14, now=NOW)
    payload = json.loads(format_blog_publication_metadata_gaps_json(report))
    text = format_blog_publication_metadata_gaps_text(report)

    assert payload["artifact_type"] == "blog_publication_metadata_gaps"
    assert payload["filters"]["days"] == 14
    assert payload["totals"] == {"gaps_found": 3, "posts_checked": 1}
    assert {
        GAP_MISSING_PUBLISHED_AT,
        GAP_MISSING_PUBLISHED_URL,
        GAP_MISSING_TITLE,
    } == {gap["gap_type"] for gap in payload["gaps"]}
    assert "Blog Publication Metadata Gaps" in text
    assert f"content_id={content_id}" in text


def test_cli_supports_json_and_fail_on_issues(db, monkeypatch, capsys):
    content_id = _insert_blog(db, "No heading here.")
    _publish(db, content_id, published_url=None)
    monkeypatch.setattr(
        blog_publication_metadata_gaps_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = blog_publication_metadata_gaps_script.main(
        ["--days", "7", "--format", "json", "--fail-on-issues"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["filters"]["days"] == 7
    assert payload["totals"]["gaps_found"] == 2
