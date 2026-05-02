"""Tests for blog excerpt duplication reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

from output.blog_excerpt_duplication import (
    BlogPostRecord,
    build_blog_excerpt_duplication_report,
    format_blog_excerpt_duplication_json,
    format_blog_excerpt_duplication_text,
    normalize_excerpt_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_excerpt_duplication.py"
spec = importlib.util.spec_from_file_location("blog_excerpt_duplication_script", SCRIPT_PATH)
blog_excerpt_duplication_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(blog_excerpt_duplication_script)


def test_exact_duplicates_normalize_case_whitespace_and_simple_punctuation():
    report = build_blog_excerpt_duplication_report(
        [
            BlogPostRecord(
                slug="first",
                title="First",
                excerpt="Static sites feel stale when the intro repeats.",
            ),
            BlogPostRecord(
                slug="second",
                title="Second",
                excerpt="  STATIC sites feel stale, when the intro repeats! ",
            ),
            BlogPostRecord(
                slug="third",
                title="Third",
                excerpt="A different opening points readers somewhere else.",
            ),
        ],
    )
    payload = json.loads(format_blog_excerpt_duplication_json(report))

    assert normalize_excerpt_text("Hello,   WORLD!") == "hello world"
    assert payload["cluster_count"] == 1
    assert payload["clusters"][0]["slugs"] == ["first", "second"]
    assert payload["clusters"][0]["titles"] == ["First", "Second"]
    assert payload["clusters"][0]["similarity_scores"] == [
        {"left_slug": "first", "right_slug": "second", "score": 1.0}
    ]


def test_near_duplicate_clusters_include_scores_and_representative_text():
    report = build_blog_excerpt_duplication_report(
        [
            {
                "slug": "alpha",
                "title": "Alpha",
                "excerpt": "Treat the launch checklist as a living system before you ship.",
            },
            {
                "slug": "beta",
                "title": "Beta",
                "excerpt": "Treat your launch checklist like a living system before shipping.",
            },
            {
                "slug": "gamma",
                "title": "Gamma",
                "excerpt": "This post is about evergreen internal link planning.",
            },
        ],
        similarity_threshold=0.72,
    )
    cluster = report.clusters[0]

    assert cluster.slugs == ("alpha", "beta")
    assert cluster.representative_text == (
        "Treat the launch checklist as a living system before you ship."
    )
    assert cluster.similarity_scores[0].score >= 0.72
    assert cluster.posts[0].normalized_text.startswith("treat the launch checklist")


def test_distinct_excerpts_do_not_create_clusters():
    report = build_blog_excerpt_duplication_report(
        [
            BlogPostRecord(slug="one", title="One", excerpt="A post about canonical URL audits."),
            BlogPostRecord(slug="two", title="Two", excerpt="A guide to better pull quote exports."),
            BlogPostRecord(slug="three", title="Three", excerpt="A note on visual opportunity scoring."),
        ],
        similarity_threshold=0.9,
    )
    text = format_blog_excerpt_duplication_text(report)

    assert report.cluster_count == 0
    assert "No duplicate excerpt clusters found." in text


def test_missing_excerpt_falls_back_to_summary():
    report = build_blog_excerpt_duplication_report(
        [
            BlogPostRecord(
                slug="one",
                title="One",
                summary="Use summaries when no excerpt exists for the generated post.",
            ),
            BlogPostRecord(
                slug="two",
                title="Two",
                summary="Use summaries when no excerpt exists for the generated post!",
            ),
        ],
    )

    assert report.cluster_count == 1
    assert [post.text_source for post in report.clusters[0].posts] == ["summary", "summary"]


def test_cli_reads_markdown_files_limits_lookback_and_emits_json(tmp_path, capsys):
    recent_a = tmp_path / "recent-a.md"
    recent_a.write_text(
        """---
title: Recent A
slug: recent-a
date: 2026-05-01
excerpt: Repeated intros make the generated blog archive feel stale.
---
Body
"""
    )
    recent_b = tmp_path / "recent-b.md"
    recent_b.write_text(
        """---
title: Recent B
slug: recent-b
date: 2026-04-30
summary: Repeated intros make the generated blog archive feel stale!
---
Body
"""
    )
    old = tmp_path / "old.md"
    old.write_text(
        """---
title: Old
slug: old
date: 2025-01-01
excerpt: Repeated intros make the generated blog archive feel stale.
---
Body
"""
    )

    exit_code = blog_excerpt_duplication_script.main(
        [
            str(recent_a),
            str(recent_b),
            str(old),
            "--lookback-days",
            "10",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["total_posts"] == 2
    assert payload["clusters"][0]["slugs"] == ["recent-b", "recent-a"]
    assert [post["text_source"] for post in payload["clusters"][0]["posts"]] == [
        "summary",
        "excerpt",
    ]


def test_analyzer_lookback_uses_injected_now():
    report = build_blog_excerpt_duplication_report(
        [
            BlogPostRecord(
                slug="recent",
                title="Recent",
                excerpt="Same preview",
                published_at="2026-05-01",
            ),
            BlogPostRecord(
                slug="old",
                title="Old",
                excerpt="Same preview",
                published_at="2026-01-01",
            ),
        ],
        lookback_days=7,
        now=datetime(2026, 5, 3, tzinfo=timezone.utc),
    )

    assert report.total_posts == 1
    assert report.cluster_count == 0
