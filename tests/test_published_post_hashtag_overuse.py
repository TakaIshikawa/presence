from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from evaluation.published_post_hashtag_overuse import (
    build_published_post_hashtag_overuse_report,
    build_published_post_hashtag_overuse_report_from_db,
    extract_hashtags,
    format_published_post_hashtag_overuse_json,
    format_published_post_hashtag_overuse_text,
)


NOW = datetime(2026, 5, 30, tzinfo=timezone.utc)


def test_extracts_hashtags_case_insensitively_without_url_fragments():
    assert extract_hashtags("Build #AI with #ai https://x.test/path#ignored") == ["#AI", "#ai"]
    report = build_published_post_hashtag_overuse_report([{"post_id": "p1", "text": "#AI #ai #Build"}], max_hashtags_per_post=2, now=NOW)
    issue = report["issues"][0]
    assert issue["canonical_hashtags"] == ["ai", "ai", "build"]
    assert issue["severity"] == "medium"
    assert issue["recommendation"]


def test_flags_repeated_sets_across_posts():
    rows = [
        {"post_id": "p1", "text": "One #AI #Launch"},
        {"post_id": "p2", "text": "Two #launch #ai"},
    ]
    report = build_published_post_hashtag_overuse_report(rows, repeated_set_threshold=2, min_diversity_ratio=0, now=NOW)
    repeated = [i for i in report["issues"] if i["issue_type"] == "repeated_hashtag_set"]
    assert {i["post_id"] for i in repeated} == {"p1", "p2"}
    assert "same set" in repeated[0]["evidence"]


def test_flags_low_diversity_window():
    rows = [{"post_id": f"p{i}", "text": f"Post {i} #AI #Build"} for i in range(4)]
    report = build_published_post_hashtag_overuse_report(rows, repeated_set_threshold=10, min_diversity_ratio=0.4, now=NOW)
    assert report["summary"]["diversity_ratio"] == 0.25
    assert {i["issue_type"] for i in report["issues"]} == {"low_diversity_window"}


def test_no_issue_case_and_formatters():
    report = build_published_post_hashtag_overuse_report([{"post_id": "p1", "text": "Plain post #One"}], now=NOW)
    assert report["issues"] == []
    assert "published_post_hashtag_overuse" in format_published_post_hashtag_overuse_json(report)
    assert "No published post" in format_published_post_hashtag_overuse_text(report)


def test_db_loads_published_posts():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE content_publications (id TEXT, content TEXT, status TEXT, published_at TEXT, platform TEXT)")
    conn.execute("INSERT INTO content_publications VALUES ('p1', '#a #b #c', 'published', '2026-05-29T00:00:00+00:00', 'x')")
    conn.execute("INSERT INTO content_publications VALUES ('p2', '#a #b #c', 'draft', '2026-05-29T00:00:00+00:00', 'x')")
    report = build_published_post_hashtag_overuse_report_from_db(conn, max_hashtags_per_post=2, now=NOW)
    assert report["summary"]["post_count"] == 1
    assert report["issues"][0]["post_id"] == "p1"
