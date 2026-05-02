"""Tests for cross-platform engagement freshness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.cross_platform_engagement_freshness import (
    build_cross_platform_engagement_freshness_report,
    format_cross_platform_engagement_freshness_json,
    format_cross_platform_engagement_freshness_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "cross_platform_engagement_freshness.py"
)
spec = importlib.util.spec_from_file_location(
    "cross_platform_engagement_freshness_script",
    SCRIPT_PATH,
)
cross_platform_engagement_freshness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(cross_platform_engagement_freshness_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, content_type: str = "x_post") -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, ?, 7.0, 1)""",
        (f"{content_type} content", content_type),
    ).lastrowid


def _publication(
    db,
    *,
    platform: str,
    published_at: str = "2026-05-01T12:00:00+00:00",
) -> int:
    content_id = _content(db, f"{platform}_post")
    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, platform_post_id, published_at)
           VALUES (?, ?, 'published', ?, ?)""",
        (content_id, platform, f"{platform}-{content_id}", published_at),
    )
    db.conn.commit()
    return content_id


def _insert_metric(db, *, table: str, content_id: int, fetched_at: str) -> None:
    if table == "post_engagement":
        db.conn.execute(
            """INSERT INTO post_engagement
               (content_id, tweet_id, like_count, retweet_count, reply_count,
                quote_count, engagement_score, fetched_at)
               VALUES (?, 'tweet-1', 1, 1, 1, 0, 3.0, ?)""",
            (content_id, fetched_at),
        )
    elif table == "bluesky_engagement":
        db.conn.execute(
            """INSERT INTO bluesky_engagement
               (content_id, bluesky_uri, like_count, repost_count, reply_count,
                quote_count, engagement_score, fetched_at)
               VALUES (?, 'at://bsky/post/1', 1, 1, 1, 0, 3.0, ?)""",
            (content_id, fetched_at),
        )
    elif table == "linkedin_engagement":
        db.conn.execute(
            """INSERT INTO linkedin_engagement
               (content_id, linkedin_url, post_id, impression_count, like_count,
                comment_count, share_count, engagement_score, fetched_at)
               VALUES (?, 'https://linkedin.example/post/1', 'li-1', 10, 1,
                       1, 1, 3.0, ?)""",
            (content_id, fetched_at),
        )
    elif table == "mastodon_engagement":
        db.conn.execute(
            """INSERT INTO mastodon_engagement
               (content_id, mastodon_url, post_id, favourite_count, boost_count,
                reply_count, engagement_score, fetched_at, raw_metrics)
               VALUES (?, 'https://mastodon.example/@me/1', 'm-1', 1, 1,
                       1, 3.0, ?, '{}')""",
            (content_id, fetched_at),
        )
    elif table == "newsletter_link_clicks":
        db.conn.execute(
            """INSERT INTO newsletter_link_clicks
               (issue_id, content_id, link_url, clicks, fetched_at)
               VALUES ('issue-1', ?, 'https://example.com/item', 3, ?)""",
            (content_id, fetched_at),
        )
    else:
        raise AssertionError(f"unsupported table: {table}")
    db.conn.commit()


def test_report_maps_all_supported_platforms_to_latest_metric_tables(db):
    platform_tables = {
        "x": "post_engagement",
        "bluesky": "bluesky_engagement",
        "linkedin": "linkedin_engagement",
        "mastodon": "mastodon_engagement",
        "newsletter": "newsletter_link_clicks",
    }
    content_ids = {
        platform: _publication(db, platform=platform)
        for platform in platform_tables
    }
    for platform, table in platform_tables.items():
        _insert_metric(
            db,
            table=table,
            content_id=content_ids[platform],
            fetched_at="2026-05-02T06:00:00+00:00",
        )

    report = build_cross_platform_engagement_freshness_report(
        db,
        days=7,
        max_age_hours=24,
        now=NOW,
    )

    assert report.missing_optional_tables == ()
    assert report.totals["by_status"] == {
        "fresh": 5,
        "missing_metrics": 0,
        "stale": 0,
    }
    assert {item.platform for item in report.items} == set(platform_tables)
    for item in report.items:
        assert item.content_id == content_ids[item.platform]
        assert item.latest_metric_at == "2026-05-02T06:00:00+00:00"
        assert item.age_hours == 6.0
        assert report.totals["by_platform"][item.platform]["fresh"] == 1


def test_missing_metric_rows_are_classified_missing_metrics(db):
    content_id = _publication(db, platform="x")

    report = build_cross_platform_engagement_freshness_report(
        db,
        platform="x",
        now=NOW,
    )

    assert report.items[0].content_id == content_id
    assert report.items[0].status == "missing_metrics"
    assert report.items[0].reason == "no_matching_metric_rows"
    assert report.totals["by_status"]["missing_metrics"] == 1


def test_missing_optional_metric_tables_are_reported_without_raising():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY)")
    conn.execute(
        """CREATE TABLE content_publications (
            content_id INTEGER,
            platform TEXT,
            status TEXT,
            published_at TEXT
        )"""
    )
    conn.execute("INSERT INTO generated_content (id) VALUES (42)")
    conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, published_at)
           VALUES (42, 'linkedin', 'published', '2026-05-01T12:00:00+00:00')"""
    )
    conn.commit()

    report = build_cross_platform_engagement_freshness_report(
        conn,
        platform="linkedin",
        now=NOW,
    )

    assert report.missing_optional_tables == ("linkedin_engagement",)
    assert report.items[0].status == "missing_metrics"
    assert report.items[0].reason == "missing_metric_table"
    assert "Missing optional metric tables: linkedin_engagement" in (
        format_cross_platform_engagement_freshness_text(report)
    )


def test_stale_threshold_and_platform_filter_are_applied(db):
    stale_id = _publication(db, platform="x")
    fresh_id = _publication(db, platform="bluesky")
    _insert_metric(
        db,
        table="post_engagement",
        content_id=stale_id,
        fetched_at="2026-05-01T05:00:00+00:00",
    )
    _insert_metric(
        db,
        table="bluesky_engagement",
        content_id=fresh_id,
        fetched_at="2026-05-02T08:00:00+00:00",
    )

    x_report = build_cross_platform_engagement_freshness_report(
        db,
        platform="x",
        max_age_hours=24,
        now=NOW,
    )
    all_report = build_cross_platform_engagement_freshness_report(
        db,
        max_age_hours=36,
        now=NOW,
    )

    assert [item.platform for item in x_report.items] == ["x"]
    assert x_report.items[0].status == "stale"
    assert x_report.items[0].age_hours == 31.0
    assert {item.platform: item.status for item in all_report.items}["x"] == "fresh"


def test_json_is_sorted_and_cli_supports_format_flags(db, monkeypatch, capsys):
    content_id = _publication(db, platform="newsletter")
    _insert_metric(
        db,
        table="newsletter_link_clicks",
        content_id=content_id,
        fetched_at="2026-05-02T11:00:00+00:00",
    )
    report = build_cross_platform_engagement_freshness_report(
        db,
        platform="newsletter",
        now=NOW,
    )
    payload = json.loads(format_cross_platform_engagement_freshness_json(report))
    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "cross_platform_engagement_freshness"

    monkeypatch.setattr(
        cross_platform_engagement_freshness_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        cross_platform_engagement_freshness_script,
        "build_cross_platform_engagement_freshness_report",
        lambda db, **kwargs: build_cross_platform_engagement_freshness_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    assert cross_platform_engagement_freshness_script.main(
        [
            "--platform",
            "newsletter",
            "--days",
            "7",
            "--max-age-hours",
            "2",
            "--format",
            "json",
        ]
    ) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["items"][0]["content_id"] == content_id

    assert cross_platform_engagement_freshness_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_invalid_builder_args_raise(db):
    with pytest.raises(ValueError, match="max_age_hours must be positive"):
        build_cross_platform_engagement_freshness_report(db, max_age_hours=0)
