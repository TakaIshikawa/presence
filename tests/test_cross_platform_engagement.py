"""Tests for cross-platform engagement normalization."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.cross_platform_engagement import (
    build_cross_platform_engagement_report,
    format_cross_platform_engagement_json,
    format_cross_platform_engagement_text,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "cross_platform_engagement.py"
)
spec = importlib.util.spec_from_file_location("cross_platform_engagement_script", SCRIPT_PATH)
cross_platform_engagement_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(cross_platform_engagement_script)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


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


def test_report_normalizes_all_supported_platforms(db):
    x_id = _content(db, "x_post")
    bluesky_id = _content(db, "x_post")
    linkedin_id = _content(db, "linkedin_post")
    mastodon_id = _content(db, "mastodon_post")

    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, like_count, retweet_count, reply_count,
            quote_count, engagement_score, fetched_at)
           VALUES (?, 'tweet-1', 2, 1, 0, 1, 10.0, ?)""",
        (x_id, "2026-04-30T12:00:00+00:00"),
    )
    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, like_count, retweet_count, reply_count,
            quote_count, engagement_score, fetched_at)
           VALUES (?, 'tweet-1', 1, 0, 0, 0, 1.0, ?)""",
        (x_id, "2026-04-20T12:00:00+00:00"),
    )
    db.conn.execute(
        """INSERT INTO bluesky_engagement
           (content_id, bluesky_uri, like_count, repost_count, reply_count,
            quote_count, engagement_score, fetched_at)
           VALUES (?, 'at://bsky/post/1', 4, 2, 1, 0, 14.0, ?)""",
        (bluesky_id, "2026-04-29T12:00:00+00:00"),
    )
    db.conn.execute(
        """INSERT INTO linkedin_engagement
           (content_id, linkedin_url, post_id, impression_count, like_count,
            comment_count, share_count, engagement_score, fetched_at)
           VALUES (?, 'https://linkedin.example/post/1', 'li-1', 100, 5,
                   1, 1, 13.0, ?)""",
        (linkedin_id, "2026-04-28T12:00:00+00:00"),
    )
    db.conn.execute(
        """INSERT INTO mastodon_engagement
           (content_id, mastodon_url, post_id, favourite_count, boost_count,
            reply_count, engagement_score, fetched_at, raw_metrics)
           VALUES (?, 'https://mastodon.example/@me/1', 'm-1', 6, 2,
                   1, 16.0, ?, '{}')""",
        (mastodon_id, "2026-04-27T12:00:00+00:00"),
    )
    send_id = db.conn.execute(
        """INSERT INTO newsletter_sends
           (issue_id, subject, source_content_ids, subscriber_count, sent_at)
           VALUES ('issue-1', 'Newsletter', '[]', 100, ?)""",
        ("2026-04-26T12:00:00+00:00",),
    ).lastrowid
    db.conn.execute(
        """INSERT INTO newsletter_engagement
           (newsletter_send_id, issue_id, opens, clicks, unsubscribes, fetched_at)
           VALUES (?, 'issue-1', 40, 5, 1, ?)""",
        (send_id, "2026-04-26T13:00:00+00:00"),
    )
    db.conn.commit()

    report = build_cross_platform_engagement_report(db, days=10, now=NOW)

    assert [row.platform for row in report.rows] == [
        "bluesky",
        "linkedin",
        "mastodon",
        "newsletter",
        "x",
    ]
    assert {row.platform: row.normalized_score for row in report.rows} == {
        "x": 100.0,
        "bluesky": 100.0,
        "linkedin": 100.0,
        "mastodon": 100.0,
        "newsletter": 100.0,
    }
    newsletter = next(row for row in report.rows if row.platform == "newsletter")
    assert newsletter.content_id is None
    assert newsletter.issue_id == "issue-1"
    assert newsletter.raw_metrics == {
        "opens": 40,
        "clicks": 5,
        "unsubscribes": 1,
        "subscriber_count": 100,
    }
    assert newsletter.raw_total == 55.0
    assert report.platform_averages[0].platform == "bluesky"
    assert report.top_rows[0].platform == "x"
    assert report.top_rows[0].freshness_adjusted_score == 95.0


def test_json_output_is_deterministic_and_contains_required_fields(db):
    content_id = _content(db)
    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, like_count, retweet_count, reply_count,
            quote_count, engagement_score, fetched_at)
           VALUES (?, 'tweet-1', 3, 0, 0, 0, 3.0, ?)""",
        (content_id, "2026-05-01T12:00:00+00:00"),
    )
    db.conn.commit()

    report = build_cross_platform_engagement_report(
        db,
        days=2,
        platform="x",
        now=NOW,
    )
    first = format_cross_platform_engagement_json(report)
    second = format_cross_platform_engagement_json(report)

    assert first == second
    payload = json.loads(first)
    assert payload["rows"] == [
        {
            "content_id": content_id,
            "fetched_at": "2026-05-01T12:00:00+00:00",
            "freshness_adjusted_score": 100.0,
            "issue_id": None,
            "normalized_score": 100.0,
            "platform": "x",
            "raw_metrics": {
                "like_count": 3,
                "quote_count": 0,
                "reply_count": 0,
                "retweet_count": 0,
            },
            "raw_total": 3.0,
        }
    ]


def test_text_output_summarizes_averages_and_top_content(db):
    first_id = _content(db)
    second_id = _content(db)
    for content_id, score, fetched_at in (
        (first_id, 10.0, "2026-05-01T12:00:00+00:00"),
        (second_id, 5.0, "2026-04-30T12:00:00+00:00"),
    ):
        db.conn.execute(
            """INSERT INTO post_engagement
               (content_id, tweet_id, like_count, retweet_count, reply_count,
                quote_count, engagement_score, fetched_at)
               VALUES (?, ?, 0, 0, 0, 0, ?, ?)""",
            (content_id, f"tweet-{content_id}", score, fetched_at),
        )
    db.conn.commit()

    report = build_cross_platform_engagement_report(
        db,
        days=10,
        platform="x",
        limit=1,
        now=NOW,
    )
    text = format_cross_platform_engagement_text(report)

    assert "Platform averages:" in text
    assert "- x: rows=2 raw_avg=7.50 normalized_avg=75.00" in text
    assert "Top content:" in text
    assert f"content_id={first_id}: raw=10.00 normalized=100.00" in text
    assert "Bottom content:" in text


def test_empty_database_returns_clear_empty_report(db):
    report = build_cross_platform_engagement_report(db, days=7, now=NOW)

    assert report.rows == ()
    assert report.top_rows == ()
    assert report.bottom_rows == ()
    assert "No engagement snapshots found" in format_cross_platform_engagement_text(report)


def test_missing_tables_are_reported_without_exceptions():
    conn = sqlite3.connect(":memory:")
    try:
        report = build_cross_platform_engagement_report(conn, days=7, now=NOW)
    finally:
        conn.close()

    assert report.rows == ()
    assert report.missing_tables == (
        "bluesky_engagement",
        "linkedin_engagement",
        "mastodon_engagement",
        "newsletter_engagement",
        "post_engagement",
    )


def test_cli_outputs_json(db, capsys):
    content_id = _content(db)
    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, like_count, retweet_count, reply_count,
            quote_count, engagement_score, fetched_at)
           VALUES (?, 'tweet-1', 3, 0, 0, 0, 3.0, ?)""",
        (content_id, "2026-05-01T12:00:00+00:00"),
    )
    db.conn.commit()

    with patch.object(
        cross_platform_engagement_script,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        cross_platform_engagement_script,
        "build_cross_platform_engagement_report",
        wraps=lambda db, **kwargs: build_cross_platform_engagement_report(
            db,
            now=NOW,
            **kwargs,
        ),
    ):
        assert cross_platform_engagement_script.main(
            ["--platform", "x", "--days", "2", "--json"]
        ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["rows"][0]["platform"] == "x"
    assert payload["rows"][0]["content_id"] == content_id
