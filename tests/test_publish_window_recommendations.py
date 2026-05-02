"""Tests for publish window recommendation reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from output.publish_window_recommendations import (
    build_publish_window_recommendation_report,
    format_publish_window_recommendations_json,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_window_recommendations.py"
spec = importlib.util.spec_from_file_location("publish_window_recommendations_script", SCRIPT_PATH)
publish_window_recommendations_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publish_window_recommendations_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, *, published: int = 0, created_at: datetime | None = None) -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published, created_at)
           VALUES ('Window test copy', 'x_post', 7.0, ?, ?)""",
        (published, (created_at or NOW - timedelta(days=10)).isoformat()),
    ).lastrowid


def _publish(
    db,
    *,
    at: datetime,
    platform: str = "x",
    status: str = "published",
    score: float = 10.0,
    likes: int = 0,
    replies: int = 0,
    reposts: int = 0,
    clicks: int = 0,
    content_id: int | None = None,
) -> int:
    content_id = content_id or _content(db, published=1)
    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, published_at, updated_at)
           VALUES (?, ?, ?, ?, ?)""",
        (content_id, platform, status, at.isoformat(), at.isoformat()),
    )
    if platform == "x":
        db.conn.execute(
            """INSERT INTO post_engagement
               (content_id, tweet_id, like_count, retweet_count, reply_count,
                quote_count, engagement_score, fetched_at)
               VALUES (?, ?, ?, ?, ?, 0, ?, ?)""",
            (
                content_id,
                f"tweet-{content_id}",
                likes,
                reposts,
                replies,
                score,
                (at + timedelta(hours=6)).isoformat(),
            ),
        )
    elif platform == "bluesky":
        db.conn.execute(
            """INSERT INTO bluesky_engagement
               (content_id, bluesky_uri, like_count, repost_count, reply_count,
                quote_count, engagement_score, fetched_at)
               VALUES (?, ?, ?, ?, ?, 0, ?, ?)""",
            (
                content_id,
                f"at://test/post/{content_id}",
                likes,
                reposts,
                replies,
                score,
                (at + timedelta(hours=6)).isoformat(),
            ),
        )
    if clicks:
        db.conn.execute(
            """INSERT INTO newsletter_link_clicks
               (issue_id, content_id, link_url, clicks, unique_clicks, fetched_at)
               VALUES ('issue-1', ?, ?, ?, ?, ?)""",
            (
                content_id,
                f"https://example.test/{content_id}",
                clicks + 2,
                clicks,
                (at + timedelta(hours=7)).isoformat(),
            ),
        )
    db.conn.commit()
    return content_id


def test_report_ranks_windows_by_normalized_engagement_with_counts_and_metrics(db):
    monday_10 = datetime(2026, 4, 27, 10, tzinfo=timezone.utc)
    friday_9 = datetime(2026, 4, 25, 9, tzinfo=timezone.utc)
    tuesday_14 = datetime(2026, 4, 28, 14, tzinfo=timezone.utc)

    for weeks_ago in range(3):
        _publish(
            db,
            at=monday_10 - timedelta(weeks=weeks_ago),
            platform="x",
            score=30.0,
            likes=10,
            replies=2,
            reposts=3,
            clicks=4,
        )
    for weeks_ago in range(3):
        _publish(
            db,
            at=friday_9 - timedelta(weeks=weeks_ago),
            platform="bluesky",
            score=20.0,
            likes=5,
            replies=1,
            reposts=2,
        )
    _publish(db, at=tuesday_14, score=100.0, likes=100)

    report = build_publish_window_recommendation_report(
        db,
        min_samples=3,
        limit=5,
        now=NOW,
    )
    payload = json.loads(format_publish_window_recommendations_json(report))

    assert payload["artifact_type"] == "publish_window_recommendations"
    assert report.totals["sample_count"] == 7
    assert report.totals["sparse_window_count"] == 1
    assert [item.day_name for item in report.recommendations] == ["Monday", "Saturday"]
    first = report.recommendations[0]
    assert first.weekday == 0
    assert first.hour == 10
    assert first.sample_count == 3
    assert first.confidence == "medium"
    assert first.total_likes == 30
    assert first.total_replies == 6
    assert first.total_reposts == 9
    assert first.total_clicks == 12
    assert first.normalized_engagement_score > report.recommendations[1].normalized_engagement_score


def test_sparse_data_confidence_can_be_reported_when_min_samples_allows_it(db):
    _publish(db, at=datetime(2026, 4, 28, 15, tzinfo=timezone.utc), score=40.0)

    report = build_publish_window_recommendation_report(
        db,
        min_samples=1,
        now=NOW,
    )

    assert len(report.recommendations) == 1
    assert report.recommendations[0].confidence == "low"
    assert report.recommendations[0].sample_count == 1
    assert report.recommendations[0].next_publish_at == "2026-05-05T15:00:00+00:00"


def test_failed_unpublished_and_missing_engagement_rows_do_not_influence_recommendations(db):
    eligible_at = datetime(2026, 4, 27, 8, tzinfo=timezone.utc)
    failed_at = datetime(2026, 4, 28, 16, tzinfo=timezone.utc)
    unpublished_at = datetime(2026, 4, 29, 18, tzinfo=timezone.utc)
    missing_engagement_at = datetime(2026, 4, 30, 20, tzinfo=timezone.utc)

    _publish(db, at=eligible_at, score=25.0)
    _publish(db, at=failed_at, status="failed", score=500.0)
    unpublished_id = _content(db, published=0)
    _publish(db, at=unpublished_at, status="queued", score=500.0, content_id=unpublished_id)
    missing_id = _content(db, published=1)
    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, published_at, updated_at)
           VALUES (?, 'x', 'published', ?, ?)""",
        (missing_id, missing_engagement_at.isoformat(), missing_engagement_at.isoformat()),
    )
    db.conn.commit()

    report = build_publish_window_recommendation_report(
        db,
        min_samples=1,
        now=NOW,
    )

    assert report.totals["sample_count"] == 1
    assert [(sample.weekday, sample.hour) for sample in report.samples] == [(0, 8)]
    assert report.recommendations[0].average_engagement_score == 25.0


def test_tie_breaking_prefers_more_samples_then_earlier_weekday_and_hour(db):
    monday_8 = datetime(2026, 4, 28, 8, tzinfo=timezone.utc)
    monday_9 = datetime(2026, 4, 28, 9, tzinfo=timezone.utc)
    friday_10 = datetime(2026, 4, 24, 10, tzinfo=timezone.utc)

    _publish(db, at=monday_8, score=12.0)
    _publish(db, at=monday_9, score=12.0)
    _publish(db, at=friday_10, score=12.0)
    _publish(db, at=friday_10 - timedelta(weeks=1), score=12.0)

    report = build_publish_window_recommendation_report(
        db,
        min_samples=1,
        limit=3,
        now=NOW,
    )

    assert [(item.weekday, item.hour, item.sample_count) for item in report.recommendations] == [
        (4, 10, 2),
        (1, 8, 1),
        (1, 9, 1),
    ]


def test_missing_schema_and_invalid_arguments_are_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_publish_window_recommendation_report(conn, now=NOW)

    assert report.missing_tables == ("content_publications",)
    assert report.recommendations == ()

    with pytest.raises(ValueError, match="days must be positive"):
        build_publish_window_recommendation_report(conn, days=0, now=NOW)
    with pytest.raises(ValueError, match="min_samples must be positive"):
        build_publish_window_recommendation_report(conn, min_samples=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_publish_window_recommendation_report(conn, limit=0, now=NOW)
    conn.close()


def test_cli_defaults_to_json_and_validates_arguments(db, monkeypatch, capsys):
    _publish(db, at=datetime(2026, 4, 27, 10, tzinfo=timezone.utc), score=10.0)

    monkeypatch.setattr(
        publish_window_recommendations_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        publish_window_recommendations_script,
        "build_publish_window_recommendation_report",
        lambda db, **kwargs: build_publish_window_recommendation_report(db, now=NOW, **kwargs),
    )

    assert publish_window_recommendations_script.main(["--days", "14", "--min-samples", "1", "--limit", "2"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["days"] == 14
    assert payload["filters"]["min_samples"] == 1
    assert payload["filters"]["limit"] == 2
    assert payload["recommendations"][0]["day_name"] == "Monday"

    assert publish_window_recommendations_script.main(["--min-samples", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
