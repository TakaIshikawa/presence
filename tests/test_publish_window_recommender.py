"""Tests for concrete publish window recommendations."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from evaluation.publish_window_recommender import (
    PublishWindowRecommender,
    recommendations_to_dicts,
)
from storage.db import Database


def _insert_content(db: Database, content_type: str = "x_post") -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=["sha"],
        source_messages=["uuid"],
        content=f"Test {content_type}",
        eval_score=8.0,
        eval_feedback="Good",
    )


def _past_bucket(now: datetime, weekday: int, hour: int, weeks_ago: int = 1) -> datetime:
    days_back = (now.weekday() - weekday) % 7
    if days_back == 0 and now.hour <= hour:
        days_back = 7
    return (now - timedelta(days=days_back, weeks=weeks_ago - 1)).replace(
        hour=hour,
        minute=0,
        second=0,
        microsecond=0,
    )


def _set_x_post(
    db: Database,
    published_at: datetime,
    score: float,
    *,
    content_type: str = "x_post",
) -> int:
    content_id = _insert_content(db, content_type=content_type)
    tweet_id = f"tweet-{content_id}"
    db.mark_published(content_id, f"https://x.com/test/{content_id}", tweet_id=tweet_id)
    db.conn.execute(
        "UPDATE generated_content SET published_at = ? WHERE id = ?",
        (published_at.isoformat(), content_id),
    )
    db.conn.execute(
        """UPDATE content_publications SET published_at = ?
           WHERE content_id = ? AND platform = 'x'""",
        (published_at.isoformat(), content_id),
    )
    db.conn.commit()
    db.insert_engagement(content_id, tweet_id, 1, 0, 0, 0, score)
    return content_id


def _set_bluesky_post(db: Database, published_at: datetime, score: float) -> int:
    content_id = _insert_content(db)
    uri = f"at://test/post/{content_id}"
    db.mark_published_bluesky(content_id, uri)
    db.conn.execute(
        """UPDATE content_publications SET published_at = ?
           WHERE content_id = ? AND platform = 'bluesky'""",
        (published_at.isoformat(), content_id),
    )
    db.conn.commit()
    db.insert_bluesky_engagement(content_id, uri, 1, 0, 0, 0, score)
    return content_id


def test_recommendations_include_required_fields_reasons_and_json(db):
    now = datetime.now(timezone.utc).replace(minute=15, second=0, microsecond=0)
    start = now + timedelta(days=1)
    historical = _past_bucket(now, start.weekday(), 10)
    _set_x_post(db, historical, 32.0)

    recommendations = PublishWindowRecommender(db, daily_limits={"x": 3}).recommend(
        platform="x",
        days=3,
        limit=1,
        now=now,
    )

    assert len(recommendations) == 1
    recommendation = recommendations[0]
    assert recommendation.platform == "x"
    assert recommendation.start_time.weekday() == start.weekday()
    assert recommendation.start_time.hour == 10
    assert recommendation.score > 0
    assert recommendation.available is True
    assert any("Historical x engagement" in reason for reason in recommendation.reasons)
    assert any("Daily cap pressure" in reason for reason in recommendation.reasons)

    data = json.loads(json.dumps(recommendations_to_dicts(recommendations)))
    assert data[0]["platform"] == "x"
    assert data[0]["start_time"] == recommendation.start_time.isoformat()
    assert data[0]["historical_signal"]["sample_size"] == 1
    assert data[0]["cap_pressure"]["limit"] == 3
    assert isinstance(data[0]["reasons"], list)


def test_daily_cap_pressure_marks_full_day_unavailable_and_ranks_lower(db):
    now = datetime.now(timezone.utc).replace(hour=8, minute=0, second=0, microsecond=0)
    full_day = now + timedelta(days=1)
    open_day = now + timedelta(days=2)
    _set_x_post(db, _past_bucket(now, full_day.weekday(), 10), 80.0)
    _set_x_post(db, _past_bucket(now, open_day.weekday(), 11), 20.0)

    queued_id = _insert_content(db)
    queued_at = full_day.replace(hour=9, minute=0, second=0, microsecond=0)
    db.queue_for_publishing(queued_id, queued_at.isoformat(), platform="x")

    recommendations = PublishWindowRecommender(db, daily_limits={"x": 1}).recommend(
        platform="x",
        days=3,
        limit=5,
        now=now,
    )

    assert recommendations[0].available is True
    assert recommendations[0].start_time.weekday() == open_day.weekday()
    unavailable = [item for item in recommendations if item.start_time.weekday() == full_day.weekday()][0]
    assert unavailable.available is False
    assert unavailable.cap_queued_count == 1
    assert any("Unavailable" in reason for reason in unavailable.reasons)


def test_historical_engagement_is_platform_and_content_type_specific(db):
    now = datetime.now(timezone.utc).replace(hour=7, minute=0, second=0, microsecond=0)
    good_day = now + timedelta(days=1)
    other_day = now + timedelta(days=2)

    _set_x_post(
        db,
        _past_bucket(now, good_day.weekday(), 9),
        40.0,
        content_type="x_thread",
    )
    _set_x_post(
        db,
        _past_bucket(now, other_day.weekday(), 14),
        5.0,
        content_type="x_post",
    )
    _set_bluesky_post(db, _past_bucket(now, other_day.weekday(), 15), 100.0)

    recommendations = PublishWindowRecommender(db, daily_limits={"x": 3}).recommend(
        platform="x",
        days=3,
        limit=5,
        content_type="x_thread",
        now=now,
    )

    assert [item.platform for item in recommendations] == ["x"]
    assert recommendations[0].start_time.weekday() == good_day.weekday()
    assert recommendations[0].start_time.hour == 9
    assert recommendations[0].content_type == "x_thread"
