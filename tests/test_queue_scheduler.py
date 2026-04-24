"""Tests for recommended publish queue scheduling."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from evaluation.posting_windows import PostingWindow
from output.queue_scheduler import QueueScheduler


def _window(day_of_week: int, hour_utc: int, score: float = 10.0) -> PostingWindow:
    return PostingWindow(
        day_of_week=day_of_week,
        day_name="Testday",
        hour_utc=hour_utc,
        sample_size=5,
        avg_engagement=score,
        normalized_engagement=score,
        confidence=0.6,
        confidence_label="medium",
        platform="x",
    )


def _insert_content(db, content: str = "Queued post") -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="ok",
    )


def test_next_recommended_slot_uses_platform_windows(db):
    recommender = MagicMock()
    recommender.recommend.return_value = [
        _window(0, 10, score=20.0),
        _window(1, 8, score=18.0),
    ]
    scheduler = QueueScheduler(
        db,
        recommender=recommender,
        daily_platform_limits={},
    )

    slot = scheduler.next_recommended_slot(
        "x",
        now=datetime(2026, 4, 20, 9, 15, tzinfo=timezone.utc),
    )

    assert slot == datetime(2026, 4, 20, 10, 0, tzinfo=timezone.utc)
    recommender.recommend.assert_called_once_with(days=90, platform="x", limit=3)


def test_recommended_slot_moves_out_of_embargo(db):
    recommender = MagicMock()
    recommender.recommend.return_value = [_window(0, 10)]
    scheduler = QueueScheduler(
        db,
        recommender=recommender,
        embargo_windows=[
            {
                "timezone": "UTC",
                "weekdays": ["monday"],
                "start": "10:00",
                "end": "11:00",
            }
        ],
        daily_platform_limits={},
    )

    slot = scheduler.next_recommended_slot(
        "x",
        now=datetime(2026, 4, 20, 9, 0, tzinfo=timezone.utc),
    )

    assert slot == datetime(2026, 4, 20, 11, 0, tzinfo=timezone.utc)


def test_recommended_slot_skips_days_with_platform_cap(db):
    recommender = MagicMock()
    recommender.recommend.return_value = [_window(0, 10)]
    content_id = _insert_content(db, "Already queued")
    db.queue_for_publishing(
        content_id,
        "2026-04-20T12:00:00+00:00",
        platform="x",
    )
    scheduler = QueueScheduler(
        db,
        recommender=recommender,
        daily_platform_limits={"x": 1},
    )

    slot = scheduler.next_recommended_slot(
        "x",
        now=datetime(2026, 4, 20, 9, 0, tzinfo=timezone.utc),
    )

    assert slot == datetime(2026, 4, 27, 10, 0, tzinfo=timezone.utc)


def test_schedule_content_inserts_publish_queue_item(db):
    recommender = MagicMock()
    recommender.recommend.return_value = [_window(0, 10)]
    scheduler = QueueScheduler(
        db,
        recommender=recommender,
        daily_platform_limits={},
    )
    content_id = _insert_content(db)

    result = scheduler.schedule_content(
        content_id,
        "bluesky",
        now=datetime(2026, 4, 20, 9, 0, tzinfo=timezone.utc),
    )

    row = db.get_publish_queue_item(result.queue_id)
    assert row["content_id"] == content_id
    assert row["platform"] == "bluesky"
    assert row["scheduled_at"] == "2026-04-20T10:00:00+00:00"
    assert db.get_publication_state(content_id, "bluesky")["status"] == "queued"
