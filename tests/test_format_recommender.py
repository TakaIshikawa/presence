"""Tests for engagement-based format recommendations."""

from datetime import datetime, timezone

import pytest

from evaluation.format_recommender import FormatRecommendation, FormatRecommender


def _insert_published_with_engagement(
    db,
    *,
    content_type: str = "x_post",
    content_format: str,
    engagement_score: float,
    auto_quality: str | None = None,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=["abc123"],
        source_messages=["uuid1"],
        content=f"Generated {content_format}",
        eval_score=8.0,
        eval_feedback="ok",
        content_format=content_format,
    )
    tweet_id = f"{content_format}-{content_id}"
    db.mark_published(content_id, f"https://x.com/test/{tweet_id}", tweet_id=tweet_id)
    db.conn.execute(
        "UPDATE generated_content SET published_at = ?, auto_quality = ? WHERE id = ?",
        (datetime.now(timezone.utc).isoformat(), auto_quality, content_id),
    )
    db.insert_engagement(
        content_id,
        tweet_id,
        like_count=0,
        retweet_count=0,
        reply_count=0,
        quote_count=0,
        engagement_score=engagement_score,
    )
    return content_id


def test_recommender_ranks_formats_by_recent_engagement(db):
    for _ in range(3):
        _insert_published_with_engagement(
            db,
            content_format="tip",
            engagement_score=18.0,
            auto_quality="resonated",
        )
        _insert_published_with_engagement(
            db,
            content_format="question",
            engagement_score=9.0,
            auto_quality="low_resonance",
        )
        _insert_published_with_engagement(
            db,
            content_format="micro_story",
            engagement_score=12.0,
            auto_quality="resonated",
        )

    recommendations = FormatRecommender(db).recommend("x_post", limit=3)

    assert [item.content_format for item in recommendations] == [
        "tip",
        "micro_story",
        "question",
    ]
    assert all(isinstance(item, FormatRecommendation) for item in recommendations)
    assert recommendations[0].sample_count == 3
    assert recommendations[0].avg_engagement == pytest.approx(18.0)
    assert "averaged 18.0 engagement" in recommendations[0].reason
    assert "3 recent posts" in recommendations[0].reason
    assert not recommendations[0].is_fallback


def test_recommender_uses_defaults_when_history_is_sparse(db):
    _insert_published_with_engagement(
        db,
        content_format="tip",
        engagement_score=100.0,
    )

    recommendations = FormatRecommender(db, min_samples=3).recommend(
        "x_post",
        limit=2,
    )

    assert [item.content_format for item in recommendations] == [
        "micro_story",
        "question",
    ]
    assert all(item.is_fallback for item in recommendations)
    assert all("Fallback default" in item.reason for item in recommendations)
    assert "need at least 3 samples" in recommendations[0].reason


def test_recommender_uses_thread_defaults_for_sparse_thread_history(db):
    recommendations = FormatRecommender(db).recommend("x_thread", limit=2)

    assert [item.content_format for item in recommendations] == [
        "mid_action",
        "bold_claim",
    ]
    assert all(item.is_fallback for item in recommendations)
