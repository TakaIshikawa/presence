"""Tests for rewrite idea seeding from low-resonance content."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from synthesis.low_resonance_rewriter import LowResonanceRewriter, SOURCE_NAME


NOW = datetime(2026, 4, 25, tzinfo=timezone.utc)


def _add_published_content(
    db,
    *,
    content: str,
    published_at: str,
    auto_quality: str = "low_resonance",
    eval_score: float = 7.0,
    topic: str | None = "testing",
    engagement_score: float = 0.0,
    predicted_score: float | None = 8.0,
    likes: int = 0,
    replies: int = 0,
) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=eval_score,
        eval_feedback="good draft",
    )
    db.conn.execute(
        """UPDATE generated_content
           SET published = 1, published_at = ?, published_url = ?, auto_quality = ?
           WHERE id = ?""",
        (published_at, f"https://example.test/posts/{content_id}", auto_quality, content_id),
    )
    db.conn.commit()
    if topic:
        db.insert_content_topics(content_id, [(topic, "", 1.0)])
    db.insert_engagement(
        content_id=content_id,
        tweet_id=f"tweet-{content_id}",
        like_count=likes,
        retweet_count=0,
        reply_count=replies,
        quote_count=0,
        engagement_score=engagement_score,
    )
    if predicted_score is not None:
        db.insert_prediction(
            content_id=content_id,
            predicted_score=predicted_score,
            hook_strength=7.5,
            specificity=4.0,
            emotional_resonance=6.0,
            novelty=7.0,
            actionability=5.5,
        )
    return content_id


def test_find_candidates_only_includes_low_resonance_published_in_window(db):
    included_id = _add_published_content(
        db,
        content="A generic test automation observation that did not land.",
        published_at="2026-04-23T10:00:00+00:00",
        topic="testing",
        engagement_score=1.0,
        predicted_score=7.0,
    )
    _add_published_content(
        db,
        content="Old weak post",
        published_at="2026-03-01T10:00:00+00:00",
        topic="old",
        engagement_score=0.0,
    )
    _add_published_content(
        db,
        content="Strong post",
        published_at="2026-04-24T10:00:00+00:00",
        auto_quality="resonated",
        topic="strong",
        engagement_score=20.0,
    )
    unpublished_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Unpublished low resonance",
        eval_score=8.0,
        eval_feedback="good",
    )
    db.conn.execute(
        "UPDATE generated_content SET auto_quality = 'low_resonance' WHERE id = ?",
        (unpublished_id,),
    )
    db.conn.commit()
    db.insert_engagement(unpublished_id, "tweet-unpublished", 0, 0, 0, 0, 0.0)

    candidates = LowResonanceRewriter(db).find_candidates(
        days=7,
        min_score_gap=2.0,
        limit=10,
        now=NOW,
    )

    assert [candidate.source_content_id for candidate in candidates] == [included_id]
    assert candidates[0].topic == "testing"
    assert candidates[0].score_gap == 6.0
    assert "weakest predicted dimension was specificity" in candidates[0].note


def test_seed_ideas_skips_existing_matching_source_metadata(db):
    content_id = _add_published_content(
        db,
        content="A low-resonance post about release notes.",
        published_at="2026-04-23T10:00:00+00:00",
        topic="release notes",
        engagement_score=0.0,
        predicted_score=6.0,
    )
    existing_id = db.add_content_idea(
        note="Existing rewrite seed",
        topic="release notes",
        source=SOURCE_NAME,
        source_metadata={
            "source": SOURCE_NAME,
            "source_content_id": content_id,
        },
    )

    results = LowResonanceRewriter(db).seed_ideas(
        days=7,
        limit=10,
        min_score_gap=1.0,
        now=NOW,
    )

    assert len(results) == 1
    assert results[0].status == "skipped"
    assert results[0].idea_id == existing_id
    assert results[0].reason == "open duplicate"
    assert len(db.get_content_ideas(status="open")) == 1


def test_seed_ideas_dry_run_returns_candidates_without_writing(db):
    _add_published_content(
        db,
        content="An abstract agent workflow post that got no engagement.",
        published_at="2026-04-22T10:00:00+00:00",
        topic="agents",
        engagement_score=0.0,
        predicted_score=8.0,
    )

    results = LowResonanceRewriter(db).seed_ideas(
        days=7,
        dry_run=True,
        limit=5,
        min_score_gap=1.0,
        priority="high",
        now=NOW,
    )

    assert len(results) == 1
    assert results[0].status == "skipped"
    assert results[0].reason == "dry run"
    assert results[0].candidate.priority == "high"
    assert db.get_content_ideas(status="open") == []


def test_seed_ideas_creates_content_idea_with_source_metadata(db):
    content_id = _add_published_content(
        db,
        content="A low-resonance post about test fixture cleanup.",
        published_at="2026-04-24T10:00:00+00:00",
        topic="testing",
        engagement_score=0.0,
        predicted_score=7.5,
    )

    results = LowResonanceRewriter(db).seed_ideas(
        days=7,
        limit=5,
        min_score_gap=1.0,
        priority="low",
        now=NOW,
    )

    assert [result.status for result in results] == ["created"]
    idea = db.get_content_idea(results[0].idea_id)
    assert idea["topic"] == "testing"
    assert idea["source"] == SOURCE_NAME
    assert idea["priority"] == "low"
    assert "Rewrite low-resonance testing" in idea["note"]
    metadata = json.loads(idea["source_metadata"])
    assert metadata["source"] == SOURCE_NAME
    assert metadata["source_content_id"] == content_id
    assert metadata["source_content_type"] == "x_post"
    assert metadata["engagement_score"] == 0.0
    assert metadata["score_gap"] == 7.5
