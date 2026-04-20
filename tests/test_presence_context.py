"""Tests for prompt context used to improve account presence."""

from datetime import datetime, timedelta, timezone

from synthesis.presence_context import PresenceContextBuilder


def _published_content(db, content: str, content_type: str = "x_post") -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="good",
        content_format="micro_story",
    )
    db.mark_published(content_id, f"https://x.com/me/status/{content_id}", str(content_id))
    return content_id


def test_voice_memory_uses_top_and_low_resonance_posts(db):
    top_id = _published_content(
        db,
        "Spent an hour making retries boring. The bug was not the timeout; it was the invisible retry state.",
    )
    low_id = _published_content(
        db,
        "The future of AI is not about tools, it is about workflows.",
    )
    db.insert_engagement(
        content_id=top_id,
        tweet_id=str(top_id),
        like_count=5,
        retweet_count=1,
        reply_count=2,
        quote_count=0,
        engagement_score=12.0,
    )
    db.conn.execute(
        "UPDATE generated_content SET auto_quality = 'low_resonance' WHERE id = ?",
        (low_id,),
    )
    db.conn.commit()

    section = PresenceContextBuilder(db).build_voice_memory("x_post")

    assert "VOICE MEMORY" in section
    assert "Spent an hour making retries boring" in section
    assert "Avoid repeating" in section
    assert "future of AI" in section


def test_content_mix_includes_planned_topics_gaps_and_role(db):
    db.insert_planned_topic(
        topic="testing",
        angle="why dry-runs caught the risky path",
        target_date=datetime.now(timezone.utc).date().isoformat(),
    )

    section = PresenceContextBuilder(db).build_content_mix("x_long_post")

    assert "CONTENT MIX PLAN" in section
    assert "depth and credibility" in section
    assert "testing" in section
    assert "dry-runs" in section
    assert "Under-covered topics" in section


def test_outcome_learning_includes_real_metrics(db):
    content_id = _published_content(db, "A concrete post that got engagement.")
    db.insert_engagement(
        content_id=content_id,
        tweet_id=str(content_id),
        like_count=2,
        retweet_count=0,
        reply_count=1,
        quote_count=0,
        engagement_score=5.0,
    )
    db.conn.execute(
        "UPDATE generated_content SET auto_quality = 'resonated' WHERE id = ?",
        (content_id,),
    )
    db.insert_profile_metrics(
        platform="x",
        follower_count=123,
        following_count=50,
        tweet_count=20,
        listed_count=1,
    )
    db.conn.commit()

    section = PresenceContextBuilder(db).build_outcome_learning("x_post")

    assert "OUTCOME LEARNING" in section
    assert "1 resonated" in section
    assert "micro_story" in section
    assert "123 followers" in section


def test_render_omits_empty_sections():
    class EmptyDB:
        pass

    rendered = PresenceContextBuilder(EmptyDB()).build_prompt_section("x_post")

    assert "VOICE MEMORY" in rendered
    assert "CONTENT MIX PLAN" in rendered
    assert "OUTCOME LEARNING" in rendered
