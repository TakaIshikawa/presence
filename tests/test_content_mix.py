"""Tests for automatic content mix planning."""

from synthesis.content_mix import ContentMixPlanner


def _publish(db, content_type: str, content: str):
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.mark_published(content_id, f"https://x.com/me/status/{content_id}", str(content_id))
    return content_id


def test_choose_thread_for_deep_source_material(db):
    decision = ContentMixPlanner(db).choose(
        accumulated_tokens=2500,
        has_prompts=True,
    )

    assert decision.content_type == "x_thread"
    assert "supports a thread" in decision.reason


def test_choose_post_when_recent_mix_is_thread_heavy(db):
    _publish(db, "x_thread", "thread 1")
    _publish(db, "x_thread", "thread 2")
    _publish(db, "x_post", "post 1")

    decision = ContentMixPlanner(db).choose(
        accumulated_tokens=3000,
        has_prompts=True,
    )

    assert decision.content_type == "x_post"
    assert "thread-heavy" in decision.reason


def test_choose_post_for_light_source_material(db):
    decision = ContentMixPlanner(db).choose(
        accumulated_tokens=300,
        has_prompts=True,
    )

    assert decision.content_type == "x_post"


def test_choose_thread_after_many_recent_short_posts(db):
    _publish(db, "x_post", "post 1")
    _publish(db, "x_post", "post 2")
    _publish(db, "x_post", "post 3")

    decision = ContentMixPlanner(db).choose(
        accumulated_tokens=1000,
        has_prompts=True,
    )

    assert decision.content_type == "x_thread"
