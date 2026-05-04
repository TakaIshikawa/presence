"""Tests for automatic content mix planning."""

import json
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from synthesis.content_mix import ContentMixPlanner

_SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_mix.py"
_SPEC = spec_from_file_location("content_mix_script", _SCRIPT_PATH)
_SCRIPT = module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
_SPEC.loader.exec_module(_SCRIPT)

build_report = _SCRIPT.build_report
format_json_report = _SCRIPT.format_json_report
format_text_report = _SCRIPT.format_text_report


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


def test_choose_visual_for_medium_depth_with_text_heavy_recent_mix(db):
    _publish(db, "x_post", "post 1")
    _publish(db, "x_post", "post 2")
    _publish(db, "x_thread", "thread 1")

    decision = ContentMixPlanner(db).choose(
        accumulated_tokens=2000,
        has_prompts=True,
    )

    assert decision.content_type == "x_visual"
    assert "visual pattern interrupt" in decision.reason


def test_choose_blog_post_for_very_deep_source_with_recent_mix(db):
    _publish(db, "x_thread", "thread 1")
    _publish(db, "x_thread", "thread 2")
    _publish(db, "x_post", "post 1")

    decision = ContentMixPlanner(db).choose(
        accumulated_tokens=4200,
        has_prompts=True,
    )

    assert decision.content_type == "blog_post"
    assert "durable blog post" in decision.reason


def test_snapshot_tracks_recent_mix_across_supported_types(db):
    _publish(db, "x_post", "post 1")
    _publish(db, "x_visual", "visual 1")
    _publish(db, "blog_post", "blog 1")

    snapshot = ContentMixPlanner(db).snapshot()

    assert snapshot.recent_limit == 6
    assert snapshot.recent_content_types[:3] == ["blog_post", "x_visual", "x_post"]
    assert snapshot.counts == {
        "x_post": 1,
        "x_thread": 0,
        "x_visual": 1,
        "blog_post": 1,
    }


def test_cli_report_formats_text_and_json(db):
    _publish(db, "x_post", "post 1")
    report = build_report(db, accumulated_tokens=900, has_prompts=True, recent_limit=4)

    text = format_text_report(report)
    payload = json.loads(format_json_report(report))

    assert "CONTENT MIX SNAPSHOT" in text
    assert "PLANNER DECISION" in text
    assert payload["inputs"]["accumulated_tokens"] == 900
    assert payload["decision"]["content_type"] == "x_post"


def test_choose_post_when_no_prompts_available(db):
    """Test fallback to x_post when no prompt context exists."""
    _publish(db, "x_thread", "thread 1")
    decision = ContentMixPlanner(db).choose(
        accumulated_tokens=3000,
        has_prompts=False,
    )

    assert decision.content_type == "x_post"
    assert "No prompt context" in decision.reason


def test_empty_recent_content_chooses_based_on_tokens(db):
    """Test content type selection with no recent content."""
    # High token count should choose thread
    decision_high = ContentMixPlanner(db).choose(
        accumulated_tokens=2000,
        has_prompts=True,
    )
    assert decision_high.content_type == "x_thread"

    # Low token count should choose post
    decision_low = ContentMixPlanner(db).choose(
        accumulated_tokens=500,
        has_prompts=True,
    )
    assert decision_low.content_type == "x_post"


def test_snapshot_with_empty_database(db):
    """Test snapshot when no content has been published."""
    snapshot = ContentMixPlanner(db).snapshot()

    assert snapshot.recent_limit == 6
    assert snapshot.recent_content_types == []
    assert snapshot.counts == {
        "x_post": 0,
        "x_thread": 0,
        "x_visual": 0,
        "blog_post": 0,
    }


def test_snapshot_respects_recent_limit(db):
    """Test that snapshot only includes content up to recent_limit."""
    for i in range(10):
        _publish(db, "x_post", f"post {i}")

    planner = ContentMixPlanner(db, recent_limit=3)
    snapshot = planner.snapshot()

    assert len(snapshot.recent_content_types) == 3
    assert snapshot.counts["x_post"] == 3


def test_content_type_distribution_calculation(db):
    """Test accurate counting of different content types."""
    _publish(db, "x_post", "post 1")
    _publish(db, "x_post", "post 2")
    _publish(db, "x_thread", "thread 1")
    _publish(db, "x_visual", "visual 1")
    _publish(db, "blog_post", "blog 1")

    snapshot = ContentMixPlanner(db).snapshot()

    assert snapshot.counts["x_post"] == 2
    assert snapshot.counts["x_thread"] == 1
    assert snapshot.counts["x_visual"] == 1
    assert snapshot.counts["blog_post"] == 1


def test_token_threshold_boundaries(db):
    """Test content type selection at threshold boundaries."""
    # Just below thread threshold should choose post
    decision_below = ContentMixPlanner(db, thread_token_threshold=1400).choose(
        accumulated_tokens=1399,
        has_prompts=True,
    )
    assert decision_below.content_type == "x_post"

    # At thread threshold should choose thread
    decision_at = ContentMixPlanner(db, thread_token_threshold=1400).choose(
        accumulated_tokens=1400,
        has_prompts=True,
    )
    assert decision_at.content_type == "x_thread"


def test_visual_requires_no_blog_or_visual_in_recent_mix(db):
    """Test that visual content requires no recent blog/visual posts."""
    _publish(db, "x_post", "post 1")
    _publish(db, "x_post", "post 2")
    _publish(db, "blog_post", "blog 1")

    decision = ContentMixPlanner(db).choose(
        accumulated_tokens=2000,
        has_prompts=True,
    )

    # Should not choose visual because blog_post exists in recent mix
    assert decision.content_type != "x_visual"


def test_blog_post_requires_breadth_in_recent_mix(db):
    """Test blog post requires diverse recent content."""
    # Not enough breadth - only one post
    _publish(db, "x_post", "post 1")

    decision_narrow = ContentMixPlanner(db, blog_token_threshold=3600).choose(
        accumulated_tokens=4000,
        has_prompts=True,
    )

    # Should not choose blog due to lack of breadth (needs >= 2 posts)
    assert decision_narrow.content_type != "blog_post"

    # Add enough breadth - 2 posts satisfies the condition
    _publish(db, "x_post", "post 2")

    decision_broad = ContentMixPlanner(db, blog_token_threshold=3600).choose(
        accumulated_tokens=4000,
        has_prompts=True,
    )

    # Should choose blog with sufficient breadth (>= 2 posts)
    assert decision_broad.content_type == "blog_post"


def test_custom_thresholds(db):
    """Test planner with custom token thresholds."""
    planner = ContentMixPlanner(
        db,
        thread_token_threshold=2000,
        visual_token_threshold=2500,
        blog_token_threshold=5000,
    )

    # Test thread threshold
    decision_thread = planner.choose(
        accumulated_tokens=2000,
        has_prompts=True,
    )
    assert decision_thread.content_type == "x_thread"

    # Test below all thresholds
    decision_post = planner.choose(
        accumulated_tokens=1000,
        has_prompts=True,
    )
    assert decision_post.content_type == "x_post"


def test_single_content_type_category(db):
    """Test behavior when only one content type exists."""
    for i in range(5):
        _publish(db, "x_post", f"post {i}")

    snapshot = ContentMixPlanner(db).snapshot()

    assert snapshot.counts["x_post"] == 5
    assert snapshot.counts["x_thread"] == 0
    assert snapshot.counts["x_visual"] == 0
    assert snapshot.counts["blog_post"] == 0


def test_decision_reason_includes_token_count(db):
    """Test that decision reasons include relevant context."""
    decision = ContentMixPlanner(db).choose(
        accumulated_tokens=800,
        has_prompts=True,
    )

    assert "800 tokens" in decision.reason
