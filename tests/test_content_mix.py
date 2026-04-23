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
