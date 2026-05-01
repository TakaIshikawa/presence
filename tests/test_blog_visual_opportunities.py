"""Tests for blog visual opportunity planning."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from output.blog_visual_opportunities import (
    build_blog_visual_opportunity_report,
    format_blog_visual_opportunity_json,
    format_blog_visual_opportunity_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_visual_opportunities.py"
spec = importlib.util.spec_from_file_location("blog_visual_opportunities_script", SCRIPT_PATH)
blog_visual_opportunities_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(blog_visual_opportunities_script)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    content: str,
    content_type: str = "blog_post",
    eval_score: float = 8.0,
    created_days_ago: int = 1,
    image_path: str | None = None,
    source_commits: list[str] | None = None,
    source_messages: list[str] | None = None,
    engagement_score: float | None = None,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=source_commits or [],
        source_messages=source_messages or [],
        source_activity_ids=[],
        content=content,
        eval_score=eval_score,
        eval_feedback="usable",
        image_path=image_path,
    )
    created_at = (NOW - timedelta(days=created_days_ago)).isoformat()
    db.conn.execute(
        "UPDATE generated_content SET created_at = ?, published_at = ? WHERE id = ?",
        (created_at, created_at, content_id),
    )
    if engagement_score is not None:
        db.insert_engagement(
            content_id=content_id,
            tweet_id=f"tweet-{content_id}",
            like_count=1,
            retweet_count=0,
            reply_count=0,
            quote_count=0,
            engagement_score=engagement_score,
        )
        db.conn.execute(
            """UPDATE post_engagement
               SET fetched_at = ?
               WHERE id = (SELECT MAX(id) FROM post_engagement WHERE content_id = ?)""",
            (NOW.isoformat(), content_id),
        )
    db.conn.commit()
    return content_id


def test_planner_prioritizes_missing_visuals_with_planned_topic_context(db):
    content = " ".join(
        ["Architecture note about a queue scheduler pipeline with source evidence."] * 80
    )
    content_id = _content(
        db,
        content=content,
        source_commits=["abc1234"],
        engagement_score=30.0,
    )
    planned_id = db.insert_planned_topic(
        "Queue reliability",
        angle="Show how retries move through the scheduler",
        source_material="abc1234 and production queue traces",
        status="generated",
    )
    db.mark_planned_topic_generated(planned_id, content_id)

    report = build_blog_visual_opportunity_report(db, days=7, limit=5, now=NOW)

    assert report.opportunity_ids == (content_id,)
    item = report.opportunities[0]
    assert item.priority == "high"
    assert item.recommended_visual_type == "title card"
    assert item.title_card_suitability == "high"
    assert item.planned_topic_id == planned_id
    assert any("planned source material" in evidence for evidence in item.source_evidence_to_include)
    assert any("commit: abc1234" == evidence for evidence in item.source_evidence_to_include)
    assert any("planned topic context" in reason for reason in item.rationale)


def test_existing_image_path_is_reported_as_covered_exclusion(db):
    covered_id = _content(db, content="Covered blog post.", image_path="/tmp/social.png")
    eligible_id = _content(db, content="Lesson learned from a migration without a visual.")

    report = build_blog_visual_opportunity_report(db, days=7, limit=5, now=NOW)

    assert report.opportunity_ids == (eligible_id,)
    assert report.counts["covered"] == 1
    assert report.excluded[0].content_id == covered_id
    assert report.excluded[0].reason == "already_has_image_path"
    assert report.excluded[0].image_path == "/tmp/social.png"


def test_blog_related_variant_is_eligible_without_blog_post_type_or_planned_topic(db):
    content_id = _content(
        db,
        content="Short social seed.",
        content_type="x_post",
        source_messages=["msg-1"],
        engagement_score=8.0,
    )
    db.upsert_content_variant(
        content_id,
        platform="blog",
        variant_type="post",
        content="Lesson learned: a longer blog adaptation benefits from a pull quote visual.",
    )

    report = build_blog_visual_opportunity_report(db, days=7, limit=5, now=NOW)

    item = report.opportunities[0]
    assert item.content_id == content_id
    assert item.text_source == "content_variants:blog:post"
    assert item.recommended_visual_type == "pull-quote card"
    assert item.title_card_suitability == "low"
    assert "message: msg-1" in item.source_evidence_to_include


def test_limit_json_and_text_output_are_stable(db):
    first_id = _content(db, content=" ".join(["Pipeline diagram candidate."] * 160))
    second_id = _content(db, content="Short blog visual candidate.", eval_score=5.0)

    report = build_blog_visual_opportunity_report(db, days=7, limit=1, now=NOW)
    payload = json.loads(format_blog_visual_opportunity_json(report))
    text = format_blog_visual_opportunity_text(report)

    assert report.opportunity_ids == (first_id,)
    assert second_id not in report.opportunity_ids
    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "blog_visual_opportunities"
    assert payload["filters"] == {"days": 7, "limit": 1}
    assert "Blog Visual Opportunities" in text
    assert "title_card=medium" in text


def test_invalid_days_and_limit_are_rejected(db):
    with pytest.raises(ValueError, match="days"):
        build_blog_visual_opportunity_report(db, days=0, now=NOW)
    with pytest.raises(ValueError, match="limit"):
        build_blog_visual_opportunity_report(db, limit=0, now=NOW)


def test_cli_supports_days_limit_and_json_format(db, capsys):
    content_id = _content(db, content="Blog post ready for a social preview image.")

    with patch.object(
        blog_visual_opportunities_script,
        "script_context",
        return_value=_script_context(db),
    ):
        exit_code = blog_visual_opportunities_script.main(
            ["--days", "7", "--limit", "1", "--format", "json"]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["filters"] == {"days": 7, "limit": 1}
    assert payload["opportunity_ids"] == [content_id]
