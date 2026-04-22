"""Tests for weekly performance brief generation."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.performance_brief import (
    PerformanceBriefBuilder,
    brief_to_dict,
    format_markdown_brief,
)


BASE_TIME = datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc)


def _insert_content(db, text: str, created_at: datetime, content_format: str) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["abc123"],
        source_messages=["uuid-1"],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
        content_format=content_format,
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created_at.isoformat(), content_id),
    )
    db.conn.commit()
    return content_id


def seed_performance_brief(db) -> dict[str, int]:
    campaign_id = db.create_campaign(
        name="Launch Lessons",
        goal="Explain launch lessons",
        start_date="2026-04-01",
        end_date="2026-04-30",
        status="active",
    )
    resonant = _insert_content(
        db,
        "The deploy lesson that changed how we review risky releases.",
        BASE_TIME,
        "micro_story",
    )
    weak = _insert_content(
        db,
        "A vague update about process improvements.",
        BASE_TIME + timedelta(days=1),
        "observation",
    )
    planned_resonant = db.insert_planned_topic(
        "testing",
        angle="preflight checks before deploys",
        target_date="2026-04-21",
        campaign_id=campaign_id,
    )
    db.mark_planned_topic_generated(planned_resonant, resonant)
    db.insert_planned_topic(
        "architecture",
        angle="operational boundaries",
        target_date="2026-04-23",
        campaign_id=campaign_id,
    )

    db.upsert_publication_success(
        resonant,
        "x",
        platform_post_id="tw-res",
        platform_url="https://x.test/tw-res",
        published_at=(BASE_TIME + timedelta(hours=1)).isoformat(),
    )
    db.upsert_publication_success(
        resonant,
        "bluesky",
        platform_post_id="at://did:plc:test/app.bsky.feed.post/res",
        platform_url="https://bsky.app/profile/test/post/res",
        published_at=(BASE_TIME + timedelta(hours=1)).isoformat(),
    )
    db.upsert_publication_success(
        weak,
        "x",
        platform_post_id="tw-weak",
        platform_url="https://x.test/tw-weak",
        published_at=(BASE_TIME + timedelta(days=1, hours=1)).isoformat(),
    )
    db.conn.execute(
        "UPDATE generated_content SET auto_quality = 'resonated' WHERE id = ?",
        (resonant,),
    )
    db.conn.execute(
        "UPDATE generated_content SET auto_quality = 'low_resonance' WHERE id = ?",
        (weak,),
    )
    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, like_count, retweet_count, reply_count, quote_count,
            engagement_score, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            resonant,
            "tw-res",
            10,
            4,
            2,
            1,
            34.0,
            (BASE_TIME + timedelta(days=2)).isoformat(),
        ),
    )
    db.conn.execute(
        """INSERT INTO bluesky_engagement
           (content_id, bluesky_uri, like_count, repost_count, reply_count, quote_count,
            engagement_score, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            resonant,
            "at://did:plc:test/app.bsky.feed.post/res",
            8,
            3,
            1,
            0,
            21.0,
            (BASE_TIME + timedelta(days=2)).isoformat(),
        ),
    )
    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, like_count, retweet_count, reply_count, quote_count,
            engagement_score, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            weak,
            "tw-weak",
            1,
            0,
            0,
            0,
            2.0,
            (BASE_TIME + timedelta(days=2)).isoformat(),
        ),
    )
    db.upsert_content_variant(
        resonant,
        platform="linkedin",
        variant_type="summary",
        content="Longer launch lesson summary",
        metadata={"source": "test"},
    )
    db.conn.commit()
    return {"campaign_id": campaign_id, "resonant": resonant, "weak": weak}


def test_performance_brief_combines_publication_engagement_and_campaign_metadata(db):
    ids = seed_performance_brief(db)

    brief = PerformanceBriefBuilder(db).build("2026-04-20")

    assert brief.week_start == "2026-04-20"
    assert brief.week_end == "2026-04-27"
    assert brief.generated_count == 2
    assert brief.published_count == 2
    assert brief.publication_count == 3
    assert brief.platform_summary["x"]["avg_engagement_score"] == 18.0
    assert brief.platform_summary["bluesky"]["total_engagement_score"] == 21.0

    top = brief.resonated[0]
    assert top.content_id == ids["resonant"]
    assert top.combined_engagement_score == 55.0
    assert top.campaign_name == "Launch Lessons"
    assert top.planned_topic == "testing"
    assert top.variants[0].platform == "linkedin"

    weak = brief.underperformed[0]
    assert weak.content_id == ids["weak"]
    assert weak.auto_quality == "low_resonance"

    assert any(topic.topic == "architecture" for topic in brief.planned_topics)
    assert any("Fill planned topic" in suggestion for suggestion in brief.try_next)


def test_performance_brief_serializes_to_dict(db):
    ids = seed_performance_brief(db)

    data = brief_to_dict(PerformanceBriefBuilder(db).build("2026-04-20"))

    assert data["published"][0]["content_id"] in {ids["resonant"], ids["weak"]}
    assert data["resonated"][0]["publications"][0]["platform"] in {"bluesky", "x"}
    assert data["planned_topics"][0]["campaign_name"] == "Launch Lessons"


def test_markdown_brief_includes_operator_links_and_ids(db):
    seed_performance_brief(db)

    output = format_markdown_brief(PerformanceBriefBuilder(db).build("2026-04-20"))

    assert "# Weekly Performance Brief: 2026-04-20 to 2026-04-27" in output
    assert "content #" in output
    assert "pub #" in output
    assert "[tw-res](https://x.test/tw-res)" in output
    assert "variant #" in output
    assert "campaign #" in output
    assert "## Try Next" in output
