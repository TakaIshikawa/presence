"""Tests for weekly audience growth insights."""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from audience_growth_insights import format_json_report, main
from evaluation.audience_growth_insights import AudienceGrowthInsights


NOW = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)


def insert_profile_metric(db, platform, followers, fetched_at):
    db.conn.execute(
        """INSERT INTO profile_metrics
           (platform, follower_count, following_count, tweet_count, listed_count, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (platform, followers, 50, 100, None, fetched_at.isoformat()),
    )
    db.conn.commit()


def publish_post(db, platform, content, published_at, engagement_score, index):
    content_id = db.insert_generated_content(
        content_type="x_thread",
        source_commits=[f"commit-{index}"],
        source_messages=[f"message-{index}"],
        content=content,
        eval_score=8.0,
        eval_feedback="Good",
    )
    db.upsert_publication_success(
        content_id=content_id,
        platform=platform,
        platform_post_id=f"{platform}-{index}",
        platform_url=f"https://example.com/{platform}/{index}",
        published_at=published_at.isoformat(),
    )
    if platform == "x":
        db.insert_engagement(
            content_id=content_id,
            tweet_id=f"tweet-{index}",
            like_count=10,
            retweet_count=2,
            reply_count=1,
            quote_count=0,
            engagement_score=engagement_score,
        )
    else:
        db.insert_bluesky_engagement(
            content_id=content_id,
            bluesky_uri=f"at://did:plc:test/app.bsky.feed.post/{index}",
            like_count=10,
            repost_count=2,
            reply_count=1,
            quote_count=0,
            engagement_score=engagement_score,
        )
    return content_id


def test_weekly_growth_metrics_for_multiple_platforms(db):
    first_start = NOW - timedelta(days=14)
    second_start = NOW - timedelta(days=7)
    insert_profile_metric(db, "x", 100, first_start)
    insert_profile_metric(db, "x", 108, second_start)
    insert_profile_metric(db, "x", 120, NOW)
    insert_profile_metric(db, "bluesky", 50, first_start)
    insert_profile_metric(db, "bluesky", 55, second_start)
    insert_profile_metric(db, "bluesky", 55, NOW)

    publish_post(db, "x", "X launch notes with concrete lessons", second_start + timedelta(days=1), 24.0, 1)
    publish_post(db, "bluesky", "Bluesky recap with useful details", first_start + timedelta(days=2), 15.0, 2)

    report = AudienceGrowthInsights(db, now=NOW).generate(weeks=2, platform="all")

    assert set(report.platforms) == {"x", "bluesky"}
    assert report.platforms["x"][0].follower_delta == 8
    assert report.platforms["x"][0].growth_rate_pct == 8.0
    assert report.platforms["x"][1].follower_delta == 12
    assert report.platforms["x"][1].published_count == 1
    assert report.platforms["x"][1].total_engagement_score == 24.0
    assert report.platforms["x"][1].engagement_to_growth_ratio == 2.0
    assert report.platforms["bluesky"][0].follower_delta == 5
    assert report.platforms["bluesky"][1].follower_delta == 0


def test_attribution_window_ranks_posts_by_engagement(db):
    week_start = NOW - timedelta(days=7)
    insert_profile_metric(db, "x", 200, week_start)
    insert_profile_metric(db, "x", 210, NOW)

    before_window_post = publish_post(
        db,
        "x",
        "Pre-window post that still coincides with the growth window",
        week_start - timedelta(days=2),
        40.0,
        1,
    )
    publish_post(db, "x", "Lower engagement in-window post", week_start + timedelta(days=1), 12.0, 2)
    publish_post(db, "x", "Older post outside attribution window", week_start - timedelta(days=5), 99.0, 3)

    report = AudienceGrowthInsights(db, now=NOW, attribution_days=3).generate(weeks=1, platform="x")
    window = report.platforms["x"][0]

    assert window.follower_delta == 10
    assert [post.content_id for post in window.top_posts] == [before_window_post, before_window_post + 1]
    assert window.top_posts[0].engagement_score == 40.0
    assert window.top_posts[0].engagement_to_growth_ratio == 4.0
    assert window.published_count == 1


def test_quiet_period_detection_surfaces_low_publish_flat_growth(db):
    week_start = NOW - timedelta(days=7)
    insert_profile_metric(db, "bluesky", 80, week_start)
    insert_profile_metric(db, "bluesky", 79, NOW)
    publish_post(db, "bluesky", "Only one low-engagement update", week_start + timedelta(days=2), 3.0, 1)

    report = AudienceGrowthInsights(db, now=NOW).generate(weeks=1, platform="bluesky")

    assert len(report.quiet_periods) == 1
    quiet = report.quiet_periods[0]
    assert quiet.platform == "bluesky"
    assert quiet.follower_delta == -1
    assert quiet.published_count == 1
    assert quiet.reason == "Low publishing volume and flat or negative follower growth."


def test_cli_json_output_with_db_flag(file_db, capsys):
    week_start = datetime.now(timezone.utc) - timedelta(days=7)
    insert_profile_metric(file_db, "x", 100, week_start)
    insert_profile_metric(file_db, "x", 106, datetime.now(timezone.utc))
    publish_post(file_db, "x", "CLI-visible audience growth post", week_start + timedelta(days=1), 18.0, 1)

    main(["--db", str(file_db.db_path), "--weeks", "1", "--platform", "x", "--json"])

    data = json.loads(capsys.readouterr().out)
    assert data["weeks"] == 1
    assert list(data["platforms"]) == ["x"]
    assert data["platforms"]["x"][0]["follower_delta"] == 6
    assert data["platforms"]["x"][0]["top_posts"][0]["content_preview"] == "CLI-visible audience growth post"


def test_format_json_report_serializes_nested_datetimes(db):
    week_start = NOW - timedelta(days=7)
    insert_profile_metric(db, "x", 100, week_start)
    insert_profile_metric(db, "x", 105, NOW)
    publish_post(db, "x", "Serialization post", week_start + timedelta(days=1), 10.0, 1)

    report = AudienceGrowthInsights(db, now=NOW).generate(weeks=1, platform="x")
    data = json.loads(format_json_report(report))

    assert data["period_end"] == "2026-04-22T12:00:00+00:00"
    assert data["platforms"]["x"][0]["week_start"] == "2026-04-15T12:00:00+00:00"
    assert data["platforms"]["x"][0]["top_posts"][0]["published_at"].startswith("2026-04-16T12:00:00")
