"""Tests for campaign_report.py."""

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from campaign_report import (
    format_json_report,
    format_retrospective_json,
    format_retrospective_table,
    format_text_report,
    main,
)
from evaluation.pipeline_analytics import (
    CampaignPerformanceReport,
    CampaignRetrospectiveReport,
    PipelineAnalytics,
)


def sample_report() -> CampaignPerformanceReport:
    """Build a minimal campaign report fixture."""
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    return CampaignPerformanceReport(
        campaign={
            "id": 7,
            "name": "Launch Campaign",
            "goal": "Explain launch lessons",
            "start_date": "2026-04-01",
            "end_date": "2026-04-30",
            "status": "active",
            "created_at": "2026-04-01T00:00:00+00:00",
        },
        period_days=14,
        period_start=now,
        period_end=now,
        topic_counts={
            "total": 3,
            "planned": 1,
            "generated": 1,
            "skipped": 1,
        },
        avg_eval_score=8.5,
        per_platform_engagement={
            "x": {
                "content_count": 1,
                "avg_engagement_score": 28.0,
                "total_engagement_score": 28.0,
                "min_engagement_score": 28.0,
                "max_engagement_score": 28.0,
            },
            "bluesky": {
                "content_count": 1,
                "avg_engagement_score": 16.0,
                "total_engagement_score": 16.0,
                "min_engagement_score": 16.0,
                "max_engagement_score": 16.0,
            },
            "newsletter": {
                "send_count": 1,
                "content_count": 1,
                "subscriber_count_total": 150,
                "avg_subscribers_per_send": 150.0,
            },
        },
        top_content=[
            {
                "content_id": 42,
                "topic": "architecture",
                "content": "Launch architecture post",
                "eval_score": 8.5,
                "combined_engagement_score": 44.0,
            }
        ],
        gaps=[
            {
                "type": "unfilled_topic",
                "planned_topic_id": 2,
                "topic": "testing",
                "target_date": "2026-04-20",
            }
        ],
    )


def sample_retrospective() -> CampaignRetrospectiveReport:
    """Build a minimal retrospective report fixture."""
    return CampaignRetrospectiveReport(
        campaign={
            "id": 7,
            "name": "Launch Campaign",
            "goal": "Explain launch lessons",
            "start_date": "2026-04-01",
            "end_date": "2026-04-30",
            "status": "completed",
            "created_at": "2026-04-01T00:00:00+00:00",
        },
        planned_topics=3,
        generated_topics=2,
        published_items=2,
        avg_engagement_score=31.5,
        top_content=[
            {
                "content_id": 42,
                "topic": "architecture",
                "content": "Launch architecture post",
                "eval_score": 8.5,
                "published_platforms": ["bluesky", "x"],
                "platform_scores": {"x": 28.0, "bluesky": 16.0},
                "combined_engagement_score": 44.0,
                "latest_published_at": "2026-04-20T12:00:00+00:00",
            }
        ],
        missed_planned_topics=[
            {
                "planned_topic_id": 2,
                "topic": "testing",
                "angle": "Launch testing",
                "target_date": "2026-04-20",
                "status": "planned",
            }
        ],
        platform_split={
            "x": {
                "published_items": 1,
                "engagement_count": 1,
                "avg_engagement_score": 28.0,
                "total_engagement_score": 28.0,
            },
            "bluesky": {
                "published_items": 1,
                "engagement_count": 1,
                "avg_engagement_score": 16.0,
                "total_engagement_score": 16.0,
            },
        },
    )


def test_format_text_report_includes_campaign_sections():
    output = format_text_report(sample_report())

    assert "Campaign Report: Launch Campaign" in output
    assert "Total 3, planned 1, generated 1, skipped 1" in output
    assert "Avg eval score: 8.50/10" in output
    assert "x: 1 content items, avg 28.00, total 28.00" in output
    assert "newsletter: 1 sends, 1 content items, 150 subscriber impressions" in output
    assert "#42 architecture" in output
    assert "unfilled topic: testing" in output


def test_format_json_report_serializes_datetimes():
    data = json.loads(format_json_report(sample_report()))

    assert data["campaign"]["id"] == 7
    assert data["period_start"] == "2026-04-22T12:00:00+00:00"
    assert data["topic_counts"]["generated"] == 1
    assert data["per_platform_engagement"]["bluesky"]["avg_engagement_score"] == 16.0


def test_empty_report_formats_error():
    assert format_text_report(None) == "No campaign data found."
    assert json.loads(format_json_report(None)) == {"error": "No campaign data found"}


def test_format_retrospective_table_includes_campaign_totals():
    output = format_retrospective_table([sample_retrospective()])

    assert "Campaign" in output
    assert "Launch Campaign" in output
    assert "completed" in output
    assert "3" in output
    assert "x:1" in output
    assert "bluesky:1" in output
    assert "#42 architecture (44.00)" in output


def test_format_retrospective_json_serializes_reports():
    data = json.loads(format_retrospective_json([sample_retrospective()]))

    assert data[0]["campaign"]["id"] == 7
    assert data[0]["planned_topics"] == 3
    assert data[0]["platform_split"]["x"]["avg_engagement_score"] == 28.0


def test_empty_retrospective_formats_error():
    assert format_retrospective_table([]) == "No campaign data found."
    assert json.loads(format_retrospective_json([])) == {
        "error": "No campaign data found"
    }


def test_main_uses_campaign_id_and_json(capsys):
    analytics = MagicMock()
    analytics.campaign_performance_report.return_value = sample_report()
    context_db = MagicMock()

    @contextmanager
    def fake_script_context():
        yield MagicMock(), context_db

    with patch("campaign_report.script_context", fake_script_context), \
         patch("campaign_report.PipelineAnalytics", return_value=analytics), \
         patch.object(sys, "argv", ["campaign_report.py", "--campaign-id", "7", "--days", "14", "--json"]):
        main()

    analytics.campaign_performance_report.assert_called_once_with(
        campaign_id=7,
        active=False,
        days=14,
    )
    data = json.loads(capsys.readouterr().out)
    assert data["campaign"]["name"] == "Launch Campaign"


def test_main_uses_retrospective_mode_and_json(capsys):
    analytics = MagicMock()
    analytics.campaign_retrospectives.return_value = [sample_retrospective()]

    @contextmanager
    def fake_script_context():
        yield MagicMock(), MagicMock()

    with patch("campaign_report.script_context", fake_script_context), \
         patch("campaign_report.PipelineAnalytics", return_value=analytics), \
         patch.object(
             sys,
             "argv",
             ["campaign_report.py", "--campaign-id", "7", "--retrospective", "--json"],
         ):
        main()

    analytics.campaign_retrospectives.assert_called_once_with(
        campaign_id=7,
        active=False,
    )
    data = json.loads(capsys.readouterr().out)
    assert data[0]["campaign"]["name"] == "Launch Campaign"


def test_main_defaults_to_active_campaign(capsys):
    analytics = MagicMock()
    analytics.campaign_performance_report.return_value = None

    @contextmanager
    def fake_script_context():
        yield MagicMock(), MagicMock()

    with patch("campaign_report.script_context", fake_script_context), \
         patch("campaign_report.PipelineAnalytics", return_value=analytics), \
         patch.object(sys, "argv", ["campaign_report.py"]):
        main()

    analytics.campaign_performance_report.assert_called_once_with(
        campaign_id=None,
        active=True,
        days=30,
    )
    assert "No campaign data found." in capsys.readouterr().out


def test_campaign_retrospective_scores_active_and_completed_campaigns(db):
    now = datetime.now(timezone.utc)
    active_id = db.create_campaign(
        name="Active Launch",
        goal="Launch",
        start_date=(now - timedelta(days=2)).date().isoformat(),
        end_date=(now + timedelta(days=5)).date().isoformat(),
        status="active",
    )
    completed_id = db.create_campaign(
        name="Completed Launch",
        goal="Retrospective",
        start_date=(now - timedelta(days=20)).date().isoformat(),
        end_date=(now - timedelta(days=10)).date().isoformat(),
        status="completed",
    )
    planned_id = db.create_campaign(name="Future Launch", status="planned")

    active_topic = db.insert_planned_topic(
        topic="architecture",
        angle="Active architecture",
        target_date=(now - timedelta(days=1)).date().isoformat(),
        campaign_id=active_id,
    )
    active_content = db.insert_generated_content(
        content_type="x_post",
        source_commits=["abc123"],
        source_messages=["msg-1"],
        content="Active architecture post",
        eval_score=8.5,
        eval_feedback="Strong",
    )
    db.mark_planned_topic_generated(active_topic, active_content)
    db.upsert_publication_success(
        active_content,
        "x",
        platform_post_id="tweet-active",
        platform_url="https://x.com/test/active",
        published_at=(now - timedelta(hours=5)).isoformat(),
    )
    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, like_count, retweet_count, reply_count,
            quote_count, engagement_score, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            active_content,
            "tweet-active",
            10,
            0,
            0,
            0,
            10.0,
            (now - timedelta(hours=2)).isoformat(),
        ),
    )
    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, like_count, retweet_count, reply_count,
            quote_count, engagement_score, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            active_content,
            "tweet-active",
            30,
            0,
            0,
            0,
            30.0,
            (now - timedelta(minutes=30)).isoformat(),
        ),
    )

    completed_topic = db.insert_planned_topic(
        topic="testing",
        angle="Completed testing",
        target_date=(now - timedelta(days=12)).date().isoformat(),
        campaign_id=completed_id,
    )
    completed_content = db.insert_generated_content(
        content_type="x_thread",
        source_commits=["def456"],
        source_messages=["msg-2"],
        content="Completed testing thread",
        eval_score=7.5,
        eval_feedback="Good",
    )
    db.mark_planned_topic_generated(completed_topic, completed_content)
    db.upsert_publication_success(
        completed_content,
        "bluesky",
        platform_post_id="at://test/post/completed",
        platform_url="https://bsky.app/profile/test/post/completed",
        published_at=(now - timedelta(days=11)).isoformat(),
    )
    db.insert_bluesky_engagement(
        completed_content,
        "at://test/post/completed",
        20,
        2,
        1,
        0,
        24.0,
    )
    db.insert_planned_topic(
        topic="developer-experience",
        angle="Missed DX",
        target_date=(now - timedelta(days=9)).date().isoformat(),
        campaign_id=completed_id,
    )
    db.insert_planned_topic(
        topic="ai-agents",
        angle="Future topic",
        target_date=(now + timedelta(days=2)).date().isoformat(),
        campaign_id=planned_id,
    )
    db.conn.commit()

    reports = PipelineAnalytics(db).campaign_retrospectives()
    reports_by_name = {report.campaign["name"]: report for report in reports}

    assert set(reports_by_name) == {"Active Launch", "Completed Launch"}
    active = reports_by_name["Active Launch"]
    assert active.planned_topics == 1
    assert active.generated_topics == 1
    assert active.published_items == 1
    assert active.avg_engagement_score == 30.0
    assert active.platform_split["x"]["avg_engagement_score"] == 30.0
    assert active.top_content[0]["content_id"] == active_content

    completed = reports_by_name["Completed Launch"]
    assert completed.planned_topics == 2
    assert completed.generated_topics == 1
    assert completed.published_items == 1
    assert completed.avg_engagement_score == 24.0
    assert completed.platform_split["bluesky"]["published_items"] == 1
    assert completed.missed_planned_topics[0]["topic"] == "developer-experience"


def test_campaign_retrospective_handles_campaign_with_no_publications(db):
    campaign_id = db.create_campaign(name="Quiet Campaign", status="completed")
    topic_id = db.insert_planned_topic(
        topic="architecture",
        angle="No publication yet",
        campaign_id=campaign_id,
    )
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["abc123"],
        source_messages=["msg-1"],
        content="Generated but unpublished",
        eval_score=7.0,
        eval_feedback="Fine",
    )
    db.mark_planned_topic_generated(topic_id, content_id)

    report = PipelineAnalytics(db).campaign_retrospective_report(
        campaign_id=campaign_id
    )

    assert report is not None
    assert report.planned_topics == 1
    assert report.generated_topics == 1
    assert report.published_items == 0
    assert report.avg_engagement_score == 0.0
    assert report.platform_split == {}
    assert report.top_content == []
