"""Tests for campaign retrospective reports."""

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from campaign_retrospective import main
from evaluation.campaign_retrospective import (
    CampaignRetrospectiveGenerator,
    format_json_report,
    format_markdown_report,
)


def seed_populated_campaign(db) -> dict[str, int]:
    now = datetime.now(timezone.utc)
    campaign_id = db.create_campaign(
        name="Launch Retrospective",
        goal="Understand launch campaign outcomes",
        start_date=(now - timedelta(days=10)).date().isoformat(),
        end_date=(now - timedelta(days=1)).date().isoformat(),
        status="completed",
    )

    architecture_topic = db.insert_planned_topic(
        topic="architecture",
        angle="Launch architecture lessons",
        target_date=(now - timedelta(days=8)).date().isoformat(),
        campaign_id=campaign_id,
    )
    architecture_content = db.insert_generated_content(
        content_type="x_post",
        source_commits=["abc123"],
        source_messages=["msg-1"],
        content="Launch architecture post with a concrete lesson.",
        eval_score=8.5,
        eval_feedback="Strong",
    )
    db.mark_planned_topic_generated(architecture_topic, architecture_content)
    db.upsert_publication_success(
        architecture_content,
        "x",
        platform_post_id="tweet-1",
        platform_url="https://x.com/test/1",
        published_at=(now - timedelta(days=7)).isoformat(),
    )
    db.insert_engagement(architecture_content, "tweet-1", 20, 3, 2, 1, 28.0)

    testing_topic = db.insert_planned_topic(
        topic="testing",
        angle="Launch testing notes",
        target_date=(now - timedelta(days=6)).date().isoformat(),
        campaign_id=campaign_id,
    )
    testing_content = db.insert_generated_content(
        content_type="x_thread",
        source_commits=["def456"],
        source_messages=["msg-2"],
        content="Testing thread from the launch.",
        eval_score=7.5,
        eval_feedback="Good",
    )
    db.mark_planned_topic_generated(testing_topic, testing_content)
    db.upsert_publication_success(
        testing_content,
        "bluesky",
        platform_post_id="at://test/post/1",
        platform_url="https://bsky.app/profile/test/post/1",
        published_at=(now - timedelta(days=5)).isoformat(),
    )
    db.insert_bluesky_engagement(testing_content, "at://test/post/1", 12, 2, 1, 0, 16.0)

    db.insert_planned_topic(
        topic="developer-experience",
        angle="Missed launch DX topic",
        target_date=(now - timedelta(days=4)).date().isoformat(),
        campaign_id=campaign_id,
    )
    return {
        "campaign_id": campaign_id,
        "architecture_content": architecture_content,
        "testing_content": testing_content,
    }


def test_build_report_summarizes_populated_campaign(db):
    seeded = seed_populated_campaign(db)

    report = CampaignRetrospectiveGenerator(db).build_report(seeded["campaign_id"])

    assert report is not None
    assert report["campaign"]["name"] == "Launch Retrospective"
    assert report["planned_topic_status_counts"] == {
        "total": 3,
        "planned": 1,
        "generated": 2,
        "skipped": 0,
    }
    assert report["publication_metrics"]["published_items"] == 2
    assert report["publication_metrics"]["platform_counts"] == {"bluesky": 1, "x": 1}
    assert report["publication_metrics"]["total_engagement_score"] == 44.0
    assert report["top_content"][0]["content_id"] == seeded["architecture_content"]
    assert report["missed_topics"][0]["topic"] == "developer-experience"
    assert any("Continue the strongest angle" in item for item in report["recommendations"])

    markdown = format_markdown_report(report)
    assert "# Campaign Retrospective: Launch Retrospective" in markdown
    assert "Published items: 2" in markdown
    assert "developer-experience" in markdown


def test_build_report_handles_campaign_with_no_published_content(db):
    campaign_id = db.create_campaign(
        name="Quiet Campaign",
        goal="Draft before publishing",
        status="active",
    )
    topic_id = db.insert_planned_topic(
        topic="architecture",
        angle="Generated but unpublished",
        campaign_id=campaign_id,
    )
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["abc123"],
        source_messages=["msg-1"],
        content="Generated draft",
        eval_score=7.0,
        eval_feedback="Fine",
    )
    db.mark_planned_topic_generated(topic_id, content_id)

    report = CampaignRetrospectiveGenerator(db).build_report(campaign_id)

    assert report is not None
    assert report["published_items"] == 0
    assert report["publication_metrics"]["platform_counts"] == {}
    assert report["publication_metrics"]["avg_engagement_score"] == 0.0
    assert report["top_content"] == []
    assert "No published campaign content yet." in format_markdown_report(report)
    assert any("publish queue" in item for item in report["recommendations"])


def test_json_output_includes_required_sections(db):
    seeded = seed_populated_campaign(db)

    data = json.loads(
        format_json_report(
            CampaignRetrospectiveGenerator(db).build_report(seeded["campaign_id"])
        )
    )

    assert data["campaign"]["id"] == seeded["campaign_id"]
    assert data["planned_topic_status_counts"]["generated"] == 2
    assert data["publication_metrics"]["platform_counts"]["x"] == 1
    assert data["top_content"]
    assert data["missed_topics"][0]["topic"] == "developer-experience"
    assert data["recommendations"]


def test_json_output_for_missing_campaign():
    assert json.loads(format_json_report(None)) == {"error": "No campaign data found"}


def test_cli_accepts_campaign_id_and_outputs_json(capsys):
    generator = MagicMock()
    generator.build_report.return_value = {
        "campaign": {"id": 7, "name": "Launch", "status": "completed"},
        "planned_topic_status_counts": {"total": 0, "planned": 0, "generated": 0, "skipped": 0},
        "publication_metrics": {
            "published_items": 0,
            "platform_counts": {},
            "platforms": {},
            "total_engagement_score": 0.0,
            "avg_engagement_score": 0.0,
            "engagement_count": 0,
        },
        "planned_topics": 0,
        "generated_topics": 0,
        "published_items": 0,
        "avg_engagement_score": 0.0,
        "top_content": [],
        "missed_topics": [],
        "platform_split": {},
        "recommendations": ["Start with one concrete planned topic."],
    }

    @contextmanager
    def fake_script_context():
        yield MagicMock(), MagicMock()

    with patch("campaign_retrospective.script_context", fake_script_context), \
         patch("campaign_retrospective.CampaignRetrospectiveGenerator", return_value=generator), \
         patch.object(
             sys,
             "argv",
             ["campaign_retrospective.py", "7", "--json", "--top-limit", "2"],
         ):
        main()

    generator.build_report.assert_called_once_with(campaign_id=7, top_limit=2)
    data = json.loads(capsys.readouterr().out)
    assert data["campaign"]["id"] == 7
    assert data["publication_metrics"]["published_items"] == 0
