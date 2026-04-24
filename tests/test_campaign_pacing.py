"""Tests for campaign pacing reports."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from campaign_pacing import format_json_report, format_text_report
from evaluation.campaign_pacing import CampaignPacingAnalyzer


def _content(db, text: str) -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="solid",
    )


def _link_topic(db, topic_id: int, content_id: int) -> None:
    db.conn.execute(
        "UPDATE planned_topics SET content_id = ?, status = 'generated' WHERE id = ?",
        (content_id, topic_id),
    )
    db.conn.commit()


def test_campaign_pacing_reports_behind_and_recommends_scheduling_existing(db):
    campaign_id = db.create_campaign(
        name="Launch Lessons",
        start_date="2026-01-01",
        end_date="2026-01-10",
        status="active",
    )
    published_topic = db.insert_planned_topic("launch", campaign_id=campaign_id)
    ready_topic = db.insert_planned_topic("operations", campaign_id=campaign_id)
    db.insert_planned_topic("debugging", campaign_id=campaign_id)
    db.insert_planned_topic("testing", campaign_id=campaign_id)

    published_content = _content(db, "Published launch post")
    _link_topic(db, published_topic, published_content)
    db.upsert_publication_success(
        published_content,
        platform="x",
        platform_post_id="post-1",
        published_at="2026-01-02T10:00:00+00:00",
    )
    ready_content = _content(db, "Ready operations post")
    _link_topic(db, ready_topic, ready_content)

    report = CampaignPacingAnalyzer(
        db,
        now=datetime(2026, 1, 6, tzinfo=timezone.utc),
    ).report(campaign_id=campaign_id)

    assert report.status == "behind"
    assert report.expected_progress == 0.6
    assert report.actual_progress == 0.25
    assert report.recommendations[0]["action"] == "schedule_existing"
    assert report.generated_unscheduled == 1
    assert len(report.remaining_topics) == 3

    payload = json.loads(format_json_report(report))
    assert set(payload) >= {
        "status",
        "expected_progress",
        "actual_progress",
        "remaining_topics",
        "scheduled_items",
        "recommendations",
    }
    assert payload["recommendations"][0]["action"] == "schedule_existing"
    assert "schedule_existing" in format_text_report(report)


def test_campaign_pacing_reports_ahead_and_recommends_pause(db):
    campaign_id = db.create_campaign(
        name="Reliability Week",
        start_date="2026-01-01",
        end_date="2026-01-10",
        status="active",
    )
    first_topic = db.insert_planned_topic("testing", campaign_id=campaign_id)
    second_topic = db.insert_planned_topic("architecture", campaign_id=campaign_id)
    db.insert_planned_topic("observability", campaign_id=campaign_id)
    db.insert_planned_topic("release", campaign_id=campaign_id)

    first_content = _content(db, "Testing post")
    second_content = _content(db, "Architecture post")
    _link_topic(db, first_topic, first_content)
    _link_topic(db, second_topic, second_content)
    db.queue_for_publishing(
        first_content,
        scheduled_at="2026-01-03T09:00:00+00:00",
        platform="x",
    )
    db.queue_for_publishing(
        second_content,
        scheduled_at="2026-01-04T09:00:00+00:00",
        platform="bluesky",
    )

    report = CampaignPacingAnalyzer(
        db,
        now=datetime(2026, 1, 2, tzinfo=timezone.utc),
    ).report(campaign_id=campaign_id)

    assert report.status == "ahead"
    assert report.expected_progress == 0.2
    assert report.actual_progress == 0.5
    assert len(report.scheduled_items) == 2
    assert report.recommendations[0]["action"] == "pause_campaign"
    assert "pause_campaign" in format_text_report(report)


def test_campaign_pacing_no_active_campaign(db):
    report = CampaignPacingAnalyzer(db).report()

    assert report is None
    assert json.loads(format_json_report(report)) == {"error": "No campaign data found"}
    assert format_text_report(report) == "No campaign data found."


def test_campaign_pacing_handles_missing_dates_and_no_planned_topics(db):
    campaign_id = db.create_campaign(
        name="Open Ended",
        start_date=None,
        end_date=None,
        status="active",
    )

    report = CampaignPacingAnalyzer(db).report(campaign_id=campaign_id)

    assert report.status == "on_track"
    assert report.expected_progress is None
    assert report.actual_progress == 0.0
    assert report.remaining_topics == []
    assert report.scheduled_items == []
    assert report.recommendations[0]["action"] == "no_action"
