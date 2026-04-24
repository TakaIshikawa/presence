"""Tests for campaign topic saturation reporting."""

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from campaign_saturation import format_json_report, format_text_report, main
from evaluation.campaign_saturation import (
    CampaignSaturationAnalyzer,
    CampaignSaturationReport,
    CampaignTopicSaturationRow,
)


def _content(db, text: str, created_at: datetime | None = None) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["abc123"],
        source_messages=["msg-1"],
        content=text,
        eval_score=8.0,
        eval_feedback="Good",
    )
    if created_at is not None:
        db.conn.execute(
            "UPDATE generated_content SET created_at = ? WHERE id = ?",
            (created_at.isoformat(), content_id),
        )
        db.conn.commit()
    return content_id


def _publish(db, content_id: int, when: datetime, platform: str = "x") -> None:
    db.upsert_publication_success(
        content_id,
        platform,
        platform_post_id=f"{platform}-{content_id}",
        platform_url=f"https://example.com/{platform}/{content_id}",
        published_at=when.isoformat(),
    )


def _planned_content(
    db,
    campaign_id: int,
    topic: str,
    when: datetime,
    *,
    publish: bool = False,
    platform: str = "x",
) -> int:
    planned_id = db.insert_planned_topic(topic=topic, campaign_id=campaign_id)
    content_id = _content(db, f"{topic} post", created_at=when)
    db.mark_planned_topic_generated(planned_id, content_id)
    if publish:
        _publish(db, content_id, when + timedelta(hours=1), platform=platform)
    return content_id


def test_campaign_saturation_handles_campaign_with_no_publications(db):
    campaign_id = db.create_campaign(name="Quiet Campaign", status="active")
    db.insert_planned_topic(topic="architecture", campaign_id=campaign_id)
    db.insert_planned_topic(topic="testing", campaign_id=campaign_id)

    report = CampaignSaturationAnalyzer(db).report(
        campaign_id=campaign_id,
        days=30,
        min_published=2,
    )

    assert report is not None
    assert [row.topic for row in report.topics] == ["architecture", "testing"]
    assert all(row.published_count == 0 for row in report.topics)
    assert all(row.saturation_level == "low" for row in report.topics)
    assert all(row.recommendation == "continue" for row in report.topics)
    assert report.summary["low"] == 2


def test_campaign_saturation_reports_balanced_topic_coverage(db):
    now = datetime.now(timezone.utc)
    campaign_id = db.create_campaign(name="Balanced Campaign", status="active")
    _planned_content(
        db,
        campaign_id,
        "architecture",
        now - timedelta(days=1),
        publish=True,
    )
    _planned_content(
        db,
        campaign_id,
        "testing",
        now - timedelta(days=2),
        publish=True,
        platform="bluesky",
    )

    report = CampaignSaturationAnalyzer(db).report(
        campaign_id=campaign_id,
        days=14,
        min_published=2,
    )

    assert report is not None
    counts = [
        (row.topic, row.planned_count, row.generated_count, row.published_count)
        for row in report.topics
    ]
    assert counts == [
        ("architecture", 1, 1, 1),
        ("testing", 1, 1, 1),
    ]
    assert all(row.saturation_level == "balanced" for row in report.topics)
    assert all(row.recommendation == "continue" for row in report.topics)


def test_campaign_saturation_pauses_repeated_over_covered_topics(db):
    now = datetime.now(timezone.utc)
    campaign_id = db.create_campaign(name="Repeated Campaign", status="active")
    first = _planned_content(
        db,
        campaign_id,
        "ai-agents",
        now - timedelta(days=3),
        publish=True,
    )
    _publish(db, first, now - timedelta(days=2, hours=20), platform="bluesky")
    _planned_content(db, campaign_id, "ai-agents", now - timedelta(days=2), publish=True)
    _planned_content(db, campaign_id, "ai-agents", now - timedelta(days=1), publish=True)
    db.insert_planned_topic(topic="testing", campaign_id=campaign_id)

    report = CampaignSaturationAnalyzer(db).report(
        campaign_id=campaign_id,
        days=14,
        min_published=3,
    )

    assert report is not None
    rows = {row.topic: row for row in report.topics}
    assert rows["ai-agents"].planned_count == 3
    assert rows["ai-agents"].generated_count == 3
    assert rows["ai-agents"].published_count == 3
    assert rows["ai-agents"].last_published_at is not None
    assert rows["ai-agents"].saturation_level == "high"
    assert rows["ai-agents"].recommendation == "pause"


def test_campaign_saturation_active_campaign_resolution(db):
    now = datetime.now(timezone.utc)
    active_id = db.create_campaign(
        name="Active Campaign",
        start_date=(now - timedelta(days=1)).date().isoformat(),
        end_date=(now + timedelta(days=7)).date().isoformat(),
        status="active",
    )
    db.create_campaign(name="Completed Campaign", status="completed")
    _planned_content(db, active_id, "architecture", now - timedelta(hours=2), publish=True)

    report = CampaignSaturationAnalyzer(db).report(active=True, days=7)

    assert report is not None
    assert report.campaign["id"] == active_id
    assert report.topics[0].topic == "architecture"


def test_campaign_saturation_json_is_stable_and_monitoring_friendly():
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    report = CampaignSaturationReport(
        campaign={"id": 7, "name": "Launch", "status": "active"},
        period_days=14,
        period_start=now - timedelta(days=14),
        period_end=now,
        min_published=2,
        topics=[
            CampaignTopicSaturationRow(
                topic="architecture",
                planned_count=1,
                generated_count=1,
                published_count=1,
                last_published_at="2026-04-21T10:00:00+00:00",
                saturation_level="balanced",
                recommendation="continue",
            )
        ],
        summary={
            "topic_count": 1,
            "low": 0,
            "balanced": 1,
            "high": 0,
            "pause": 0,
            "diversify": 0,
            "continue": 1,
        },
    )

    payload = json.loads(format_json_report(report))

    assert payload["campaign"]["id"] == 7
    assert payload["period_start"] == "2026-04-08T12:00:00+00:00"
    assert payload["topics"][0] == {
        "generated_count": 1,
        "last_published_at": "2026-04-21T10:00:00+00:00",
        "planned_count": 1,
        "published_count": 1,
        "recommendation": "continue",
        "saturation_level": "balanced",
        "topic": "architecture",
    }


def test_campaign_saturation_text_report_includes_required_columns():
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    report = CampaignSaturationReport(
        campaign={"id": 7, "name": "Launch", "status": "active"},
        period_days=14,
        period_start=now - timedelta(days=14),
        period_end=now,
        min_published=2,
        topics=[
            CampaignTopicSaturationRow(
                topic="architecture",
                planned_count=1,
                generated_count=1,
                published_count=1,
                last_published_at="2026-04-21T10:00:00+00:00",
                saturation_level="balanced",
                recommendation="continue",
            )
        ],
        summary={},
    )

    output = format_text_report(report)

    assert "Campaign Saturation: Launch" in output
    assert "Plan" in output
    assert "Gen" in output
    assert "Pub" in output
    assert "Last Published" in output
    assert "balanced" in output
    assert "continue" in output


def test_campaign_saturation_main_uses_campaign_id_json_and_min_published(capsys):
    analyzer = MagicMock()
    analyzer.report.return_value = None

    @contextmanager
    def fake_script_context():
        yield MagicMock(), MagicMock()

    with patch("campaign_saturation.script_context", fake_script_context), \
         patch("campaign_saturation.CampaignSaturationAnalyzer", return_value=analyzer), \
         patch.object(
             sys,
             "argv",
             [
                 "campaign_saturation.py",
                 "--campaign-id",
                 "7",
                 "--days",
                 "14",
                 "--min-published",
                 "2",
                 "--json",
             ],
         ):
        main()

    analyzer.report.assert_called_once_with(
        campaign_id=7,
        active=False,
        days=14,
        min_published=2,
    )
    assert json.loads(capsys.readouterr().out) == {"error": "No campaign data found"}


def test_campaign_saturation_main_defaults_to_active(capsys):
    analyzer = MagicMock()
    analyzer.report.return_value = None

    @contextmanager
    def fake_script_context():
        yield MagicMock(), MagicMock()

    with patch("campaign_saturation.script_context", fake_script_context), \
         patch("campaign_saturation.CampaignSaturationAnalyzer", return_value=analyzer), \
         patch.object(sys, "argv", ["campaign_saturation.py"]):
        main()

    analyzer.report.assert_called_once_with(
        campaign_id=None,
        active=True,
        days=30,
        min_published=3,
    )
    assert "No campaign data found." in capsys.readouterr().out
