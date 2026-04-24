"""Tests for campaign pacing forecasts."""

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from campaign_forecast import format_json_report, format_text_report, main
from evaluation.campaign_forecast import CampaignForecaster


def _content(db, *, content_type="x_post", created_at=None):
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=["abc123"],
        source_messages=["msg-1"],
        content=f"{content_type} content",
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


def test_forecast_summarizes_active_campaign_counts_and_risk(db):
    now = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)
    campaign_id = db.create_campaign(
        name="Launch Arc",
        goal="Ship the launch story",
        start_date="2026-04-20",
        end_date="2026-04-30",
        status="active",
    )
    old_campaign_id = db.create_campaign(name="Completed Arc", status="completed")

    generated_topic = db.insert_planned_topic(
        topic="architecture",
        angle="boundary choices",
        target_date="2026-04-23",
        campaign_id=campaign_id,
    )
    generated_content = _content(
        db,
        content_type="x_thread",
        created_at=now - timedelta(days=2),
    )
    db.mark_planned_topic_generated(generated_topic, generated_content)
    db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (generated_content, "2026-04-26T09:00:00+00:00", "x", "queued"),
    )
    db.insert_planned_topic(
        topic="testing",
        angle="missed edge cases",
        target_date="2026-04-24",
        campaign_id=campaign_id,
    )
    db.insert_planned_topic(
        topic="debugging",
        angle="launch triage",
        target_date="2026-04-28",
        campaign_id=campaign_id,
    )
    db.insert_planned_topic(
        topic="ai-agents",
        angle="not counted",
        target_date="2026-04-27",
        campaign_id=old_campaign_id,
    )
    db.conn.commit()

    report = CampaignForecaster(db).forecast(days=7, now=now)

    assert len(report.campaigns) == 1
    forecast = report.campaigns[0]
    assert forecast.campaign["id"] == campaign_id
    assert forecast.planned_count == 3
    assert forecast.generated_count == 1
    assert forecast.queued_count == 1
    assert forecast.overdue_count == 1
    assert forecast.remaining_count == 2
    assert forecast.days_remaining == 5
    assert forecast.estimated_generation_rate == round(1 / 7, 3)
    assert forecast.required_generation_rate == 0.4
    assert forecast.miss_risk == "at_risk"
    assert forecast.recommendation.topic == "testing"
    assert forecast.recommendation.content_type == "x_thread"


def test_forecast_flags_likely_miss_when_rate_is_too_low(db):
    now = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)
    campaign_id = db.create_campaign(
        name="Sparse Arc",
        end_date="2026-04-27",
        status="active",
    )
    for index in range(3):
        db.insert_planned_topic(
            topic=f"topic-{index}",
            target_date=f"2026-04-2{index + 5}",
            campaign_id=campaign_id,
        )

    forecast = CampaignForecaster(db).forecast(days=7, now=now).campaigns[0]

    assert forecast.remaining_count == 3
    assert forecast.days_remaining == 2
    assert forecast.estimated_generation_rate == 0.0
    assert forecast.miss_risk == "likely_miss"


def test_campaign_id_filter_limits_report_and_missing_id_errors(db):
    now = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)
    first_id = db.create_campaign(name="First", end_date="2026-04-30", status="active")
    second_id = db.create_campaign(name="Second", end_date="2026-05-01", status="active")
    db.insert_planned_topic(topic="architecture", campaign_id=first_id)
    db.insert_planned_topic(topic="testing", campaign_id=second_id)

    report = CampaignForecaster(db).forecast(campaign_id=second_id, now=now)

    assert [forecast.campaign["id"] for forecast in report.campaigns] == [second_id]
    with pytest.raises(ValueError, match="Campaign 999 does not exist"):
        CampaignForecaster(db).forecast(campaign_id=999, now=now)


def test_json_and_text_include_same_campaign_ids_and_recommendations(db):
    now = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)
    campaign_id = db.create_campaign(name="Parity Arc", end_date="2026-04-30", status="active")
    planned_id = db.insert_planned_topic(
        topic="product-thinking",
        angle="prioritize the sharp edge",
        target_date="2026-04-26",
        campaign_id=campaign_id,
    )

    report = CampaignForecaster(db).forecast(days=7, now=now)
    data = json.loads(format_json_report(report))
    text = format_text_report(report)

    assert data["campaigns"][0]["campaign"]["id"] == campaign_id
    assert f"ID {campaign_id}" in text
    recommendation = data["campaigns"][0]["recommendation"]
    assert recommendation["planned_topic_id"] == planned_id
    assert recommendation["topic"] == "product-thinking"
    assert recommendation["content_type"] == "x_post"
    assert f"Planned topic ID: {planned_id}" in text
    assert "Topic: product-thinking" in text
    assert "Content type: x_post" in text


def test_main_writes_output_and_reports_missing_campaign(capsys, tmp_path):
    report = MagicMock()
    report.to_dict.return_value = {"campaigns": []}
    forecaster = MagicMock()
    forecaster.forecast.return_value = report

    @contextmanager
    def fake_script_context():
        yield MagicMock(), MagicMock()

    output_path = tmp_path / "forecast.json"
    with patch("campaign_forecast.script_context", fake_script_context), \
         patch("campaign_forecast.CampaignForecaster", return_value=forecaster):
        main(["--campaign-id", "7", "--days", "5", "--json", "--output", str(output_path)])

    forecaster.forecast.assert_called_once_with(campaign_id=7, days=5)
    assert json.loads(output_path.read_text()) == {"campaigns": []}
    assert capsys.readouterr().out == ""

    failing = MagicMock()
    failing.forecast.side_effect = ValueError("Campaign 99 does not exist")
    with patch("campaign_forecast.script_context", fake_script_context), \
         patch("campaign_forecast.CampaignForecaster", return_value=failing), \
         pytest.raises(SystemExit) as exc:
        main(["--campaign-id", "99"])

    assert exc.value.code == 2
    assert "Campaign 99 does not exist" in capsys.readouterr().err
