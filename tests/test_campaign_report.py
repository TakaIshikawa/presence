"""Tests for campaign_report.py."""

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from campaign_report import format_json_report, format_text_report, main
from evaluation.pipeline_analytics import CampaignPerformanceReport


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
