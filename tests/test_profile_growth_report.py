"""Tests for profile_growth_report.py CLI formatting."""

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.pipeline_analytics import PlatformGrowthStats, ProfileGrowthReport
from profile_growth_report import format_json_report, format_text_report, main


def sample_report() -> ProfileGrowthReport:
    """Build a profile growth report fixture."""
    return ProfileGrowthReport(
        period_days=14,
        period_start=datetime(2026, 4, 1, tzinfo=timezone.utc),
        period_end=datetime(2026, 4, 15, tzinfo=timezone.utc),
        platforms={
            "x": PlatformGrowthStats(
                platform="x",
                start_followers=100,
                end_followers=112,
                follower_delta=12,
                follower_delta_pct=12.0,
                start_following=50,
                end_following=52,
                following_delta=2,
                start_post_count=300,
                end_post_count=305,
                profile_post_delta=5,
                posting_volume=4,
                engagement_count=3,
                avg_engagement_score=18.5,
                min_engagement_score=10.0,
                max_engagement_score=30.0,
                total_engagement_score=55.5,
            ),
            "bluesky": PlatformGrowthStats(
                platform="bluesky",
                start_followers=40,
                end_followers=46,
                follower_delta=6,
                follower_delta_pct=15.0,
                start_following=20,
                end_following=21,
                following_delta=1,
                start_post_count=80,
                end_post_count=82,
                profile_post_delta=2,
                posting_volume=2,
                engagement_count=1,
                avg_engagement_score=22.0,
                min_engagement_score=22.0,
                max_engagement_score=22.0,
                total_engagement_score=22.0,
            ),
        },
    )


def test_format_text_report_includes_growth_activity_and_engagement():
    output = format_text_report(sample_report())

    assert "Profile Growth Report (last 14 days)" in output
    assert "X" in output
    assert "Followers: 100 -> 112 (+12, +12.0%)" in output
    assert "Profile posts: 300 -> 305 (+5)" in output
    assert "Published volume: 4" in output
    assert "Engagement score: avg 18.50, min 10.00, max 30.00, total 55.50 (3 posts)" in output
    assert "BLUESKY" in output


def test_format_json_report_serializes_datetimes_and_platform_stats():
    data = json.loads(format_json_report(sample_report()))

    assert data["period_days"] == 14
    assert data["period_start"] == "2026-04-01T00:00:00+00:00"
    assert data["platforms"]["x"]["follower_delta"] == 12
    assert data["platforms"]["bluesky"]["posting_volume"] == 2


def test_main_supports_platform_and_json_flags(capsys):
    report = sample_report()
    analytics = MagicMock()
    analytics.profile_growth_report.return_value = report

    @contextmanager
    def fake_script_context():
        yield None, MagicMock()

    with patch.object(sys, "argv", ["profile_growth_report.py", "--days", "14", "--platform", "bluesky", "--json"]):
        with patch("profile_growth_report.script_context", fake_script_context):
            with patch("profile_growth_report.PipelineAnalytics", return_value=analytics):
                main()

    analytics.profile_growth_report.assert_called_once_with(days=14, platform="bluesky")
    output = json.loads(capsys.readouterr().out)
    assert output["platforms"]["bluesky"]["follower_delta"] == 6
