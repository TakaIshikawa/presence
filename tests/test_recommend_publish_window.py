"""Tests for recommend_publish_window.py CLI helpers."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publish_window_recommender import PublishWindowRecommendation
from recommend_publish_window import format_json_report, format_text_report, main


def _recommendation() -> PublishWindowRecommendation:
    return PublishWindowRecommendation(
        platform="x",
        start_time=datetime(2026, 4, 27, 10, tzinfo=timezone.utc),
        score=12.5,
        available=True,
        reasons=[
            "Historical x engagement for Monday 10:00 UTC is 14.00 from 3 posts.",
            "Daily cap pressure is 1/3 (1 published, 0 queued).",
        ],
        historical_score=14.0,
        historical_avg_engagement=18.0,
        historical_sample_size=3,
        historical_confidence="medium",
        cap_limit=3,
        cap_published_count=1,
        cap_queued_count=0,
        cap_pressure=0.33,
        content_type="x_thread",
    )


def test_format_text_report_includes_window_status_and_reasons():
    output = format_text_report(
        [_recommendation()],
        platform="x",
        days=7,
        limit=10,
        content_type="x_thread",
    )

    assert "Publish Window Recommendations (next 7 days, platform: x, content type: x_thread)" in output
    assert "x at 2026-04-27T10:00:00+00:00 - score 12.50, available" in output
    assert "Historical x engagement" in output
    assert "Daily cap pressure" in output


def test_format_json_report_is_stable_for_automation():
    data = json.loads(format_json_report([_recommendation()]))

    assert list(data[0].keys()) == [
        "platform",
        "start_time",
        "score",
        "available",
        "reasons",
        "historical_signal",
        "cap_pressure",
        "content_type",
    ]
    assert data[0]["platform"] == "x"
    assert data[0]["historical_signal"]["confidence"] == "medium"
    assert data[0]["cap_pressure"]["queued_count"] == 0


def test_main_supports_platform_days_json_and_content_type(capsys):
    recommender = MagicMock()
    recommender.recommend.return_value = [_recommendation()]
    config = MagicMock()
    config.publishing.daily_platform_limits = {"x": 3}

    @contextmanager
    def fake_script_context():
        yield config, MagicMock()

    argv = [
        "recommend_publish_window.py",
        "--platform",
        "x",
        "--days",
        "5",
        "--limit",
        "2",
        "--content-type",
        "x_thread",
        "--json",
    ]
    with patch.object(sys, "argv", argv):
        with patch("recommend_publish_window.script_context", fake_script_context):
            with patch(
                "recommend_publish_window.PublishWindowRecommender",
                return_value=recommender,
            ) as recommender_cls:
                main()

    recommender_cls.assert_called_once()
    assert recommender_cls.call_args.kwargs["daily_limits"] == {"x": 3}
    recommender.recommend.assert_called_once_with(
        platform="x",
        days=5,
        limit=2,
        content_type="x_thread",
    )
    output = json.loads(capsys.readouterr().out)
    assert output[0]["content_type"] == "x_thread"
