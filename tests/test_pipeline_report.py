"""Tests for pipeline_report.py — CLI entry point for pipeline analytics."""

import sys
import json
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock
from argparse import Namespace

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipeline_report import main, format_text_report, format_json_report
from evaluation.pipeline_analytics import PipelineHealthReport


# --- fixtures ---


@pytest.fixture
def mock_args():
    """Default args for format functions."""
    return Namespace(
        days=30,
        content_type="x_thread",
        format="text"
    )


@pytest.fixture
def mock_full_report():
    """Full report with all data populated."""
    return PipelineHealthReport(
        period_start=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        period_end=datetime(2025, 1, 31, 23, 59, 59, tzinfo=timezone.utc),
        total_runs=100,
        outcomes={
            "published": 60,
            "below_threshold": 25,
            "all_filtered": 15,
        },
        conversion_rate=0.6,
        avg_final_score=7.5,
        avg_candidates_per_run=3.2,
        filter_breakdown={
            "repetition_rejected": 45,
            "stale_pattern_rejected": 30,
            "low_quality_rejected": 20,
        },
        score_distribution={
            "0-3": 5,
            "3-5": 15,
            "5-7": 30,
            "7-9": 35,
            "9-10": 15,
        },
        refinement_stats={
            "total_refined": 50,
            "picked_refined": 35,
            "picked_original": 15,
        },
        avg_engagement_by_score_band={
            "0-3": 2.5,
            "3-5": 4.0,
            "5-7": 6.5,
            "7-9": 8.2,
            "9-10": 9.5,
        },
    )


@pytest.fixture
def mock_weekly_trends():
    """Mock weekly trend data."""
    return [
        {
            "week": "2025-01",
            "runs": 25,
            "published": 15,
            "conversion_rate": 60.0,
            "avg_score": 7.5,
            "avg_engagement": 8.0,
        },
        {
            "week": "2025-02",
            "runs": 30,
            "published": 18,
            "conversion_rate": 60.0,
            "avg_score": 7.8,
            "avg_engagement": 8.3,
        },
        {
            "week": "2025-03",
            "runs": 28,
            "published": 20,
            "conversion_rate": 71.4,
            "avg_score": 8.0,
            "avg_engagement": 8.5,
        },
    ]


@pytest.fixture
def mock_analytics_full(mock_full_report, mock_weekly_trends):
    """Mock PipelineAnalytics with full data."""
    analytics = MagicMock()
    analytics.health_report.return_value = mock_full_report
    analytics.trend.return_value = mock_weekly_trends
    analytics.filter_effectiveness.return_value = {
        "repetition_rejected": {"count": 45, "percentage": 47.4},
        "stale_pattern_rejected": {"count": 30, "percentage": 31.6},
    }
    analytics.score_engagement_correlation.return_value = [
        {
            "content_id": 1,
            "eval_score": 8.5,
            "engagement_score": 9.0,
            "content_type": "x_thread",
            "published_at": "2025-01-15T12:00:00Z",
        },
        {
            "content_id": 2,
            "eval_score": 7.0,
            "engagement_score": 7.5,
            "content_type": "x_thread",
            "published_at": "2025-01-14T10:00:00Z",
        },
    ]
    return analytics


@pytest.fixture
def mock_analytics_empty():
    """Mock PipelineAnalytics with no data."""
    analytics = MagicMock()
    analytics.health_report.return_value = None
    return analytics


# --- TestFormatTextReport ---


class TestFormatTextReport:
    """Test text report formatting function."""

    def test_no_data_message(self, mock_analytics_empty, mock_args):
        """Returns 'No pipeline data found' message when analytics returns None."""
        output = format_text_report(mock_analytics_empty, mock_args)

        assert "No pipeline data found for x_thread in last 30 days." in output

    def test_includes_all_sections(self, mock_analytics_full, mock_args):
        """Includes all sections when full report data is present."""
        output = format_text_report(mock_analytics_full, mock_args)

        # Overview section
        assert "Pipeline Health Report (last 30 days)" in output
        assert "Content type:  x_thread" in output
        assert "Period:        2025-01-01 to 2025-01-31" in output
        assert "Total runs:    100" in output

        # Outcomes section
        assert "Outcomes:" in output
        assert "published" in output
        assert "below_threshold" in output
        assert "all_filtered" in output

        # Key metrics section
        assert "Key Metrics:" in output
        assert "Conversion rate:" in output
        assert "Avg final score:" in output
        assert "Avg candidates/run:" in output

        # Filter effectiveness section
        assert "Filter Effectiveness:" in output
        assert "repetition_rejected" in output

        # Score distribution section
        assert "Score Distribution:" in output
        assert "0-3" in output
        assert "9-10" in output

        # Refinement stats section
        assert "Refinement:" in output
        assert "Total refined:" in output
        assert "Picked refined:" in output

        # Engagement correlation section
        assert "Score vs Engagement Correlation:" in output

        # Weekly trends section
        assert "Weekly Trends (last 8 weeks):" in output
        assert "Week" in output
        assert "Runs" in output
        assert "Published" in output

    def test_outcome_percentages(self, mock_analytics_full, mock_args):
        """Correctly formats percentages for outcomes (count/total_runs*100)."""
        output = format_text_report(mock_analytics_full, mock_args)

        # published: 60/100 = 60%
        assert "published" in output
        assert "60 ( 60.0%)" in output

        # below_threshold: 25/100 = 25%
        assert "below_threshold" in output
        assert "25 ( 25.0%)" in output

        # all_filtered: 15/100 = 15%
        assert "all_filtered" in output
        assert "15 ( 15.0%)" in output

    def test_empty_filter_breakdown(self, mock_analytics_full, mock_args):
        """Handles empty filter_breakdown gracefully."""
        # Modify the report to have empty filter_breakdown
        empty_filter_report = PipelineHealthReport(
            period_start=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            period_end=datetime(2025, 1, 31, 23, 59, 59, tzinfo=timezone.utc),
            total_runs=100,
            outcomes={"published": 60},
            conversion_rate=0.6,
            avg_final_score=7.5,
            avg_candidates_per_run=3.2,
            filter_breakdown={},  # Empty
            score_distribution={"0-3": 0, "3-5": 0, "5-7": 0, "7-9": 0, "9-10": 0},
            refinement_stats={"total_refined": 0, "picked_refined": 0, "picked_original": 0},
            avg_engagement_by_score_band={"0-3": 0, "3-5": 0, "5-7": 0, "7-9": 0, "9-10": 0},
        )

        analytics = MagicMock()
        analytics.health_report.return_value = empty_filter_report
        analytics.trend.return_value = []

        output = format_text_report(analytics, mock_args)

        # Filter effectiveness section should not appear
        assert "Filter Effectiveness:" not in output

    def test_zero_refinement_stats(self, mock_analytics_full, mock_args):
        """Handles zero refinement stats (total_refined=0 skips refinement section)."""
        # Modify the report to have zero refinement stats
        zero_refinement_report = PipelineHealthReport(
            period_start=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            period_end=datetime(2025, 1, 31, 23, 59, 59, tzinfo=timezone.utc),
            total_runs=100,
            outcomes={"published": 60},
            conversion_rate=0.6,
            avg_final_score=7.5,
            avg_candidates_per_run=3.2,
            filter_breakdown={"repetition_rejected": 10},
            score_distribution={"0-3": 5, "3-5": 15, "5-7": 30, "7-9": 35, "9-10": 15},
            refinement_stats={"total_refined": 0, "picked_refined": 0, "picked_original": 0},
            avg_engagement_by_score_band={"0-3": 2.5, "3-5": 4.0, "5-7": 6.5, "7-9": 8.2, "9-10": 9.5},
        )

        analytics = MagicMock()
        analytics.health_report.return_value = zero_refinement_report
        analytics.trend.return_value = []

        output = format_text_report(analytics, mock_args)

        # Refinement section should not appear
        assert "Refinement:" not in output

    def test_all_zero_engagement_bands(self, mock_analytics_full, mock_args):
        """Handles engagement bands with all-zero values (skips correlation section)."""
        # Modify the report to have all-zero engagement bands
        zero_engagement_report = PipelineHealthReport(
            period_start=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            period_end=datetime(2025, 1, 31, 23, 59, 59, tzinfo=timezone.utc),
            total_runs=100,
            outcomes={"published": 60},
            conversion_rate=0.6,
            avg_final_score=7.5,
            avg_candidates_per_run=3.2,
            filter_breakdown={"repetition_rejected": 10},
            score_distribution={"0-3": 5, "3-5": 15, "5-7": 30, "7-9": 35, "9-10": 15},
            refinement_stats={"total_refined": 50, "picked_refined": 35, "picked_original": 15},
            avg_engagement_by_score_band={"0-3": 0.0, "3-5": 0.0, "5-7": 0.0, "7-9": 0.0, "9-10": 0.0},
        )

        analytics = MagicMock()
        analytics.health_report.return_value = zero_engagement_report
        analytics.trend.return_value = []

        output = format_text_report(analytics, mock_args)

        # Engagement correlation section should not appear
        assert "Score vs Engagement Correlation:" not in output

    def test_filter_breakdown_sorting(self, mock_analytics_full, mock_args):
        """Filter breakdown is sorted by count descending."""
        output = format_text_report(mock_analytics_full, mock_args)

        # Find the filter effectiveness section
        lines = output.split('\n')
        filter_section_start = None
        for i, line in enumerate(lines):
            if "Filter Effectiveness:" in line:
                filter_section_start = i
                break

        assert filter_section_start is not None

        # Extract filter lines (next 3 lines after header)
        filter_lines = []
        for i in range(filter_section_start + 1, filter_section_start + 4):
            if i < len(lines) and "rejected" in lines[i]:
                filter_lines.append(lines[i])

        # Check that repetition_rejected (45) comes before stale_pattern_rejected (30)
        assert any("repetition_rejected" in line for line in filter_lines)
        assert any("stale_pattern_rejected" in line for line in filter_lines)

        # Verify order by finding indices
        repetition_idx = next(i for i, line in enumerate(filter_lines) if "repetition_rejected" in line)
        stale_idx = next(i for i, line in enumerate(filter_lines) if "stale_pattern_rejected" in line)
        assert repetition_idx < stale_idx

    def test_key_metrics_formatting(self, mock_analytics_full, mock_args):
        """Key metrics are formatted correctly."""
        output = format_text_report(mock_analytics_full, mock_args)

        # Conversion rate: 0.6 * 100 = 60.0%
        assert "Conversion rate:        60.0%" in output

        # Avg final score: 7.5/10
        assert "Avg final score:         7.5/10" in output

        # Avg candidates/run: 3.2
        assert "Avg candidates/run:      3.2" in output

    def test_refinement_percentages(self, mock_analytics_full, mock_args):
        """Refinement stats show correct percentages."""
        output = format_text_report(mock_analytics_full, mock_args)

        # total_refined: 50
        # picked_refined: 35 -> 35/50 = 70%
        # picked_original: 15 -> 15/50 = 30%
        assert "Total refined:         50" in output
        assert "Picked refined:          35 ( 70.0%)" in output
        assert "Kept original:           15 ( 30.0%)" in output


# --- TestFormatJsonReport ---


class TestFormatJsonReport:
    """Test JSON report formatting function."""

    def test_error_json_when_no_data(self, mock_analytics_empty, mock_args):
        """Returns error JSON when analytics returns None."""
        output = format_json_report(mock_analytics_empty, mock_args)
        data = json.loads(output)

        assert data == {"error": "No data found"}

    def test_valid_json_with_all_keys(self, mock_analytics_full, mock_args):
        """Output is valid JSON with all expected keys."""
        output = format_json_report(mock_analytics_full, mock_args)
        data = json.loads(output)

        # Verify all expected keys are present
        assert "content_type" in data
        assert "days" in data
        assert "period_start" in data
        assert "period_end" in data
        assert "total_runs" in data
        assert "outcomes" in data
        assert "conversion_rate" in data
        assert "avg_final_score" in data
        assert "avg_candidates_per_run" in data
        assert "filter_breakdown" in data
        assert "score_distribution" in data
        assert "refinement_stats" in data
        assert "avg_engagement_by_score_band" in data
        assert "weekly_trends" in data
        assert "filter_effectiveness" in data
        assert "score_engagement_correlation" in data

    def test_values_properly_rounded(self, mock_analytics_full, mock_args):
        """Values are properly rounded (conversion_rate to 3 decimals, scores to 2)."""
        output = format_json_report(mock_analytics_full, mock_args)
        data = json.loads(output)

        # conversion_rate: 0.6 -> 3 decimals
        assert data["conversion_rate"] == 0.6
        assert isinstance(data["conversion_rate"], float)

        # avg_final_score: 7.5 -> 2 decimals
        assert data["avg_final_score"] == 7.5
        assert isinstance(data["avg_final_score"], float)

        # avg_candidates_per_run: 3.2 -> 2 decimals
        assert data["avg_candidates_per_run"] == 3.2

        # avg_engagement_by_score_band values -> 2 decimals each
        for band, value in data["avg_engagement_by_score_band"].items():
            # Check that values are rounded to 2 decimals
            assert isinstance(value, float)
            # Verify no more than 2 decimal places
            assert len(str(value).split('.')[-1]) <= 2 or value == int(value)

    def test_weekly_trends_limited_to_8_weeks(self, mock_analytics_full, mock_args):
        """Weekly trends limited to 8 weeks."""
        # Create analytics with exactly 8 weeks
        eight_weeks = [
            {"week": f"2025-{i:02d}", "runs": 10, "published": 5,
             "conversion_rate": 50.0, "avg_score": 7.0, "avg_engagement": 8.0}
            for i in range(1, 9)
        ]

        analytics = MagicMock()
        analytics.health_report.return_value = mock_analytics_full.health_report.return_value
        analytics.trend.return_value = eight_weeks
        analytics.filter_effectiveness.return_value = {}
        analytics.score_engagement_correlation.return_value = []

        output = format_json_report(analytics, mock_args)
        data = json.loads(output)

        # Verify trend was called with weeks=8
        analytics.trend.assert_called_once_with(content_type="x_thread", weeks=8)

        # Verify the data has 8 weeks
        assert len(data["weekly_trends"]) == 8

    def test_score_engagement_correlation_limited_to_20(self, mock_analytics_full, mock_args):
        """Score_engagement_correlation limited to 20 items."""
        # Create analytics with 25 correlation items
        many_correlations = [
            {
                "content_id": i,
                "eval_score": 7.5,
                "engagement_score": 8.0,
                "content_type": "x_thread",
                "published_at": "2025-01-15T12:00:00Z",
            }
            for i in range(1, 26)
        ]

        analytics = MagicMock()
        analytics.health_report.return_value = mock_analytics_full.health_report.return_value
        analytics.trend.return_value = []
        analytics.filter_effectiveness.return_value = {}
        analytics.score_engagement_correlation.return_value = many_correlations

        output = format_json_report(analytics, mock_args)
        data = json.loads(output)

        # Should be limited to 20
        assert len(data["score_engagement_correlation"]) == 20

    def test_json_structure_matches_report(self, mock_analytics_full, mock_args):
        """JSON structure matches the report data."""
        output = format_json_report(mock_analytics_full, mock_args)
        data = json.loads(output)

        assert data["content_type"] == "x_thread"
        assert data["days"] == 30
        assert data["total_runs"] == 100
        assert data["outcomes"]["published"] == 60
        assert data["outcomes"]["below_threshold"] == 25
        assert data["outcomes"]["all_filtered"] == 15
        assert data["filter_breakdown"]["repetition_rejected"] == 45
        assert data["score_distribution"]["0-3"] == 5
        assert data["score_distribution"]["9-10"] == 15
        assert data["refinement_stats"]["total_refined"] == 50
        assert data["refinement_stats"]["picked_refined"] == 35
        assert data["refinement_stats"]["picked_original"] == 15


# --- TestMainFunction ---


class TestMainFunction:
    """Test the main CLI entry point."""

    def test_default_arguments(self, mock_analytics_full, capsys):
        """Default args: --days=30, --content-type=x_thread, --format=text."""
        with patch("pipeline_report.script_context") as mock_context, \
             patch("sys.argv", ["pipeline_report.py"]):

            mock_db = MagicMock()
            mock_config = MagicMock()
            mock_context.return_value.__enter__ = lambda self: (mock_config, mock_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            with patch("pipeline_report.PipelineAnalytics") as mock_analytics_cls:
                mock_analytics_cls.return_value = mock_analytics_full
                main()

        captured = capsys.readouterr()

        # Should produce text output (not JSON)
        assert "Pipeline Health Report" in captured.out
        assert "Content type:  x_thread" in captured.out

        # Verify analytics was called with default parameters
        mock_analytics_full.health_report.assert_called_once_with(
            content_type="x_thread",
            days=30
        )

    def test_json_format_output(self, mock_analytics_full, capsys):
        """JSON format output."""
        with patch("pipeline_report.script_context") as mock_context, \
             patch("sys.argv", ["pipeline_report.py", "--format", "json"]):

            mock_db = MagicMock()
            mock_config = MagicMock()
            mock_context.return_value.__enter__ = lambda self: (mock_config, mock_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            with patch("pipeline_report.PipelineAnalytics") as mock_analytics_cls:
                mock_analytics_cls.return_value = mock_analytics_full
                main()

        captured = capsys.readouterr()

        # Should be valid JSON
        data = json.loads(captured.out)
        assert "content_type" in data
        assert "total_runs" in data
        assert "weekly_trends" in data

        # Should not contain text report formatting
        assert "Pipeline Health Report" not in captured.out

    def test_custom_days_and_content_type(self, mock_analytics_full, capsys):
        """Custom --days and --content-type values passed correctly."""
        with patch("pipeline_report.script_context") as mock_context, \
             patch("sys.argv", ["pipeline_report.py", "--days", "60", "--content-type", "x_post"]):

            mock_db = MagicMock()
            mock_config = MagicMock()
            mock_context.return_value.__enter__ = lambda self: (mock_config, mock_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            with patch("pipeline_report.PipelineAnalytics") as mock_analytics_cls:
                mock_analytics_cls.return_value = mock_analytics_full
                main()

        captured = capsys.readouterr()

        # Verify report was generated
        assert "Pipeline Health Report (last 60 days)" in captured.out
        assert "Content type:  x_post" in captured.out

        # Verify analytics was called with custom parameters
        mock_analytics_full.health_report.assert_called_once_with(
            content_type="x_post",
            days=60
        )

    def test_logging_level_is_warning(self, mock_analytics_full, capsys):
        """Logging level is set to WARNING for cleaner output."""
        with patch("pipeline_report.script_context") as mock_context, \
             patch("sys.argv", ["pipeline_report.py"]), \
             patch("pipeline_report.logging.basicConfig") as mock_logging:

            mock_db = MagicMock()
            mock_config = MagicMock()
            mock_context.return_value.__enter__ = lambda self: (mock_config, mock_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            with patch("pipeline_report.PipelineAnalytics") as mock_analytics_cls:
                mock_analytics_cls.return_value = mock_analytics_full
                main()

        # Verify logging was configured with WARNING level
        mock_logging.assert_called_once()
        call_kwargs = mock_logging.call_args[1]
        import logging
        assert call_kwargs["level"] == logging.WARNING

    def test_script_context_called(self, mock_analytics_full):
        """script_context is properly used."""
        with patch("pipeline_report.script_context") as mock_context, \
             patch("sys.argv", ["pipeline_report.py"]):

            mock_db = MagicMock()
            mock_config = MagicMock()
            mock_context.return_value.__enter__ = lambda self: (mock_config, mock_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            with patch("pipeline_report.PipelineAnalytics") as mock_analytics_cls:
                mock_analytics_cls.return_value = mock_analytics_full
                main()

        # Verify context manager was used
        mock_context.assert_called_once()

    def test_pipeline_analytics_initialized_with_db(self, mock_analytics_full):
        """PipelineAnalytics is initialized with the database."""
        mock_db = MagicMock()

        with patch("pipeline_report.script_context") as mock_context, \
             patch("sys.argv", ["pipeline_report.py"]), \
             patch("pipeline_report.PipelineAnalytics") as mock_analytics_cls:

            mock_config = MagicMock()
            mock_context.return_value.__enter__ = lambda self: (mock_config, mock_db)
            mock_context.return_value.__exit__ = lambda self, *args: None
            mock_analytics_cls.return_value = mock_analytics_full

            main()

        # Verify PipelineAnalytics was instantiated with the db
        mock_analytics_cls.assert_called_once_with(mock_db)
