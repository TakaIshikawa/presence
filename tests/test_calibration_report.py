"""Tests for calibration_report.py — CLI entry point for prediction calibration."""

import sys
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from calibration_report import main, format_report, format_json
from storage.db import Database
from evaluation.prediction_calibrator import CalibrationReport, ErrorPattern


# --- fixtures ---


@pytest.fixture
def test_db(tmp_path):
    """Create temporary SQLite database with schema."""
    db_path = tmp_path / "test_presence.db"
    db = Database(str(db_path))
    db.connect()
    schema_path = Path(__file__).parent.parent / "schema.sql"
    db.init_schema(str(schema_path))
    yield db
    db.close()


@pytest.fixture
def populated_db(test_db):
    """Database with prediction and engagement data for testing."""
    now = datetime.now(timezone.utc)

    # Create content items with predictions and actuals
    predictions_data = [
        # Well-calibrated predictions
        {
            "predicted": 7.0,
            "actual": 7.5,
            "hook": 8.0,
            "specificity": 7.0,
            "emotional_resonance": 6.0,
            "novelty": 7.0,
            "actionability": 7.0,
            "days_ago": 1,
        },
        {
            "predicted": 5.0,
            "actual": 5.5,
            "hook": 6.0,
            "specificity": 5.0,
            "emotional_resonance": 5.0,
            "novelty": 4.0,
            "actionability": 5.0,
            "days_ago": 2,
        },
        # Overestimated predictions
        {
            "predicted": 8.0,
            "actual": 4.0,
            "hook": 9.0,
            "specificity": 8.0,
            "emotional_resonance": 7.0,
            "novelty": 8.0,
            "actionability": 8.0,
            "days_ago": 3,
        },
        {
            "predicted": 9.0,
            "actual": 5.0,
            "hook": 9.5,
            "specificity": 9.0,
            "emotional_resonance": 8.0,
            "novelty": 9.0,
            "actionability": 9.0,
            "days_ago": 4,
        },
        # Underestimated predictions
        {
            "predicted": 4.0,
            "actual": 8.0,
            "hook": 4.0,
            "specificity": 4.0,
            "emotional_resonance": 4.0,
            "novelty": 3.0,
            "actionability": 4.0,
            "days_ago": 5,
        },
        # Low score band (0-3)
        {
            "predicted": 2.0,
            "actual": 2.5,
            "hook": 2.0,
            "specificity": 2.0,
            "emotional_resonance": 2.0,
            "novelty": 2.0,
            "actionability": 2.0,
            "days_ago": 6,
        },
        # Mid score band (3-6)
        {
            "predicted": 4.5,
            "actual": 4.0,
            "hook": 5.0,
            "specificity": 4.0,
            "emotional_resonance": 4.0,
            "novelty": 4.0,
            "actionability": 4.0,
            "days_ago": 7,
        },
        # High score band (6-10)
        {
            "predicted": 8.5,
            "actual": 8.0,
            "hook": 9.0,
            "specificity": 8.0,
            "emotional_resonance": 8.0,
            "novelty": 8.0,
            "actionability": 8.0,
            "days_ago": 8,
        },
        # Additional predictions to reach 10+ (needed for calibration context)
        {
            "predicted": 6.0,
            "actual": 6.5,
            "hook": 6.5,
            "specificity": 6.0,
            "emotional_resonance": 6.0,
            "novelty": 6.0,
            "actionability": 6.0,
            "days_ago": 9,
        },
        {
            "predicted": 7.5,
            "actual": 7.0,
            "hook": 7.5,
            "specificity": 7.0,
            "emotional_resonance": 7.5,
            "novelty": 7.0,
            "actionability": 7.0,
            "days_ago": 10,
        },
    ]

    for i, pred_data in enumerate(predictions_data, 1):
        # Insert content
        content_id = test_db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content=f"Test content {i}",
            eval_score=pred_data["predicted"],
            eval_feedback="Test content"
        )

        # Insert prediction with actuals
        error = pred_data["actual"] - pred_data["predicted"]
        timestamp = (now - timedelta(days=pred_data["days_ago"])).isoformat()

        test_db.conn.execute(
            """INSERT INTO engagement_predictions
               (content_id, predicted_score, actual_engagement_score,
                prediction_error, hook_strength, specificity,
                emotional_resonance, novelty, actionability, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                content_id,
                pred_data["predicted"],
                pred_data["actual"],
                error,
                pred_data["hook"],
                pred_data["specificity"],
                pred_data["emotional_resonance"],
                pred_data["novelty"],
                pred_data["actionability"],
                timestamp,
            ),
        )
        test_db.insert_engagement(
            content_id=content_id,
            tweet_id=f"tweet-{i}",
            like_count=0,
            retweet_count=0,
            reply_count=0,
            quote_count=0,
            engagement_score=pred_data["actual"],
        )

    test_db.conn.commit()
    return test_db


@pytest.fixture
def empty_db(test_db):
    """Database with no predictions."""
    # Add some content but no predictions
    test_db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Content without predictions",
        eval_score=7.0,
        eval_feedback="Test"
    )
    test_db.conn.commit()
    return test_db


# --- TestFormatReport ---


class TestFormatReport:
    """Test report formatting function."""

    def test_format_empty_report(self):
        """Test formatting when no data available."""
        report = CalibrationReport(
            overall_mae=0.0,
            overall_correlation=None,
            criterion_correlations={},
            overestimation_bias=0.0,
            score_band_accuracy={},
            sample_size=0,
            worst_criterion=None,
            best_criterion=None,
        )
        patterns = []

        output = format_report(report, patterns)

        assert "ENGAGEMENT PREDICTION CALIBRATION REPORT" in output
        assert "No predictions with actual engagement data yet." in output

    def test_format_report_with_data(self):
        """Test formatting with actual calibration data."""
        report = CalibrationReport(
            overall_mae=2.5,
            overall_correlation=0.75,
            criterion_correlations={
                "hook_strength": 0.8,
                "specificity": 0.6,
                "emotional_resonance": 0.5,
                "novelty": 0.3,
                "actionability": 0.7,
            },
            overestimation_bias=1.2,
            score_band_accuracy={
                "0-3": 0.5,
                "3-6": 2.0,
                "6-10": 3.5,
            },
            sample_size=15,
            worst_criterion="novelty",
            best_criterion="hook_strength",
        )
        patterns = [
            ErrorPattern(
                pattern_type="high_hook_low_actual",
                description="High hook_strength scores but low actual engagement (possible clickbait detection issue)",
                avg_error=2.5,
                count=3,
            )
        ]

        output = format_report(report, patterns)

        # Check header and summary
        assert "ENGAGEMENT PREDICTION CALIBRATION REPORT" in output
        assert "Sample size: 15 predictions" in output

        # Check overall accuracy metrics
        assert "OVERALL ACCURACY:" in output
        assert "Mean Absolute Error: 2.50" in output
        assert "Correlation (Pearson r): 0.750" in output

        # Check bias analysis
        assert "BIAS ANALYSIS:" in output
        assert "Bias: +1.20 (OVERESTIMATING)" in output

        # Check per-criterion correlations
        assert "PER-CRITERION CORRELATIONS:" in output
        assert "Hook Strength" in output
        assert "+0.800" in output
        assert "(STRONG)" in output
        assert "Novelty" in output
        assert "+0.300" in output
        assert "(WEAK)" in output

        # Check worst/best criterion
        assert "Worst criterion: Novelty" in output
        assert "Best criterion: Hook Strength" in output

        # Check score band accuracy
        assert "ACCURACY BY PREDICTED SCORE BAND:" in output
        assert "0-3" in output
        assert "MAE = 0.50" in output
        assert "3-6" in output
        assert "MAE = 2.00" in output
        assert "6-10" in output
        assert "MAE = 3.50" in output
        assert "(WORSE THAN AVERAGE)" in output

        # Check error patterns
        assert "DETECTED ERROR PATTERNS:" in output
        assert "High hook_strength scores but low actual engagement" in output
        assert "Avg error: +2.50 (3 cases)" in output

    def test_format_report_well_calibrated(self):
        """Test formatting when bias is well-calibrated (< 0.3)."""
        report = CalibrationReport(
            overall_mae=0.5,
            overall_correlation=0.9,
            criterion_correlations={},
            overestimation_bias=0.1,
            score_band_accuracy={},
            sample_size=10,
            worst_criterion=None,
            best_criterion=None,
        )
        patterns = []

        output = format_report(report, patterns)

        assert "BIAS ANALYSIS:" in output
        assert "Bias: +0.10 (well-calibrated)" in output

    def test_format_report_underestimating(self):
        """Test formatting when predictions are underestimating."""
        report = CalibrationReport(
            overall_mae=1.5,
            overall_correlation=0.6,
            criterion_correlations={},
            overestimation_bias=-0.8,
            score_band_accuracy={},
            sample_size=10,
            worst_criterion=None,
            best_criterion=None,
        )
        patterns = []

        output = format_report(report, patterns)

        assert "Bias: -0.80 (UNDERESTIMATING)" in output

    def test_format_report_criterion_status_labels(self):
        """Test criterion correlation status labels."""
        report = CalibrationReport(
            overall_mae=1.0,
            overall_correlation=0.5,
            criterion_correlations={
                "strong_criterion": 0.75,
                "moderate_criterion": 0.5,
                "weak_criterion": 0.2,
                "negative_criterion": -0.1,
            },
            overestimation_bias=0.0,
            score_band_accuracy={},
            sample_size=10,
            worst_criterion="negative_criterion",
            best_criterion="strong_criterion",
        )
        patterns = []

        output = format_report(report, patterns)

        assert "(STRONG)" in output
        assert "(MODERATE)" in output
        assert "(WEAK)" in output
        assert "(NEGATIVE!)" in output

    def test_format_report_score_band_status(self):
        """Test score band accuracy status indicators."""
        overall_mae = 2.0
        report = CalibrationReport(
            overall_mae=overall_mae,
            overall_correlation=0.6,
            criterion_correlations={},
            overestimation_bias=0.0,
            score_band_accuracy={
                "0-3": 1.0,  # Better than average (< 0.7 * 2.0 = 1.4)
                "3-6": 2.0,  # Average
                "6-10": 3.0,  # Worse than average (> 1.3 * 2.0 = 2.6)
            },
            sample_size=10,
            worst_criterion=None,
            best_criterion=None,
        )
        patterns = []

        output = format_report(report, patterns)

        assert "BETTER THAN AVERAGE" in output
        assert "WORSE THAN AVERAGE" in output


class TestFormatJson:
    """Test JSON formatting function."""

    def test_format_json_empty_report(self):
        """Test JSON formatting with empty report."""
        report = CalibrationReport(
            overall_mae=0.0,
            overall_correlation=None,
            criterion_correlations={},
            overestimation_bias=0.0,
            score_band_accuracy={},
            sample_size=0,
            worst_criterion=None,
            best_criterion=None,
        )
        patterns = []

        output = format_json(report, patterns)
        data = json.loads(output)

        assert data["platform"] == "all"
        assert data["sample_size"] == 0
        assert data["overall_mae"] == 0.0
        assert data["overall_correlation"] is None
        assert data["overestimation_bias"] == 0.0
        assert data["criterion_correlations"] == {}
        assert data["score_band_accuracy"] == {}
        assert data["worst_criterion"] is None
        assert data["best_criterion"] is None
        assert data["error_patterns"] == []

    def test_format_json_with_data(self):
        """Test JSON formatting with actual data."""
        report = CalibrationReport(
            overall_mae=2.5,
            overall_correlation=0.75,
            criterion_correlations={
                "hook_strength": 0.8,
                "specificity": 0.6,
            },
            overestimation_bias=1.2,
            score_band_accuracy={
                "0-3": 0.5,
                "3-6": 2.0,
                "6-10": 3.5,
            },
            sample_size=15,
            worst_criterion="novelty",
            best_criterion="hook_strength",
        )
        patterns = [
            ErrorPattern(
                pattern_type="high_hook_low_actual",
                description="High hook_strength scores but low actual engagement",
                avg_error=2.5,
                count=3,
            ),
            ErrorPattern(
                pattern_type="high_score_overestimation",
                description="Consistently overestimating high-scoring content",
                avg_error=3.0,
                count=5,
            ),
        ]

        output = format_json(report, patterns, platform="x")
        data = json.loads(output)

        assert data["platform"] == "x"
        assert data["sample_size"] == 15
        assert data["overall_mae"] == 2.5
        assert data["overall_correlation"] == 0.75
        assert data["overestimation_bias"] == 1.2
        assert data["criterion_correlations"]["hook_strength"] == 0.8
        assert data["criterion_correlations"]["specificity"] == 0.6
        assert data["score_band_accuracy"]["0-3"] == 0.5
        assert data["score_band_accuracy"]["3-6"] == 2.0
        assert data["score_band_accuracy"]["6-10"] == 3.5
        assert data["worst_criterion"] == "novelty"
        assert data["best_criterion"] == "hook_strength"
        assert len(data["error_patterns"]) == 2
        assert data["error_patterns"][0]["type"] == "high_hook_low_actual"
        assert data["error_patterns"][0]["avg_error"] == 2.5
        assert data["error_patterns"][0]["count"] == 3
        assert data["error_patterns"][1]["type"] == "high_score_overestimation"
        assert data["error_patterns"][1]["avg_error"] == 3.0
        assert data["error_patterns"][1]["count"] == 5


# --- TestMainFunction ---


class TestMainFunction:
    """Test the main CLI entry point."""

    def test_main_with_populated_db(self, populated_db, capsys, tmp_path):
        """Test main function with prediction data."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("calibration_report.script_context") as mock_context, \
             patch("calibration_report.update_monitoring") as mock_monitoring, \
             patch("sys.argv", ["calibration_report.py", "--days", "30"]):

            mock_context.return_value.__enter__ = lambda self: (mock_config, populated_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            # Run main
            main()

            # Check monitoring was updated
            mock_monitoring.assert_called_once_with("calibration_report")

        # Capture output
        captured = capsys.readouterr()

        # Verify report sections are present
        assert "ENGAGEMENT PREDICTION CALIBRATION REPORT" in captured.out
        assert "Sample size:" in captured.out
        assert "OVERALL ACCURACY:" in captured.out
        assert "BIAS ANALYSIS:" in captured.out
        assert "PER-CRITERION CORRELATIONS:" in captured.out
        assert "ACCURACY BY PREDICTED SCORE BAND:" in captured.out

        # Verify calibration context is shown
        assert "CALIBRATION CONTEXT (for injection):" in captured.out

    def test_main_with_empty_db(self, empty_db, capsys, tmp_path):
        """Test main function with no prediction data."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("calibration_report.script_context") as mock_context, \
             patch("calibration_report.update_monitoring") as mock_monitoring, \
             patch("sys.argv", ["calibration_report.py"]):

            mock_context.return_value.__enter__ = lambda self: (mock_config, empty_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            main()

            mock_monitoring.assert_called_once_with("calibration_report")

        captured = capsys.readouterr()

        # Should show empty state message
        assert "No predictions with actual engagement data yet." in captured.out
        # Should not show calibration context (insufficient data)
        assert "CALIBRATION CONTEXT (for injection):" not in captured.out

    def test_main_json_output(self, populated_db, capsys, tmp_path):
        """Test main function with --json flag."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("calibration_report.script_context") as mock_context, \
             patch("calibration_report.update_monitoring"), \
             patch("sys.argv", ["calibration_report.py", "--json"]):

            mock_context.return_value.__enter__ = lambda self: (mock_config, populated_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            main()

        captured = capsys.readouterr()

        # Should be valid JSON
        data = json.loads(captured.out)
        assert "sample_size" in data
        assert data["platform"] == "all"
        assert "overall_mae" in data
        assert "criterion_correlations" in data
        assert "error_patterns" in data

        # Should not contain human-readable formatting
        assert "ENGAGEMENT PREDICTION CALIBRATION REPORT" not in captured.out

    def test_main_platform_argument_filters_report(self, populated_db, capsys, tmp_path):
        """Test main function passes --platform through to calibration."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("calibration_report.script_context") as mock_context, \
             patch("calibration_report.update_monitoring"), \
             patch("sys.argv", ["calibration_report.py", "--platform", "x", "--json"]):

            mock_context.return_value.__enter__ = lambda self: (mock_config, populated_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            main()

        captured = capsys.readouterr()
        data = json.loads(captured.out)

        assert data["platform"] == "x"
        assert data["sample_size"] == 10

    def test_main_custom_days_argument(self, populated_db, capsys, tmp_path):
        """Test main function respects --days argument."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("calibration_report.script_context") as mock_context, \
             patch("calibration_report.update_monitoring"), \
             patch("sys.argv", ["calibration_report.py", "--days", "60"]):

            mock_context.return_value.__enter__ = lambda self: (mock_config, populated_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            main()

        captured = capsys.readouterr()

        # Verify report was generated
        assert "ENGAGEMENT PREDICTION CALIBRATION REPORT" in captured.out

    def test_main_stores_report_in_meta(self, populated_db, tmp_path):
        """Test that main stores report in meta table."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("calibration_report.script_context") as mock_context, \
             patch("calibration_report.update_monitoring"), \
             patch("sys.argv", ["calibration_report.py"]):

            mock_context.return_value.__enter__ = lambda self: (mock_config, populated_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            main()

        # Check meta table was updated
        cursor = populated_db.conn.execute(
            "SELECT value FROM meta WHERE key = 'calibration_report'"
        )
        result = cursor.fetchone()
        assert result is not None

        # Verify it's valid JSON
        report_data = json.loads(result[0])
        assert "sample_size" in report_data
        assert report_data["sample_size"] > 0

    def test_main_logging_output(self, populated_db, capsys, tmp_path):
        """Test that main function produces expected log messages."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("calibration_report.script_context") as mock_context, \
             patch("calibration_report.update_monitoring"), \
             patch("sys.argv", ["calibration_report.py", "--days", "30"]):

            mock_context.return_value.__enter__ = lambda self: (mock_config, populated_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            main()

        # Capture output which includes logging to stderr
        captured = capsys.readouterr()
        combined_output = captured.out + captured.err

        # Verify report was printed
        assert "Sample size:" in combined_output
        assert "OVERALL ACCURACY:" in combined_output
