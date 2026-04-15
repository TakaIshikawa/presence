"""Tests for prediction calibration system."""

import sqlite3
import pytest
from datetime import datetime, timedelta

from evaluation.prediction_calibrator import (
    PredictionCalibrator,
    CalibrationReport,
    ErrorPattern,
)


class MockDB:
    """Mock database for testing calibration."""

    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self._setup_schema()

    def _setup_schema(self):
        """Create engagement_predictions table."""
        self.conn.execute(
            """CREATE TABLE engagement_predictions (
                id INTEGER PRIMARY KEY,
                content_id INTEGER,
                predicted_score REAL,
                actual_engagement_score REAL,
                prediction_error REAL,
                hook_strength REAL,
                specificity REAL,
                emotional_resonance REAL,
                novelty REAL,
                actionability REAL,
                created_at TEXT
            )"""
        )
        self.conn.commit()

    def insert_prediction(
        self,
        content_id,
        predicted,
        actual,
        hook=None,
        specificity=None,
        emotional_resonance=None,
        novelty=None,
        actionability=None,
        days_ago=0,
    ):
        """Insert a test prediction."""
        error = actual - predicted
        timestamp = (datetime.now() - timedelta(days=days_ago)).isoformat()
        self.conn.execute(
            """INSERT INTO engagement_predictions
               (content_id, predicted_score, actual_engagement_score,
                prediction_error, hook_strength, specificity,
                emotional_resonance, novelty, actionability, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                content_id,
                predicted,
                actual,
                error,
                hook,
                specificity,
                emotional_resonance,
                novelty,
                actionability,
                timestamp,
            ),
        )
        self.conn.commit()

    def get_predictions_with_actuals(self, days=30):
        """Get predictions with actuals (implements DB interface)."""
        cursor = self.conn.execute(
            """SELECT predicted_score, actual_engagement_score, prediction_error,
                      hook_strength, specificity, emotional_resonance,
                      novelty, actionability, content_id, created_at
               FROM engagement_predictions
               WHERE actual_engagement_score IS NOT NULL
                 AND created_at >= datetime('now', ?)
               ORDER BY created_at DESC""",
            (f'-{days} days',)
        )
        rows = cursor.fetchall()
        return [
            {
                "predicted_score": row[0],
                "actual_engagement_score": row[1],
                "prediction_error": row[2],
                "hook_strength": row[3],
                "specificity": row[4],
                "emotional_resonance": row[5],
                "novelty": row[6],
                "actionability": row[7],
                "content_id": row[8],
                "created_at": row[9],
            }
            for row in rows
        ]

    def get_predictions_by_criterion(self, criterion, days=30):
        """Get criterion-specific predictions."""
        cursor = self.conn.execute(
            f"""SELECT {criterion}, actual_engagement_score
                FROM engagement_predictions
                WHERE actual_engagement_score IS NOT NULL
                  AND {criterion} IS NOT NULL
                  AND created_at >= datetime('now', ?)
                ORDER BY created_at DESC""",
            (f'-{days} days',)
        )
        return cursor.fetchall()


class TestCalibrationReport:
    """Tests for compute_calibration_report."""

    @pytest.fixture
    def db(self):
        return MockDB()

    @pytest.fixture
    def calibrator(self, db):
        return PredictionCalibrator(db)

    def test_empty_database_returns_zero_report(self, calibrator):
        """Empty database should return zeroed report."""
        report = calibrator.compute_calibration_report()

        assert report.sample_size == 0
        assert report.overall_mae == 0.0
        assert report.overall_correlation is None
        assert report.criterion_correlations == {}
        assert report.worst_criterion is None
        assert report.best_criterion is None

    def test_single_prediction(self, db, calibrator):
        """Single prediction should compute MAE but not correlation."""
        db.insert_prediction(1, predicted=7.0, actual=6.0)

        report = calibrator.compute_calibration_report()

        assert report.sample_size == 1
        assert report.overall_mae == 1.0
        assert report.overall_correlation is None  # Need 3+ for correlation

    def test_overestimation_bias(self, db, calibrator):
        """Consistently overestimating should show positive bias."""
        # All predictions overestimate by 2 points
        db.insert_prediction(1, predicted=8.0, actual=6.0)
        db.insert_prediction(2, predicted=7.0, actual=5.0)
        db.insert_prediction(3, predicted=6.0, actual=4.0)

        report = calibrator.compute_calibration_report()

        assert report.sample_size == 3
        assert report.overestimation_bias == pytest.approx(2.0)
        assert report.overall_mae == pytest.approx(2.0)

    def test_underestimation_bias(self, db, calibrator):
        """Consistently underestimating should show negative bias."""
        # All predictions underestimate by 1.5 points
        db.insert_prediction(1, predicted=5.0, actual=6.5)
        db.insert_prediction(2, predicted=6.0, actual=7.5)
        db.insert_prediction(3, predicted=4.0, actual=5.5)

        report = calibrator.compute_calibration_report()

        assert report.overestimation_bias == pytest.approx(-1.5)

    def test_correlation_with_perfect_predictions(self, db, calibrator):
        """Perfect predictions should have correlation = 1.0."""
        for i in range(5):
            score = 5.0 + i
            db.insert_prediction(i, predicted=score, actual=score)

        report = calibrator.compute_calibration_report()

        assert report.overall_correlation == pytest.approx(1.0)
        assert report.overall_mae == 0.0

    def test_correlation_with_random_predictions(self, db, calibrator):
        """Random predictions should be computed correctly."""
        # Predicted and actual - just ensure correlation is calculated
        predictions = [
            (7.0, 3.0),
            (4.0, 8.0),
            (9.0, 5.0),
            (2.0, 7.0),
            (6.0, 4.0),
        ]
        for i, (pred, actual) in enumerate(predictions):
            db.insert_prediction(i, predicted=pred, actual=actual)

        report = calibrator.compute_calibration_report()

        # Just ensure correlation is calculated (value doesn't matter for random data)
        assert report.overall_correlation is not None
        assert -1.0 <= report.overall_correlation <= 1.0

    def test_criterion_correlations(self, db, calibrator):
        """Should compute per-criterion correlations."""
        # Hook strength perfectly predicts engagement
        for i in range(5):
            hook = 5.0 + i
            db.insert_prediction(
                i,
                predicted=6.0,
                actual=hook,  # actual matches hook
                hook=hook,
                specificity=5.0,
                emotional_resonance=5.0,
                novelty=5.0,
                actionability=5.0,
            )

        report = calibrator.compute_calibration_report()

        assert report.criterion_correlations["hook_strength"] == pytest.approx(1.0, abs=0.01)
        assert report.best_criterion == "hook_strength"

    def test_worst_criterion_detection(self, db, calibrator):
        """Should identify worst-performing criterion."""
        # Novelty anti-correlates with engagement
        for i in range(5):
            novelty = 5.0 + i
            actual = 10.0 - novelty  # inverse relationship
            db.insert_prediction(
                i,
                predicted=6.0,
                actual=actual,
                hook=6.0,
                specificity=6.0,
                emotional_resonance=6.0,
                novelty=novelty,
                actionability=6.0,
            )

        report = calibrator.compute_calibration_report()

        assert report.worst_criterion == "novelty"
        assert report.criterion_correlations["novelty"] < 0

    def test_score_band_accuracy(self, db, calibrator):
        """Should compute accuracy by score band."""
        # Low scores: perfect predictions
        db.insert_prediction(1, predicted=2.0, actual=2.0)
        db.insert_prediction(2, predicted=2.5, actual=2.5)

        # Mid scores: off by 1
        db.insert_prediction(3, predicted=5.0, actual=4.0)
        db.insert_prediction(4, predicted=4.5, actual=5.5)

        # High scores: off by 2
        db.insert_prediction(5, predicted=8.0, actual=6.0)
        db.insert_prediction(6, predicted=7.0, actual=9.0)

        report = calibrator.compute_calibration_report()

        assert report.score_band_accuracy["0-3"] == pytest.approx(0.0)
        assert report.score_band_accuracy["3-6"] == pytest.approx(1.0)
        assert report.score_band_accuracy["6-10"] == pytest.approx(2.0)

    def test_filters_old_predictions(self, db, calibrator):
        """Should only include predictions within time window."""
        # Recent prediction
        db.insert_prediction(1, predicted=7.0, actual=6.0, days_ago=5)
        # Old prediction (outside 30-day window)
        db.insert_prediction(2, predicted=8.0, actual=3.0, days_ago=35)

        report = calibrator.compute_calibration_report(days=30)

        assert report.sample_size == 1
        assert report.overall_mae == 1.0  # Only recent prediction


class TestCalibrationContext:
    """Tests for generate_calibration_context."""

    @pytest.fixture
    def db(self):
        return MockDB()

    @pytest.fixture
    def calibrator(self, db):
        return PredictionCalibrator(db)

    def test_empty_context_for_small_sample(self, calibrator):
        """Should return empty string for < 10 predictions."""
        report = CalibrationReport(
            overall_mae=1.0,
            overall_correlation=0.8,
            criterion_correlations={},
            overestimation_bias=0.5,
            score_band_accuracy={},
            sample_size=5,
            worst_criterion=None,
            best_criterion=None,
        )

        context = calibrator.generate_calibration_context(report)

        assert context == ""

    def test_overestimation_warning(self, calibrator):
        """Should warn about overestimation bias."""
        report = CalibrationReport(
            overall_mae=1.5,
            overall_correlation=0.6,
            criterion_correlations={},
            overestimation_bias=1.2,  # Overestimating
            score_band_accuracy={},
            sample_size=20,
            worst_criterion=None,
            best_criterion=None,
        )

        context = calibrator.generate_calibration_context(report)

        assert "OVERESTIMATE" in context
        assert "1.2" in context
        assert "conservative" in context

    def test_underestimation_warning(self, calibrator):
        """Should warn about underestimation bias."""
        report = CalibrationReport(
            overall_mae=1.0,
            overall_correlation=0.7,
            criterion_correlations={},
            overestimation_bias=-0.8,  # Underestimating
            score_band_accuracy={},
            sample_size=15,
            worst_criterion=None,
            best_criterion=None,
        )

        context = calibrator.generate_calibration_context(report)

        assert "UNDERESTIMATE" in context
        assert "optimistic" in context

    def test_worst_criterion_warning(self, calibrator):
        """Should warn about poorly-performing criterion."""
        report = CalibrationReport(
            overall_mae=1.0,
            overall_correlation=0.6,
            criterion_correlations={
                "hook_strength": 0.7,
                "specificity": 0.2,  # Low correlation
                "emotional_resonance": 0.5,
            },
            overestimation_bias=0.1,
            score_band_accuracy={},
            sample_size=25,
            worst_criterion="specificity",
            best_criterion="hook_strength",
        )

        context = calibrator.generate_calibration_context(report)

        assert "Specificity" in context
        assert "least accurate" in context

    def test_score_band_warning(self, calibrator):
        """Should warn about problematic score bands."""
        report = CalibrationReport(
            overall_mae=1.0,
            overall_correlation=0.7,
            criterion_correlations={},
            overestimation_bias=0.2,
            score_band_accuracy={
                "0-3": 0.5,
                "3-6": 0.8,
                "6-10": 2.5,  # Much worse than overall
            },
            sample_size=30,
            worst_criterion=None,
            best_criterion=None,
        )

        context = calibrator.generate_calibration_context(report)

        assert "6-10" in context
        assert "higher error" in context


class TestErrorPatternDetection:
    """Tests for detect_error_patterns."""

    @pytest.fixture
    def db(self):
        return MockDB()

    @pytest.fixture
    def calibrator(self, db):
        return PredictionCalibrator(db)

    def test_empty_database_no_patterns(self, calibrator):
        """Empty database should return no patterns."""
        patterns = calibrator.detect_error_patterns()

        assert patterns == []

    def test_high_hook_low_actual_pattern(self, db, calibrator):
        """Should detect clickbait pattern (high hook, low actual)."""
        # Need 5+ total predictions for pattern detection
        # 3 cases of high hook but low engagement
        for i in range(3):
            db.insert_prediction(
                i,
                predicted=8.0,
                actual=4.0,
                hook=8.0,
                specificity=6.0,
                emotional_resonance=6.0,
                novelty=6.0,
                actionability=6.0,
            )
        # Add 2 normal predictions to reach minimum
        for i in range(3, 5):
            db.insert_prediction(
                i,
                predicted=6.0,
                actual=6.0,
                hook=6.0,
                specificity=6.0,
                emotional_resonance=6.0,
                novelty=6.0,
                actionability=6.0,
            )

        patterns = calibrator.detect_error_patterns()

        high_hook_patterns = [
            p for p in patterns if p.pattern_type == "high_hook_low_actual"
        ]
        assert len(high_hook_patterns) == 1
        assert high_hook_patterns[0].count == 3
        assert "clickbait" in high_hook_patterns[0].description.lower()

    def test_high_score_overestimation_pattern(self, db, calibrator):
        """Should detect systematic overestimation of high scores."""
        # Need 5+ total predictions
        # 4 high-scoring predictions, all overestimate significantly
        for i in range(4):
            db.insert_prediction(
                i,
                predicted=8.0,
                actual=6.0,  # Overestimating by 2
                hook=7.0,
                specificity=7.0,
                emotional_resonance=7.0,
                novelty=7.0,
                actionability=7.0,
            )
        # Add 1 normal prediction to reach minimum
        db.insert_prediction(
            4,
            predicted=5.0,
            actual=5.0,
            hook=5.0,
            specificity=5.0,
            emotional_resonance=5.0,
            novelty=5.0,
            actionability=5.0,
        )

        patterns = calibrator.detect_error_patterns()

        overestimation_patterns = [
            p for p in patterns if p.pattern_type == "high_score_overestimation"
        ]
        assert len(overestimation_patterns) == 1
        assert overestimation_patterns[0].avg_error == pytest.approx(2.0)

    def test_low_novelty_high_actual_pattern(self, db, calibrator):
        """Should detect that familiar topics can perform well."""
        # Need 5+ total predictions
        # 3 cases of low novelty but high engagement
        for i in range(3):
            db.insert_prediction(
                i,
                predicted=5.0,
                actual=8.0,
                hook=6.0,
                specificity=6.0,
                emotional_resonance=6.0,
                novelty=3.0,  # Low novelty
                actionability=6.0,
            )
        # Add 2 normal predictions to reach minimum
        for i in range(3, 5):
            db.insert_prediction(
                i,
                predicted=6.0,
                actual=6.0,
                hook=6.0,
                specificity=6.0,
                emotional_resonance=6.0,
                novelty=6.0,
                actionability=6.0,
            )

        patterns = calibrator.detect_error_patterns()

        low_novelty_patterns = [
            p for p in patterns if p.pattern_type == "low_novelty_high_actual"
        ]
        assert len(low_novelty_patterns) == 1
        assert "familiar topics" in low_novelty_patterns[0].description.lower()

    def test_minimum_samples_required(self, db, calibrator):
        """Should require at least 5 total predictions to detect patterns."""
        # Only 4 predictions - too few
        for i in range(4):
            db.insert_prediction(
                i,
                predicted=8.0,
                actual=4.0,
                hook=8.0,
                specificity=6.0,
                emotional_resonance=6.0,
                novelty=6.0,
                actionability=6.0,
            )

        patterns = calibrator.detect_error_patterns()

        assert patterns == []

    def test_pattern_threshold_requires_3_cases(self, db, calibrator):
        """Each pattern type needs at least 3 matching cases."""
        # Only 2 high-hook/low-actual cases
        for i in range(2):
            db.insert_prediction(
                i,
                predicted=8.0,
                actual=4.0,
                hook=8.0,
                specificity=6.0,
                emotional_resonance=6.0,
                novelty=6.0,
                actionability=6.0,
            )
        # Add 3 more normal predictions to reach 5 total
        for i in range(2, 5):
            db.insert_prediction(
                i,
                predicted=6.0,
                actual=6.0,
                hook=6.0,
                specificity=6.0,
                emotional_resonance=6.0,
                novelty=6.0,
                actionability=6.0,
            )

        patterns = calibrator.detect_error_patterns()

        # Should not detect high_hook_low_actual with only 2 cases
        high_hook_patterns = [
            p for p in patterns if p.pattern_type == "high_hook_low_actual"
        ]
        assert len(high_hook_patterns) == 0


class TestCalibrationIntegration:
    """Integration tests for full calibration workflow."""

    @pytest.fixture
    def db(self):
        return MockDB()

    @pytest.fixture
    def calibrator(self, db):
        return PredictionCalibrator(db)

    def test_full_workflow_with_synthetic_data(self, db, calibrator):
        """Test complete workflow from data to calibration context."""
        # Create realistic prediction data with known biases
        # Bias 1: Overestimate high-scoring content
        for i in range(5):
            db.insert_prediction(
                i,
                predicted=8.5,
                actual=6.5,
                hook=8.0,
                specificity=7.0,
                emotional_resonance=7.0,
                novelty=6.0,
                actionability=6.0,
            )

        # Bias 2: Hook strength is misleading (high hook, low actual < 5)
        for i in range(5, 10):
            db.insert_prediction(
                i,
                predicted=7.0,
                actual=4.0,  # Low actual to trigger pattern
                hook=8.0,  # High hook
                specificity=6.0,
                emotional_resonance=5.0,
                novelty=5.0,
                actionability=5.0,
            )

        # Some well-calibrated predictions
        for i in range(10, 15):
            score = 5.0 + (i - 10) * 0.5
            db.insert_prediction(
                i,
                predicted=score,
                actual=score + 0.2,
                hook=score,
                specificity=score,
                emotional_resonance=score,
                novelty=score,
                actionability=score,
            )

        # Generate report
        report = calibrator.compute_calibration_report()
        assert report.sample_size == 15
        assert report.overestimation_bias > 0.5  # Should detect overestimation

        # Generate context
        context = calibrator.generate_calibration_context(report)
        assert "OVERESTIMATE" in context

        # Detect patterns
        patterns = calibrator.detect_error_patterns()
        high_hook_patterns = [
            p for p in patterns if p.pattern_type == "high_hook_low_actual"
        ]
        assert len(high_hook_patterns) >= 1
