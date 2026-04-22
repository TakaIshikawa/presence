"""Calibration system for engagement predictions.

Tracks prediction accuracy over time, analyzes error patterns, and generates
calibration context to improve future predictions.
"""

import statistics
from dataclasses import dataclass
from typing import Optional

from storage.db import Database


@dataclass
class CalibrationReport:
    """Summary of prediction accuracy metrics."""
    overall_mae: float
    overall_correlation: Optional[float]
    criterion_correlations: dict[str, float]
    overestimation_bias: float
    score_band_accuracy: dict[str, float]
    sample_size: int
    worst_criterion: Optional[str]
    best_criterion: Optional[str]


@dataclass
class ErrorPattern:
    """Identified systematic error pattern."""
    pattern_type: str
    description: str
    avg_error: float
    count: int


class PredictionCalibrator:
    """Analyzes prediction accuracy and generates calibration context."""

    CRITERIA = [
        "hook_strength",
        "specificity",
        "emotional_resonance",
        "novelty",
        "actionability",
    ]

    def __init__(self, db: Database) -> None:
        """Initialize calibrator with database connection.

        Args:
            db: Database instance with get_predictions_with_actuals method
        """
        self.db = db

    def compute_calibration_report(
        self, days: int = 30, platform: str = "all"
    ) -> CalibrationReport:
        """Compute calibration metrics from recent predictions.

        Args:
            days: Number of days to look back for predictions
            platform: Platform outcome to calibrate against ('all', 'x', 'bluesky')

        Returns:
            CalibrationReport with accuracy metrics and error analysis
        """
        predictions = self.db.get_predictions_with_actuals(days, platform=platform)

        if not predictions:
            return CalibrationReport(
                overall_mae=0.0,
                overall_correlation=None,
                criterion_correlations={},
                overestimation_bias=0.0,
                score_band_accuracy={},
                sample_size=0,
                worst_criterion=None,
                best_criterion=None,
            )

        # Extract scores
        predicted_scores = [p["predicted_score"] for p in predictions]
        actual_scores = [p["actual_engagement_score"] for p in predictions]
        errors = [p["prediction_error"] for p in predictions]

        # Overall metrics
        overall_mae = sum(abs(e) for e in errors) / len(errors)
        # Error is (actual - predicted), so negative = overestimate, positive = underestimate
        # We want bias where positive = overestimate for clearer messaging
        overestimation_bias = -sum(errors) / len(errors)

        # Correlation (requires at least 3 samples)
        overall_correlation = None
        if len(predictions) >= 3:
            try:
                overall_correlation = statistics.correlation(
                    predicted_scores, actual_scores
                )
            except statistics.StatisticsError:
                overall_correlation = None

        # Per-criterion correlations
        criterion_correlations = {}
        for criterion in self.CRITERIA:
            criterion_pairs = [
                (p[criterion], p["actual_engagement_score"])
                for p in predictions
                if p.get(criterion) is not None
            ]
            if len(criterion_pairs) >= 3:
                try:
                    criterion_values = [pair[0] for pair in criterion_pairs]
                    actual_values = [pair[1] for pair in criterion_pairs]
                    corr = statistics.correlation(criterion_values, actual_values)
                    criterion_correlations[criterion] = corr
                except statistics.StatisticsError:
                    criterion_correlations[criterion] = 0.0
            else:
                criterion_correlations[criterion] = 0.0

        # Find best/worst criteria
        worst_criterion = None
        best_criterion = None
        if criterion_correlations:
            worst_criterion = min(
                criterion_correlations.items(), key=lambda x: x[1]
            )[0]
            best_criterion = max(
                criterion_correlations.items(), key=lambda x: x[1]
            )[0]

        # Score band accuracy (0-3, 3-6, 6-10)
        score_bands = {"0-3": [], "3-6": [], "6-10": []}
        for pred, actual in zip(predicted_scores, actual_scores):
            error = abs(pred - actual)
            if pred < 3:
                score_bands["0-3"].append(error)
            elif pred < 6:
                score_bands["3-6"].append(error)
            else:
                score_bands["6-10"].append(error)

        score_band_accuracy = {
            band: sum(errs) / len(errs) if errs else 0.0
            for band, errs in score_bands.items()
        }

        return CalibrationReport(
            overall_mae=overall_mae,
            overall_correlation=overall_correlation,
            criterion_correlations=criterion_correlations,
            overestimation_bias=overestimation_bias,
            score_band_accuracy=score_band_accuracy,
            sample_size=len(predictions),
            worst_criterion=worst_criterion,
            best_criterion=best_criterion,
        )

    def generate_calibration_context(self, report: CalibrationReport) -> str:
        """Generate calibration context for injection into predictor prompt.

        Args:
            report: CalibrationReport from compute_calibration_report()

        Returns:
            Calibration context string, or empty if insufficient data
        """
        if report.sample_size < 10:
            return ""

        lines = ["CALIBRATION NOTE (based on recent prediction accuracy):"]

        # Bias warning
        if abs(report.overestimation_bias) > 0.5:
            if report.overestimation_bias > 0:
                lines.append(
                    f"- Your predictions tend to OVERESTIMATE by "
                    f"{report.overestimation_bias:.1f} points. Be more conservative."
                )
            else:
                lines.append(
                    f"- Your predictions tend to UNDERESTIMATE by "
                    f"{abs(report.overestimation_bias):.1f} points. Be more optimistic."
                )

        # Worst criterion warning
        if report.worst_criterion and report.criterion_correlations:
            worst_corr = report.criterion_correlations[report.worst_criterion]
            if worst_corr < 0.3:
                criterion_name = report.worst_criterion.replace("_", " ").title()
                lines.append(
                    f"- '{criterion_name}' scores have been least accurate "
                    f"(correlation: {worst_corr:.2f}). Recalibrate this dimension."
                )

        # Score band warning
        if report.score_band_accuracy:
            worst_band = max(
                report.score_band_accuracy.items(), key=lambda x: x[1]
            )[0]
            worst_mae = report.score_band_accuracy[worst_band]
            if worst_mae > report.overall_mae * 1.2:
                lines.append(
                    f"- Predictions in the {worst_band} range have higher error "
                    f"(MAE: {worst_mae:.1f}). Be extra careful in this range."
                )

        if len(lines) == 1:
            # No specific warnings, just overall accuracy
            lines.append(
                f"- Overall MAE: {report.overall_mae:.1f} "
                f"(correlation: {report.overall_correlation:.2f})"
            )

        return "\n".join(lines)

    def detect_error_patterns(
        self, days: int = 30, platform: str = "all"
    ) -> list[ErrorPattern]:
        """Detect systematic error patterns in predictions.

        Args:
            days: Number of days to look back for predictions
            platform: Platform outcome to analyze ('all', 'x', 'bluesky')

        Returns:
            List of ErrorPattern objects describing systematic errors
        """
        predictions = self.db.get_predictions_with_actuals(days, platform=platform)

        if len(predictions) < 5:
            return []

        patterns = []

        # Pattern 1: High hook strength but low actual engagement
        high_hook_low_actual = [
            p for p in predictions
            if p.get("hook_strength", 0) >= 7
            and p["actual_engagement_score"] < 5
        ]
        if len(high_hook_low_actual) >= 3:
            # Error is (actual - predicted), negative means overestimated
            avg_error = -sum(p["prediction_error"] for p in high_hook_low_actual) / len(
                high_hook_low_actual
            )
            patterns.append(
                ErrorPattern(
                    pattern_type="high_hook_low_actual",
                    description=(
                        "High hook_strength scores but low actual engagement "
                        "(possible clickbait detection issue)"
                    ),
                    avg_error=avg_error,
                    count=len(high_hook_low_actual),
                )
            )

        # Pattern 2: Consistently overestimating high scores
        high_predicted = [
            p for p in predictions if p["predicted_score"] >= 7
        ]
        if len(high_predicted) >= 3:
            # Error is (actual - predicted), negative means overestimated
            avg_error = -sum(p["prediction_error"] for p in high_predicted) / len(
                high_predicted
            )
            if avg_error > 1.0:  # Overestimating by >1 point on average
                patterns.append(
                    ErrorPattern(
                        pattern_type="high_score_overestimation",
                        description=(
                            f"Consistently overestimating high-scoring content "
                            f"(avg error: {avg_error:.1f})"
                        ),
                        avg_error=avg_error,
                        count=len(high_predicted),
                    )
                )

        # Pattern 3: Low novelty but high actual engagement
        low_novelty_high_actual = [
            p for p in predictions
            if p.get("novelty", 10) < 5
            and p["actual_engagement_score"] >= 7
        ]
        if len(low_novelty_high_actual) >= 3:
            # Error is (actual - predicted), positive means underestimated
            avg_error = -sum(
                p["prediction_error"] for p in low_novelty_high_actual
            ) / len(low_novelty_high_actual)
            patterns.append(
                ErrorPattern(
                    pattern_type="low_novelty_high_actual",
                    description=(
                        "Low novelty scores but high actual engagement "
                        "(familiar topics can still perform well)"
                    ),
                    avg_error=avg_error,
                    count=len(low_novelty_high_actual),
                )
            )

        return patterns
