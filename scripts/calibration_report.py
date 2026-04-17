#!/usr/bin/env python3
"""Generate calibration report for engagement predictions."""

import argparse
import json
import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from evaluation.prediction_calibrator import (
    PredictionCalibrator,
    CalibrationReport,
    ErrorPattern,
)

logger = logging.getLogger(__name__)


def format_report(report: CalibrationReport, patterns: list[ErrorPattern]) -> str:
    """Format calibration report as human-readable text."""
    lines = []
    lines.append("=" * 70)
    lines.append("ENGAGEMENT PREDICTION CALIBRATION REPORT")
    lines.append("=" * 70)
    lines.append("")

    if report.sample_size == 0:
        lines.append("No predictions with actual engagement data yet.")
        return "\n".join(lines)

    lines.append(f"Sample size: {report.sample_size} predictions")
    lines.append("")

    # Overall accuracy
    lines.append("OVERALL ACCURACY:")
    lines.append(f"  Mean Absolute Error: {report.overall_mae:.2f}")
    if report.overall_correlation is not None:
        lines.append(f"  Correlation (Pearson r): {report.overall_correlation:.3f}")
    lines.append("")

    # Bias
    lines.append("BIAS ANALYSIS:")
    if abs(report.overestimation_bias) < 0.3:
        lines.append(f"  Bias: {report.overestimation_bias:+.2f} (well-calibrated)")
    elif report.overestimation_bias > 0:
        lines.append(f"  Bias: {report.overestimation_bias:+.2f} (OVERESTIMATING)")
    else:
        lines.append(f"  Bias: {report.overestimation_bias:+.2f} (UNDERESTIMATING)")
    lines.append("")

    # Per-criterion accuracy
    if report.criterion_correlations:
        lines.append("PER-CRITERION CORRELATIONS:")
        for criterion, corr in sorted(
            report.criterion_correlations.items(),
            key=lambda x: x[1],
            reverse=True
        ):
            criterion_name = criterion.replace("_", " ").title()
            status = ""
            if corr >= 0.7:
                status = " (STRONG)"
            elif corr >= 0.4:
                status = " (MODERATE)"
            elif corr >= 0.0:
                status = " (WEAK)"
            else:
                status = " (NEGATIVE!)"
            lines.append(f"  {criterion_name:25s}: {corr:+.3f}{status}")
        lines.append("")

        if report.worst_criterion:
            worst_name = report.worst_criterion.replace("_", " ").title()
            lines.append(f"  Worst criterion: {worst_name}")
        if report.best_criterion:
            best_name = report.best_criterion.replace("_", " ").title()
            lines.append(f"  Best criterion: {best_name}")
        lines.append("")

    # Score band accuracy
    if report.score_band_accuracy:
        lines.append("ACCURACY BY PREDICTED SCORE BAND:")
        for band in ["0-3", "3-6", "6-10"]:
            mae = report.score_band_accuracy.get(band, 0.0)
            status = ""
            if mae > report.overall_mae * 1.3:
                status = " (WORSE THAN AVERAGE)"
            elif mae < report.overall_mae * 0.7:
                status = " (BETTER THAN AVERAGE)"
            lines.append(f"  {band:5s}: MAE = {mae:.2f}{status}")
        lines.append("")

    # Error patterns
    if patterns:
        lines.append("DETECTED ERROR PATTERNS:")
        for i, pattern in enumerate(patterns, 1):
            lines.append(f"  {i}. {pattern.description}")
            lines.append(f"     Avg error: {pattern.avg_error:+.2f} ({pattern.count} cases)")
        lines.append("")

    # Calibration context
    lines.append("CALIBRATION CONTEXT FOR PREDICTOR:")
    lines.append("-" * 70)
    lines.append("  (See calibration context below)")
    lines.append("-" * 70)

    return "\n".join(lines)


def format_json(report: CalibrationReport, patterns: list[ErrorPattern]) -> str:
    """Format calibration report as JSON."""
    return json.dumps(
        {
            "sample_size": report.sample_size,
            "overall_mae": report.overall_mae,
            "overall_correlation": report.overall_correlation,
            "overestimation_bias": report.overestimation_bias,
            "criterion_correlations": report.criterion_correlations,
            "score_band_accuracy": report.score_band_accuracy,
            "worst_criterion": report.worst_criterion,
            "best_criterion": report.best_criterion,
            "error_patterns": [
                {
                    "type": p.pattern_type,
                    "description": p.description,
                    "avg_error": p.avg_error,
                    "count": p.count,
                }
                for p in patterns
            ],
        },
        indent=2,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate calibration report for engagement predictions"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to look back (default: 30)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON instead of human-readable format",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    with script_context() as (config, db):
        calibrator = PredictionCalibrator(db)

        logger.info(f"Computing calibration report (last {args.days} days)...")
        report = calibrator.compute_calibration_report(days=args.days)
        patterns = calibrator.detect_error_patterns(days=args.days)

        if args.json:
            print(format_json(report, patterns))
        else:
            print(format_report(report, patterns))
            print()

            # Print calibration context
            context = calibrator.generate_calibration_context(report)
            if context:
                print("CALIBRATION CONTEXT (for injection):")
                print("-" * 70)
                print(context)
                print("-" * 70)

        # Store report in meta table for dashboard access
        if report.sample_size > 0:
            report_json = format_json(report, patterns)
            db.conn.execute(
                """INSERT OR REPLACE INTO meta (key, value)
                   VALUES ('calibration_report', ?)""",
                (report_json,)
            )
            db.conn.commit()
            logger.info("Stored calibration report in database")

        update_monitoring("calibration_report")
        logger.info("Done.")


if __name__ == "__main__":
    main()
