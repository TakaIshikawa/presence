#!/usr/bin/env python3
"""Validate engagement predictions against actual engagement data."""

import logging
import sys
from pathlib import Path
from typing import Tuple, Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring

logger = logging.getLogger(__name__)


def validate_prediction_row(
    content_id: int,
    predicted_score: Optional[float],
    actual_score: Optional[float]
) -> Tuple[bool, Optional[str]]:
    """Validate a single prediction row.

    Returns:
        (is_valid, error_message)
    """
    # Check required fields
    if content_id is None:
        return False, "Missing content_id"

    if predicted_score is None:
        return False, f"Content {content_id}: Missing predicted_score"

    # Validate predicted_score range (0-10)
    if not (0 <= predicted_score <= 10):
        return False, f"Content {content_id}: predicted_score {predicted_score} out of range [0, 10]"

    # Validate actual_score if present
    if actual_score is not None and actual_score < 0:
        return False, f"Content {content_id}: actual engagement_score {actual_score} cannot be negative"

    return True, None


def main() -> int:
    """Main entry point. Returns exit code (0=success, 1=validation errors)."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    validation_errors = []

    with script_context() as (config, db):
        # Find published content with engagement data but no backfilled prediction actuals
        cursor = db.conn.execute(
            """SELECT gc.id, gc.content, pe.engagement_score, ep.predicted_score
               FROM generated_content gc
               INNER JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY fetched_at DESC) AS rn
                   FROM post_engagement
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               INNER JOIN engagement_predictions ep ON ep.content_id = gc.id
               WHERE gc.published = 1
                 AND ep.actual_engagement_score IS NULL
               ORDER BY gc.published_at DESC"""
        )
        rows = cursor.fetchall()

        if not rows:
            print("No predictions need backfilling")
        else:
            print(f"Validating {len(rows)} predictions before backfilling")

            # First pass: validate all rows
            for row in rows:
                content_id = row[0]
                actual_score = row[2]
                predicted_score = row[3]

                is_valid, error_msg = validate_prediction_row(
                    content_id, predicted_score, actual_score
                )
                if not is_valid:
                    validation_errors.append(error_msg)
                    print(f"  INVALID: {error_msg}", file=sys.stderr)

            # If validation errors found, report and exit
            if validation_errors:
                print(f"\nValidation failed: {len(validation_errors)} invalid prediction(s) found", file=sys.stderr)
                for error in validation_errors:
                    print(f"  - {error}", file=sys.stderr)
                update_monitoring("validate_predictions")
                return 1

            # Second pass: backfill valid predictions
            print(f"All predictions valid. Backfilling {len(rows)} predictions with actual engagement")
            for row in rows:
                content_id = row[0]
                content_preview = row[1][:50] + "..." if len(row[1]) > 50 else row[1]
                actual_score = row[2]
                predicted_score = row[3]

                db.backfill_prediction_actuals(content_id, actual_score)
                error = actual_score - predicted_score
                print(
                    f"  Content {content_id}: predicted={predicted_score:.1f}, "
                    f"actual={actual_score:.1f}, error={error:+.1f}"
                )

        # Get prediction accuracy summary
        accuracy = db.get_prediction_accuracy(days=30)

        if accuracy["count"] > 0:
            print(f"\nPrediction Accuracy (last 30 days):")
            print(f"  Total predictions: {accuracy['count']}")
            print(f"  Mean Absolute Error: {accuracy['mae']}")
            print(f"  Avg Predicted: {accuracy['avg_predicted']}")
            print(f"  Avg Actual: {accuracy['avg_actual']}")
            if accuracy["correlation"] is not None:
                print(f"  Correlation: {accuracy['correlation']}")

            if accuracy["criteria_breakdown"]:
                print(f"\n  Per-Criteria Breakdown:")
                for criterion, stats in accuracy["criteria_breakdown"].items():
                    print(
                        f"    {criterion}: avg={stats['avg']:.2f} (n={stats['count']})"
                    )
        else:
            print("\nNo predictions with actual engagement yet")

        update_monitoring("validate_predictions")
        print("\nDone.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
