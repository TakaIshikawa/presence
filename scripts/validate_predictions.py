#!/usr/bin/env python3
"""Validate engagement predictions against actual engagement data."""

import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

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
            logger.info("No predictions need backfilling")
        else:
            logger.info(f"Backfilling {len(rows)} predictions with actual engagement")
            for row in rows:
                content_id = row[0]
                content_preview = row[1][:50] + "..." if len(row[1]) > 50 else row[1]
                actual_score = row[2]
                predicted_score = row[3]

                db.backfill_prediction_actuals(content_id, actual_score)
                error = actual_score - predicted_score
                logger.info(
                    f"  Content {content_id}: predicted={predicted_score:.1f}, "
                    f"actual={actual_score:.1f}, error={error:+.1f}"
                )

        # Get prediction accuracy summary
        accuracy = db.get_prediction_accuracy(days=30)

        if accuracy["count"] > 0:
            logger.info(f"\nPrediction Accuracy (last 30 days):")
            logger.info(f"  Total predictions: {accuracy['count']}")
            logger.info(f"  Mean Absolute Error: {accuracy['mae']}")
            logger.info(f"  Avg Predicted: {accuracy['avg_predicted']}")
            logger.info(f"  Avg Actual: {accuracy['avg_actual']}")
            if accuracy["correlation"] is not None:
                logger.info(f"  Correlation: {accuracy['correlation']}")

            if accuracy["criteria_breakdown"]:
                logger.info(f"\n  Per-Criteria Breakdown:")
                for criterion, stats in accuracy["criteria_breakdown"].items():
                    logger.info(
                        f"    {criterion}: avg={stats['avg']:.2f} (n={stats['count']})"
                    )
        else:
            logger.info("\nNo predictions with actual engagement yet")

        update_monitoring("validate_predictions")
        logger.info("\nDone.")


if __name__ == "__main__":
    main()
