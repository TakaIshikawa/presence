"""Tests for engagement prediction integration."""

import pytest
from datetime import datetime, timedelta, timezone

from evaluation.engagement_predictor import EngagementPrediction


class TestEngagementPredictionDB:
    """Test database methods for engagement predictions."""

    def test_insert_prediction(self, db):
        """Test storing a prediction in the database."""
        # Insert content first
        content_id = db.insert_generated_content(
            content_type="x_thread",
            source_commits=["abc123"],
            source_messages=["msg-1"],
            content="Test thread content",
            eval_score=8.5,
            eval_feedback="Good",
        )

        # Insert prediction
        prediction_id = db.insert_prediction(
            content_id=content_id,
            predicted_score=25.0,
            hook_strength=8.0,
            specificity=7.5,
            emotional_resonance=6.0,
            novelty=7.0,
            actionability=5.5,
            prompt_version="v1",
        )

        assert prediction_id > 0

        # Verify stored
        cursor = db.conn.execute(
            "SELECT * FROM engagement_predictions WHERE id = ?", (prediction_id,)
        )
        row = cursor.fetchone()
        assert row is not None
        assert row["content_id"] == content_id
        assert row["predicted_score"] == 25.0
        assert row["hook_strength"] == 8.0
        assert row["specificity"] == 7.5
        assert row["emotional_resonance"] == 6.0
        assert row["novelty"] == 7.0
        assert row["actionability"] == 5.5
        assert row["prompt_version"] == "v1"
        assert row["actual_engagement_score"] is None
        assert row["prediction_error"] is None

    def test_backfill_prediction_actuals(self, db):
        """Test backfilling actual engagement scores."""
        # Insert content and prediction
        content_id = db.insert_generated_content(
            content_type="x_thread",
            source_commits=["abc123"],
            source_messages=["msg-1"],
            content="Test thread",
            eval_score=8.0,
            eval_feedback="Good",
        )
        db.insert_prediction(
            content_id=content_id,
            predicted_score=20.0,
            prompt_version="v1",
        )

        # Backfill with actual score
        actual_score = 28.5
        db.backfill_prediction_actuals(content_id, actual_score)

        # Verify backfilled
        cursor = db.conn.execute(
            "SELECT * FROM engagement_predictions WHERE content_id = ?",
            (content_id,),
        )
        row = cursor.fetchone()
        assert row["actual_engagement_score"] == 28.5
        assert row["prediction_error"] == 8.5  # 28.5 - 20.0

    def test_backfill_no_prediction(self, db):
        """Test backfilling when no prediction exists (should not crash)."""
        content_id = db.insert_generated_content(
            content_type="x_thread",
            source_commits=["abc123"],
            source_messages=["msg-1"],
            content="Test",
            eval_score=8.0,
            eval_feedback="Good",
        )

        # Should not crash
        db.backfill_prediction_actuals(content_id, 25.0)

    def test_get_prediction_accuracy_no_data(self, db):
        """Test accuracy calculation with no predictions."""
        accuracy = db.get_prediction_accuracy(days=30)

        assert accuracy["count"] == 0
        assert accuracy["mae"] is None
        assert accuracy["correlation"] is None
        assert accuracy["avg_predicted"] is None
        assert accuracy["avg_actual"] is None

    def test_get_prediction_accuracy_with_data(self, db):
        """Test accuracy calculation with multiple predictions."""
        now = datetime.now(timezone.utc)

        # Create multiple predictions with actuals
        predictions = [
            (20.0, 25.0),  # error = 5.0
            (30.0, 28.0),  # error = -2.0
            (15.0, 18.0),  # error = 3.0
        ]

        for predicted, actual in predictions:
            content_id = db.insert_generated_content(
                content_type="x_thread",
                source_commits=["abc"],
                source_messages=["msg"],
                content="Test",
                eval_score=8.0,
                eval_feedback="Good",
            )
            db.insert_prediction(
                content_id=content_id,
                predicted_score=predicted,
                hook_strength=7.0,
                specificity=6.0,
                emotional_resonance=5.0,
                novelty=6.5,
                actionability=5.5,
                prompt_version="v1",
            )
            db.backfill_prediction_actuals(content_id, actual)

        accuracy = db.get_prediction_accuracy(days=30)

        assert accuracy["count"] == 3
        # MAE = (5.0 + 2.0 + 3.0) / 3 = 3.33
        assert accuracy["mae"] == pytest.approx(3.33, abs=0.01)
        # Avg predicted = (20 + 30 + 15) / 3 = 21.67
        assert accuracy["avg_predicted"] == pytest.approx(21.67, abs=0.01)
        # Avg actual = (25 + 28 + 18) / 3 = 23.67
        assert accuracy["avg_actual"] == pytest.approx(23.67, abs=0.01)
        # Should have correlation
        assert accuracy["correlation"] is not None

    def test_get_prediction_accuracy_criteria_breakdown(self, db):
        """Test per-criteria breakdown in accuracy report."""
        content_id = db.insert_generated_content(
            content_type="x_thread",
            source_commits=["abc"],
            source_messages=["msg"],
            content="Test",
            eval_score=8.0,
            eval_feedback="Good",
        )
        db.insert_prediction(
            content_id=content_id,
            predicted_score=20.0,
            hook_strength=8.0,
            specificity=7.5,
            emotional_resonance=6.0,
            novelty=7.0,
            actionability=5.5,
            prompt_version="v1",
        )
        db.backfill_prediction_actuals(content_id, 25.0)

        accuracy = db.get_prediction_accuracy(days=30)

        assert "criteria_breakdown" in accuracy
        breakdown = accuracy["criteria_breakdown"]
        assert "hook_strength" in breakdown
        assert breakdown["hook_strength"]["avg"] == 8.0
        assert breakdown["hook_strength"]["count"] == 1

    def test_get_prediction_accuracy_filters_by_days(self, db):
        """Test that accuracy calculation respects the days parameter."""
        now = datetime.now(timezone.utc)

        # Insert old prediction (35 days ago)
        content_id_old = db.insert_generated_content(
            content_type="x_thread",
            source_commits=["abc"],
            source_messages=["msg"],
            content="Old content",
            eval_score=8.0,
            eval_feedback="Good",
        )
        pred_id_old = db.insert_prediction(
            content_id=content_id_old,
            predicted_score=20.0,
            prompt_version="v1",
        )
        # Manually set created_at to 35 days ago
        old_time = (now - timedelta(days=35)).isoformat()
        db.conn.execute(
            "UPDATE engagement_predictions SET created_at = ? WHERE id = ?",
            (old_time, pred_id_old),
        )
        db.backfill_prediction_actuals(content_id_old, 25.0)

        # Insert recent prediction (2 days ago)
        content_id_recent = db.insert_generated_content(
            content_type="x_thread",
            source_commits=["def"],
            source_messages=["msg2"],
            content="Recent content",
            eval_score=8.0,
            eval_feedback="Good",
        )
        db.insert_prediction(
            content_id=content_id_recent,
            predicted_score=30.0,
            prompt_version="v1",
        )
        db.backfill_prediction_actuals(content_id_recent, 28.0)
        db.conn.commit()

        # Query for last 30 days should only get the recent one
        accuracy = db.get_prediction_accuracy(days=30)
        assert accuracy["count"] == 1
        assert accuracy["avg_predicted"] == 30.0


class TestPipelinePredictionIntegration:
    """Test prediction integration in the synthesis pipeline."""

    def test_pipeline_result_has_prediction_fields(self):
        """Test that PipelineResult includes prediction fields."""
        from synthesis.pipeline import PipelineResult
        from synthesis.evaluator_v2 import ComparisonResult

        result = PipelineResult(
            batch_id="test-batch",
            candidates=["content"],
            comparison=ComparisonResult(
                ranking=[0],
                best_score=8.0,
                groundedness=7.0,
                rawness=6.0,
                narrative_specificity=7.0,
                voice=8.0,
                engagement_potential=7.5,
                best_feedback="Good",
                improvement="",
                reject_reason=None,
                raw_response="",
            ),
            refinement=None,
            final_content="Final content",
            final_score=8.0,
            source_prompts=["prompt"],
            source_commits=["commit"],
            filter_stats={},
            predicted_engagement=25.0,
            engagement_prediction_detail={
                "predicted_score": 25.0,
                "hook_strength": 8.0,
                "specificity": 7.5,
                "emotional_resonance": 6.0,
                "novelty": 7.0,
                "actionability": 5.5,
                "prompt_version": "v1",
            },
        )

        assert result.predicted_engagement == 25.0
        assert result.engagement_prediction_detail is not None
        assert result.engagement_prediction_detail["hook_strength"] == 8.0

    def test_pipeline_stores_prediction_in_db(self, db):
        """Test that pipeline integration stores predictions correctly."""
        # This is an integration test placeholder
        # In practice, we'd mock the EngagementPredictor and verify storage
        # For now, we just verify the DB methods work correctly

        content_id = db.insert_generated_content(
            content_type="x_thread",
            source_commits=["abc"],
            source_messages=["msg"],
            content="Test content",
            eval_score=8.0,
            eval_feedback="Good",
        )

        # Simulate what poll_commits.py does
        prediction_detail = {
            "predicted_score": 22.5,
            "hook_strength": 7.5,
            "specificity": 7.0,
            "emotional_resonance": 6.5,
            "novelty": 6.0,
            "actionability": 5.5,
            "prompt_version": "v1",
        }

        db.insert_prediction(
            content_id=content_id,
            predicted_score=prediction_detail["predicted_score"],
            hook_strength=prediction_detail.get("hook_strength"),
            specificity=prediction_detail.get("specificity"),
            emotional_resonance=prediction_detail.get("emotional_resonance"),
            novelty=prediction_detail.get("novelty"),
            actionability=prediction_detail.get("actionability"),
            prompt_version=prediction_detail.get("prompt_version", "v1"),
        )

        # Verify stored
        cursor = db.conn.execute(
            "SELECT * FROM engagement_predictions WHERE content_id = ?",
            (content_id,),
        )
        row = cursor.fetchone()
        assert row["predicted_score"] == 22.5
        assert row["hook_strength"] == 7.5


class TestValidationScript:
    """Test the validation script's backfill logic."""

    def test_validation_finds_predictions_needing_backfill(self, db):
        """Test that validation script query finds correct predictions."""
        # Create content with engagement but no backfilled prediction
        content_id = db.insert_generated_content(
            content_type="x_thread",
            source_commits=["abc"],
            source_messages=["msg"],
            content="Test content",
            eval_score=8.0,
            eval_feedback="Good",
        )
        db.mark_published(content_id, "https://x.com/test/1", "tweet-1")

        # Add engagement
        db.insert_engagement(
            content_id=content_id,
            tweet_id="tweet-1",
            like_count=10,
            retweet_count=2,
            reply_count=1,
            quote_count=0,
            engagement_score=15.0,
        )

        # Add prediction (not backfilled)
        db.insert_prediction(
            content_id=content_id,
            predicted_score=20.0,
            prompt_version="v1",
        )

        # Query that validation script uses
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
                 AND ep.actual_engagement_score IS NULL"""
        )
        rows = cursor.fetchall()

        assert len(rows) == 1
        assert rows[0][0] == content_id
        assert rows[0][2] == 15.0  # engagement_score
        assert rows[0][3] == 20.0  # predicted_score

    def test_validation_ignores_already_backfilled(self, db):
        """Test that validation script skips already backfilled predictions."""
        content_id = db.insert_generated_content(
            content_type="x_thread",
            source_commits=["abc"],
            source_messages=["msg"],
            content="Test content",
            eval_score=8.0,
            eval_feedback="Good",
        )
        db.mark_published(content_id, "https://x.com/test/1", "tweet-1")

        db.insert_engagement(
            content_id=content_id,
            tweet_id="tweet-1",
            like_count=10,
            retweet_count=2,
            reply_count=1,
            quote_count=0,
            engagement_score=15.0,
        )

        db.insert_prediction(
            content_id=content_id,
            predicted_score=20.0,
            prompt_version="v1",
        )

        # Backfill it
        db.backfill_prediction_actuals(content_id, 15.0)

        # Query should return nothing
        cursor = db.conn.execute(
            """SELECT gc.id
               FROM generated_content gc
               INNER JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY fetched_at DESC) AS rn
                   FROM post_engagement
               ) pe ON pe.content_id = gc.id AND pe.rn = 1
               INNER JOIN engagement_predictions ep ON ep.content_id = gc.id
               WHERE gc.published = 1
                 AND ep.actual_engagement_score IS NULL"""
        )
        rows = cursor.fetchall()
        assert len(rows) == 0
