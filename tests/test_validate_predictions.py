"""Tests for validate_predictions.py — CLI entry point for validating and backfilling predictions."""

import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from validate_predictions import main, validate_prediction_row
from storage.db import Database


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
def db_with_valid_predictions(test_db):
    """Database with valid predictions needing backfill."""
    now = datetime.now(timezone.utc)

    # Create published content with valid predictions and engagement data
    for i in range(3):
        # Insert content
        content_id = test_db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content=f"Valid test content {i+1}",
            eval_score=7.0,
            eval_feedback="Test content"
        )

        # Mark as published
        test_db.conn.execute(
            "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
            ((now - timedelta(days=i)).isoformat(), content_id)
        )

        # Insert engagement data
        test_db.conn.execute(
            """INSERT INTO post_engagement
               (content_id, tweet_id, like_count, retweet_count, reply_count,
                quote_count, engagement_score, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (content_id, f"tweet_{i+1}", 10, 2, 1, 0, 5.5 + i, now.isoformat())
        )

        # Insert prediction (valid, no actuals yet)
        test_db.conn.execute(
            """INSERT INTO engagement_predictions
               (content_id, predicted_score, hook_strength, specificity,
                emotional_resonance, novelty, actionability, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (content_id, 6.0 + i * 0.5, 7.0, 6.0, 6.0, 5.0, 6.0,
             (now - timedelta(days=i)).isoformat())
        )

    test_db.conn.commit()
    return test_db


@pytest.fixture
def db_with_invalid_predictions(test_db):
    """Database with invalid predictions (out-of-range scores)."""
    now = datetime.now(timezone.utc)

    # Case 1: predicted_score out of range (> 10)
    content_id_1 = test_db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Content with invalid predicted_score",
        eval_score=7.0,
        eval_feedback="Test"
    )
    test_db.conn.execute(
        "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
        (now.isoformat(), content_id_1)
    )
    test_db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, engagement_score, fetched_at)
           VALUES (?, ?, ?, ?)""",
        (content_id_1, "tweet_1", 5.0, now.isoformat())
    )
    test_db.conn.execute(
        """INSERT INTO engagement_predictions
           (content_id, predicted_score, created_at)
           VALUES (?, ?, ?)""",
        (content_id_1, 15.0, now.isoformat())  # Invalid: > 10
    )

    # Case 2: predicted_score negative
    content_id_2 = test_db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Content with negative predicted_score",
        eval_score=7.0,
        eval_feedback="Test"
    )
    test_db.conn.execute(
        "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
        (now.isoformat(), content_id_2)
    )
    test_db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, engagement_score, fetched_at)
           VALUES (?, ?, ?, ?)""",
        (content_id_2, "tweet_2", 5.0, now.isoformat())
    )
    test_db.conn.execute(
        """INSERT INTO engagement_predictions
           (content_id, predicted_score, created_at)
           VALUES (?, ?, ?)""",
        (content_id_2, -2.0, now.isoformat())  # Invalid: < 0
    )

    # Case 3: negative actual engagement_score
    content_id_3 = test_db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Content with negative engagement_score",
        eval_score=7.0,
        eval_feedback="Test"
    )
    test_db.conn.execute(
        "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
        (now.isoformat(), content_id_3)
    )
    test_db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, engagement_score, fetched_at)
           VALUES (?, ?, ?, ?)""",
        (content_id_3, "tweet_3", -5.0, now.isoformat())  # Invalid: negative
    )
    test_db.conn.execute(
        """INSERT INTO engagement_predictions
           (content_id, predicted_score, created_at)
           VALUES (?, ?, ?)""",
        (content_id_3, 6.0, now.isoformat())
    )

    test_db.conn.commit()
    return test_db


@pytest.fixture
def db_with_mixed_predictions(test_db):
    """Database with both valid and invalid predictions."""
    now = datetime.now(timezone.utc)

    # Valid prediction
    content_id_valid = test_db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Valid content",
        eval_score=7.0,
        eval_feedback="Test"
    )
    test_db.conn.execute(
        "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
        (now.isoformat(), content_id_valid)
    )
    test_db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, engagement_score, fetched_at)
           VALUES (?, ?, ?, ?)""",
        (content_id_valid, "tweet_valid", 5.0, now.isoformat())
    )
    test_db.conn.execute(
        """INSERT INTO engagement_predictions
           (content_id, predicted_score, created_at)
           VALUES (?, ?, ?)""",
        (content_id_valid, 6.0, now.isoformat())
    )

    # Invalid prediction (out of range)
    content_id_invalid = test_db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Invalid content",
        eval_score=7.0,
        eval_feedback="Test"
    )
    test_db.conn.execute(
        "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
        (now.isoformat(), content_id_invalid)
    )
    test_db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, engagement_score, fetched_at)
           VALUES (?, ?, ?, ?)""",
        (content_id_invalid, "tweet_invalid", 5.0, now.isoformat())
    )
    test_db.conn.execute(
        """INSERT INTO engagement_predictions
           (content_id, predicted_score, created_at)
           VALUES (?, ?, ?)""",
        (content_id_invalid, 12.0, now.isoformat())  # Invalid: > 10
    )

    test_db.conn.commit()
    return test_db


@pytest.fixture
def empty_db(test_db):
    """Database with no predictions needing backfill."""
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


# --- Test validate_prediction_row ---


class TestValidatePredictionRow:
    """Test the validate_prediction_row function."""

    def test_valid_prediction(self):
        """Test validation of valid prediction data."""
        is_valid, error = validate_prediction_row(
            content_id=1,
            predicted_score=7.5,
            actual_score=8.0
        )
        assert is_valid is True
        assert error is None

    def test_valid_prediction_no_actual(self):
        """Test validation with no actual score yet (before backfill)."""
        is_valid, error = validate_prediction_row(
            content_id=1,
            predicted_score=7.5,
            actual_score=None
        )
        assert is_valid is True
        assert error is None

    def test_valid_prediction_boundary_values(self):
        """Test validation at boundary values (0 and 10)."""
        # Lower boundary
        is_valid, error = validate_prediction_row(
            content_id=1,
            predicted_score=0.0,
            actual_score=0.0
        )
        assert is_valid is True
        assert error is None

        # Upper boundary
        is_valid, error = validate_prediction_row(
            content_id=2,
            predicted_score=10.0,
            actual_score=100.0  # Actual can be > 10
        )
        assert is_valid is True
        assert error is None

    def test_missing_content_id(self):
        """Test validation fails when content_id is None."""
        is_valid, error = validate_prediction_row(
            content_id=None,
            predicted_score=7.5,
            actual_score=8.0
        )
        assert is_valid is False
        assert "Missing content_id" in error

    def test_missing_predicted_score(self):
        """Test validation fails when predicted_score is None."""
        is_valid, error = validate_prediction_row(
            content_id=1,
            predicted_score=None,
            actual_score=8.0
        )
        assert is_valid is False
        assert "Missing predicted_score" in error
        assert "Content 1" in error

    def test_predicted_score_out_of_range_high(self):
        """Test validation fails when predicted_score > 10."""
        is_valid, error = validate_prediction_row(
            content_id=1,
            predicted_score=15.0,
            actual_score=8.0
        )
        assert is_valid is False
        assert "out of range" in error
        assert "15.0" in error
        assert "Content 1" in error

    def test_predicted_score_out_of_range_low(self):
        """Test validation fails when predicted_score < 0."""
        is_valid, error = validate_prediction_row(
            content_id=2,
            predicted_score=-2.0,
            actual_score=8.0
        )
        assert is_valid is False
        assert "out of range" in error
        assert "-2.0" in error
        assert "Content 2" in error

    def test_negative_actual_score(self):
        """Test validation fails when actual engagement_score is negative."""
        is_valid, error = validate_prediction_row(
            content_id=3,
            predicted_score=7.0,
            actual_score=-5.0
        )
        assert is_valid is False
        assert "cannot be negative" in error
        assert "-5.0" in error
        assert "Content 3" in error


# --- Test main function ---


class TestMainFunction:
    """Test the main CLI entry point."""

    def test_main_with_valid_predictions(self, db_with_valid_predictions, capsys, tmp_path):
        """Test main succeeds and backfills valid predictions."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("validate_predictions.script_context") as mock_context, \
             patch("validate_predictions.update_monitoring") as mock_monitoring:

            mock_context.return_value.__enter__ = lambda self: (mock_config, db_with_valid_predictions)
            mock_context.return_value.__exit__ = lambda self, *args: None

            # Run main
            exit_code = main()

            # Should succeed
            assert exit_code == 0
            mock_monitoring.assert_called_once_with("validate_predictions")

        # Capture output
        captured = capsys.readouterr()

        # Verify validation and backfilling happened
        assert "Validating 3 predictions before backfilling" in captured.out
        assert "All predictions valid" in captured.out
        assert "Backfilling 3 predictions with actual engagement" in captured.out
        assert "predicted=" in captured.out
        assert "actual=" in captured.out
        assert "error=" in captured.out

        # Verify predictions were actually backfilled
        cursor = db_with_valid_predictions.conn.execute(
            "SELECT COUNT(*) FROM engagement_predictions WHERE actual_engagement_score IS NOT NULL"
        )
        count = cursor.fetchone()[0]
        assert count == 3

        # Verify accuracy report was printed
        assert "Prediction Accuracy (last 30 days)" in captured.out
        assert "Total predictions: 3" in captured.out
        assert "Mean Absolute Error:" in captured.out

    def test_main_with_invalid_predictions(self, db_with_invalid_predictions, capsys, tmp_path):
        """Test main fails and reports invalid predictions."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("validate_predictions.script_context") as mock_context, \
             patch("validate_predictions.update_monitoring") as mock_monitoring:

            mock_context.return_value.__enter__ = lambda self: (mock_config, db_with_invalid_predictions)
            mock_context.return_value.__exit__ = lambda self, *args: None

            # Run main
            exit_code = main()

            # Should fail
            assert exit_code == 1
            mock_monitoring.assert_called_once_with("validate_predictions")

        # Capture output
        captured = capsys.readouterr()
        combined = captured.out + captured.err

        # Verify validation errors were reported
        assert "Validating 3 predictions before backfilling" in combined
        assert "INVALID:" in combined
        assert "Validation failed: 3 invalid prediction(s) found" in combined

        # Check specific error messages
        assert "predicted_score 15.0 out of range" in combined  # Case 1
        assert "predicted_score -2.0 out of range" in combined  # Case 2
        assert "engagement_score -5.0 cannot be negative" in combined  # Case 3

        # Verify NO predictions were backfilled (validation failed)
        cursor = db_with_invalid_predictions.conn.execute(
            "SELECT COUNT(*) FROM engagement_predictions WHERE actual_engagement_score IS NOT NULL"
        )
        count = cursor.fetchone()[0]
        assert count == 0

        # Verify accuracy report was NOT printed (validation failed)
        assert "Prediction Accuracy" not in combined

    def test_main_with_mixed_predictions(self, db_with_mixed_predictions, capsys, tmp_path):
        """Test main fails when any prediction is invalid (mixed valid/invalid)."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("validate_predictions.script_context") as mock_context, \
             patch("validate_predictions.update_monitoring") as mock_monitoring:

            mock_context.return_value.__enter__ = lambda self: (mock_config, db_with_mixed_predictions)
            mock_context.return_value.__exit__ = lambda self, *args: None

            # Run main
            exit_code = main()

            # Should fail (even though some are valid)
            assert exit_code == 1
            mock_monitoring.assert_called_once_with("validate_predictions")

        # Capture output
        captured = capsys.readouterr()
        combined = captured.out + captured.err

        # Verify validation errors were reported
        assert "Validating 2 predictions before backfilling" in combined
        assert "Validation failed: 1 invalid prediction(s) found" in combined
        assert "predicted_score 12.0 out of range" in combined

        # Verify NO predictions were backfilled (validation failed for at least one)
        cursor = db_with_mixed_predictions.conn.execute(
            "SELECT COUNT(*) FROM engagement_predictions WHERE actual_engagement_score IS NOT NULL"
        )
        count = cursor.fetchone()[0]
        assert count == 0

    def test_main_with_empty_db(self, empty_db, capsys, tmp_path):
        """Test main succeeds when no predictions need backfilling."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("validate_predictions.script_context") as mock_context, \
             patch("validate_predictions.update_monitoring") as mock_monitoring:

            mock_context.return_value.__enter__ = lambda self: (mock_config, empty_db)
            mock_context.return_value.__exit__ = lambda self, *args: None

            # Run main
            exit_code = main()

            # Should succeed
            assert exit_code == 0
            mock_monitoring.assert_called_once_with("validate_predictions")

        # Capture output
        captured = capsys.readouterr()

        # Verify appropriate message
        assert "No predictions need backfilling" in captured.out
        assert "No predictions with actual engagement yet" in captured.out

    def test_main_logs_individual_predictions(self, db_with_valid_predictions, capsys, tmp_path):
        """Test that main logs each prediction with identifying info."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("validate_predictions.script_context") as mock_context, \
             patch("validate_predictions.update_monitoring"):

            mock_context.return_value.__enter__ = lambda self: (mock_config, db_with_valid_predictions)
            mock_context.return_value.__exit__ = lambda self, *args: None

            main()

        captured = capsys.readouterr()

        # Verify each prediction is logged with content_id and scores
        assert "Content 1:" in captured.out
        assert "Content 2:" in captured.out
        assert "Content 3:" in captured.out

        # Verify predicted and actual scores are shown
        assert "predicted=" in captured.out
        assert "actual=" in captured.out
        assert "error=" in captured.out

    def test_main_reports_invalid_rows_with_identifying_info(
        self, db_with_invalid_predictions, capsys, tmp_path
    ):
        """Test that main reports invalid rows with content_id and specific errors."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("validate_predictions.script_context") as mock_context, \
             patch("validate_predictions.update_monitoring"):

            mock_context.return_value.__enter__ = lambda self: (mock_config, db_with_invalid_predictions)
            mock_context.return_value.__exit__ = lambda self, *args: None

            exit_code = main()

        assert exit_code == 1
        captured = capsys.readouterr()
        combined = captured.out + captured.err

        # Each invalid row should be reported with content_id
        assert "Content 1:" in combined
        assert "Content 2:" in combined
        assert "Content 3:" in combined

        # Specific error details should be included
        assert "15.0 out of range" in combined
        assert "-2.0 out of range" in combined
        assert "-5.0 cannot be negative" in combined

    def test_main_accuracy_summary_when_valid(self, db_with_valid_predictions, capsys, tmp_path):
        """Test accuracy summary is shown after successful backfill."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("validate_predictions.script_context") as mock_context, \
             patch("validate_predictions.update_monitoring"):

            mock_context.return_value.__enter__ = lambda self: (mock_config, db_with_valid_predictions)
            mock_context.return_value.__exit__ = lambda self, *args: None

            exit_code = main()

        assert exit_code == 0
        captured = capsys.readouterr()

        # Accuracy summary should be present
        assert "Prediction Accuracy (last 30 days):" in captured.out
        assert "Total predictions:" in captured.out
        assert "Mean Absolute Error:" in captured.out
        assert "Avg Predicted:" in captured.out
        assert "Avg Actual:" in captured.out

    def test_main_no_accuracy_summary_when_invalid(
        self, db_with_invalid_predictions, capsys, tmp_path
    ):
        """Test accuracy summary is NOT shown when validation fails."""
        mock_config = MagicMock()
        mock_config.paths.database = str(tmp_path / "test.db")

        with patch("validate_predictions.script_context") as mock_context, \
             patch("validate_predictions.update_monitoring"):

            mock_context.return_value.__enter__ = lambda self: (mock_config, db_with_invalid_predictions)
            mock_context.return_value.__exit__ = lambda self, *args: None

            exit_code = main()

        assert exit_code == 1
        captured = capsys.readouterr()
        combined = captured.out + captured.err

        # Accuracy summary should NOT be present (validation failed before backfill)
        assert "Prediction Accuracy" not in combined
        assert "Mean Absolute Error:" not in combined
