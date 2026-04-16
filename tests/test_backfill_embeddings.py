"""Tests for backfill_embeddings.py — one-time embedding backfill for published content."""

import sys
import logging
from pathlib import Path
from unittest.mock import patch, MagicMock, Mock
from dataclasses import dataclass

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from storage.db import Database


# --- Test Fixtures ---


@pytest.fixture
def test_db(tmp_path):
    """Create temporary SQLite database with schema."""
    db_path = tmp_path / "test_backfill.db"
    db = Database(str(db_path))
    db.connect()
    schema_path = Path(__file__).parent.parent / "schema.sql"
    db.init_schema(str(schema_path))
    yield db
    db.close()


@pytest.fixture
def mock_config():
    """Mock config with embeddings settings."""
    @dataclass
    class EmbeddingsConfig:
        api_key: str = "test-api-key"
        model: str = "voyage-3-lite"

    @dataclass
    class MockConfig:
        embeddings: EmbeddingsConfig = None

    config = MockConfig()
    config.embeddings = EmbeddingsConfig()
    return config


@pytest.fixture
def mock_config_no_embeddings():
    """Mock config without embeddings settings."""
    @dataclass
    class MockConfig:
        embeddings = None

    return MockConfig()


@pytest.fixture
def mock_embeddings():
    """Mock VoyageEmbeddings class and its methods."""
    with patch("backfill_embeddings.VoyageEmbeddings") as mock_class:
        mock_instance = Mock()
        # Return a list of 1024-dimensional embeddings (Voyage-3-lite dimension)
        mock_instance.embed_batch.return_value = [
            [0.1] * 1024,  # Mock embedding vector
            [0.2] * 1024,
            [0.3] * 1024,
        ]
        mock_class.return_value = mock_instance
        yield mock_instance


# --- Tests ---


class TestBackfillEmbeddings:
    """Tests for backfill_embeddings script."""

    def test_empty_database_no_error(self, test_db, mock_config, mock_embeddings, caplog):
        """Test that script handles empty database gracefully without errors."""
        with patch("backfill_embeddings.script_context") as mock_context:
            mock_context.return_value.__enter__.return_value = (mock_config, test_db)

            with patch("backfill_embeddings.time.sleep"):
                from backfill_embeddings import main

                with caplog.at_level(logging.INFO):
                    main()

        # Should log that there's nothing to backfill
        assert any("No content to backfill" in record.message for record in caplog.records)
        assert all(record.levelno <= logging.INFO for record in caplog.records)

        # Embedding function should not be called
        mock_embeddings.embed_batch.assert_not_called()

    def test_processes_only_missing_embeddings(self, test_db, mock_config, mock_embeddings, caplog):
        """Test that script only processes rows without embeddings (resumable)."""
        # Create content: 2 without embeddings, 1 with embedding
        test_db.conn.execute(
            """INSERT INTO generated_content (content, content_type, eval_score, published, content_embedding)
               VALUES (?, ?, ?, ?, ?)""",
            ("Content 1", "x_post", 7.0, 1, None)  # No embedding
        )
        test_db.conn.execute(
            """INSERT INTO generated_content (content, content_type, eval_score, published, content_embedding)
               VALUES (?, ?, ?, ?, ?)""",
            ("Content 2", "x_post", 7.0, 1, b'\x00\x01\x02\x03')  # Has embedding
        )
        test_db.conn.execute(
            """INSERT INTO generated_content (content, content_type, eval_score, published, content_embedding)
               VALUES (?, ?, ?, ?, ?)""",
            ("Content 3", "x_post", 7.0, 1, None)  # No embedding
        )
        test_db.conn.execute(
            """INSERT INTO generated_content (content, content_type, eval_score, published, content_embedding)
               VALUES (?, ?, ?, ?, ?)""",
            ("Content 4", "x_post", 7.0, 0, None)  # Not published - should be skipped
        )
        test_db.conn.commit()

        # Mock embed_batch to return exactly 2 embeddings (for Content 1 and Content 3)
        mock_embeddings.embed_batch.return_value = [
            [0.1] * 1024,
            [0.2] * 1024,
        ]

        with patch("backfill_embeddings.script_context") as mock_context:
            mock_context.return_value.__enter__.return_value = (mock_config, test_db)

            with patch("backfill_embeddings.time.sleep"):
                from backfill_embeddings import main

                with caplog.at_level(logging.INFO):
                    main()

        # Should log progress for exactly 2 items
        assert any("Backfilling embeddings for 2 published posts" in record.message for record in caplog.records)
        assert any("Done. Backfilled 2 embeddings" in record.message for record in caplog.records)

        # Embedding function should be called exactly once (batch of 2)
        assert mock_embeddings.embed_batch.call_count == 1
        call_args = mock_embeddings.embed_batch.call_args[0][0]
        assert len(call_args) == 2
        assert "Content 1" in call_args
        assert "Content 3" in call_args

        # Verify embeddings were saved to database
        cursor = test_db.conn.execute(
            """SELECT id, content_embedding FROM generated_content
               WHERE published = 1 ORDER BY id"""
        )
        rows = cursor.fetchall()
        assert rows[0]["content_embedding"] is not None  # Content 1 now has embedding
        assert rows[1]["content_embedding"] is not None  # Content 2 already had embedding
        assert rows[2]["content_embedding"] is not None  # Content 3 now has embedding

    def test_logs_progress_at_info_level(self, test_db, mock_config, mock_embeddings, caplog):
        """Test that script logs progress messages at INFO level."""
        # Create 3 content items without embeddings
        for i in range(3):
            test_db.conn.execute(
                """INSERT INTO generated_content (content, content_type, eval_score, published)
                   VALUES (?, ?, ?, ?)""",
                (f"Content {i}", "x_post", 7.0, 1)
            )
        test_db.conn.commit()

        mock_embeddings.embed_batch.return_value = [
            [0.1] * 1024,
            [0.2] * 1024,
            [0.3] * 1024,
        ]

        with patch("backfill_embeddings.script_context") as mock_context:
            mock_context.return_value.__enter__.return_value = (mock_config, test_db)

            with patch("backfill_embeddings.time.sleep"):
                from backfill_embeddings import main

                with caplog.at_level(logging.INFO):
                    main()

        # Check for expected log messages at INFO level
        info_messages = [record.message for record in caplog.records if record.levelno == logging.INFO]

        assert any("Backfilling embeddings for 3 published posts" in msg for msg in info_messages)
        assert any("Embedded 3/3" in msg for msg in info_messages)
        assert any("Done. Backfilled 3 embeddings" in msg for msg in info_messages)

    def test_embedding_called_once_per_unembedded_row(self, test_db, mock_config, mock_embeddings, caplog):
        """Test that embedding function is called exactly once per row without embeddings."""
        # Create 5 content items without embeddings
        for i in range(5):
            test_db.conn.execute(
                """INSERT INTO generated_content (content, content_type, eval_score, published)
                   VALUES (?, ?, ?, ?)""",
                (f"Content {i}", "x_post", 7.0, 1)
            )
        test_db.conn.commit()

        # Mock to return 5 embeddings
        mock_embeddings.embed_batch.return_value = [
            [float(i)] * 1024 for i in range(5)
        ]

        with patch("backfill_embeddings.script_context") as mock_context:
            mock_context.return_value.__enter__.return_value = (mock_config, test_db)

            with patch("backfill_embeddings.time.sleep"):
                from backfill_embeddings import main
                main()

        # Should call embed_batch exactly once with all 5 texts
        assert mock_embeddings.embed_batch.call_count == 1
        call_args = mock_embeddings.embed_batch.call_args[0][0]
        assert len(call_args) == 5

    def test_batch_processing_with_large_dataset(self, test_db, mock_config, mock_embeddings, caplog):
        """Test that script correctly batches requests for datasets larger than batch size."""
        # Create 45 content items (should trigger 3 batches: 20 + 20 + 5)
        for i in range(45):
            test_db.conn.execute(
                """INSERT INTO generated_content (content, content_type, eval_score, published)
                   VALUES (?, ?, ?, ?)""",
                (f"Content {i}", "x_post", 7.0, 1)
            )
        test_db.conn.commit()

        # Mock to return correct number of embeddings per batch
        def mock_embed_batch(texts):
            return [[0.1] * 1024 for _ in range(len(texts))]

        mock_embeddings.embed_batch.side_effect = mock_embed_batch

        with patch("backfill_embeddings.script_context") as mock_context:
            mock_context.return_value.__enter__.return_value = (mock_config, test_db)

            with patch("backfill_embeddings.time.sleep"):
                from backfill_embeddings import main

                with caplog.at_level(logging.INFO):
                    main()

        # Should call embed_batch 3 times (batch_size=20: 20, 20, 5)
        assert mock_embeddings.embed_batch.call_count == 3

        # Verify batch sizes
        call_lengths = [len(call[0][0]) for call in mock_embeddings.embed_batch.call_args_list]
        assert call_lengths == [20, 20, 5]

        # Verify progress logging
        info_messages = [record.message for record in caplog.records if record.levelno == logging.INFO]
        assert any("Embedded 20/45" in msg for msg in info_messages)
        assert any("Embedded 40/45" in msg for msg in info_messages)
        assert any("Embedded 45/45" in msg for msg in info_messages)
        assert any("Done. Backfilled 45 embeddings" in msg for msg in info_messages)

    def test_no_embeddings_config_exits_gracefully(self, test_db, mock_config_no_embeddings, caplog):
        """Test that script exits gracefully when embeddings config is missing."""
        with patch("backfill_embeddings.script_context") as mock_context:
            mock_context.return_value.__enter__.return_value = (mock_config_no_embeddings, test_db)

            from backfill_embeddings import main

            with caplog.at_level(logging.WARNING):
                main()

        # Should log warning and exit
        assert any(
            record.levelno == logging.WARNING and "No embeddings config found" in record.message
            for record in caplog.records
        )

    def test_rate_limit_handling(self, test_db, mock_config, mock_embeddings, caplog):
        """Test that script handles rate limiting with retry logic."""
        # Create 2 content items
        for i in range(2):
            test_db.conn.execute(
                """INSERT INTO generated_content (content, content_type, eval_score, published)
                   VALUES (?, ?, ?, ?)""",
                (f"Content {i}", "x_post", 7.0, 1)
            )
        test_db.conn.commit()

        # Mock rate limit error on first call, success on second
        class RateLimitError(Exception):
            """Mock rate limit exception."""
            pass

        rate_limit_error = RateLimitError("429 Rate Limit Exceeded")

        mock_embeddings.embed_batch.side_effect = [
            rate_limit_error,
            [[0.1] * 1024, [0.2] * 1024]
        ]

        with patch("backfill_embeddings.script_context") as mock_context:
            mock_context.return_value.__enter__.return_value = (mock_config, test_db)

            with patch("backfill_embeddings.time.sleep") as mock_sleep:
                from backfill_embeddings import main

                with caplog.at_level(logging.WARNING):
                    main()

        # Should log rate limit warning
        assert any(
            record.levelno == logging.WARNING and "Rate limited" in record.message
            for record in caplog.records
        )

        # Should call embed_batch twice (first fails, second succeeds)
        assert mock_embeddings.embed_batch.call_count == 2

        # Should sleep for 25 seconds on rate limit
        assert any(call[0][0] == 25 for call in mock_sleep.call_args_list)
