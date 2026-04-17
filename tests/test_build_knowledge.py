"""Tests for build_knowledge.py orchestration and filtering logic."""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.ingest import InsightExtractionError
from knowledge.embeddings import EmbeddingError


def _make_config(embeddings_enabled=True):
    config = MagicMock()
    config.paths.database = ":memory:"
    config.paths.claude_logs = "/tmp/fake_logs"
    config.anthropic.api_key = "test-key"
    config.synthesis.model = "test-model"
    config.github.username = "testuser"
    if embeddings_enabled:
        config.embeddings.provider = "voyage"
        config.embeddings.api_key = "embed-key"
        config.embeddings.model = "embed-model"
    else:
        config.embeddings = None
    return config


def _make_message(uuid="uuid-1", prompt_text="Working on a complex feature", project_path="/project"):
    msg = MagicMock()
    msg.message_uuid = uuid
    msg.prompt_text = prompt_text
    msg.project_path = project_path
    return msg


@pytest.fixture
def build_mocks():
    """Set up common mocks for build_knowledge tests."""
    with patch("build_knowledge.time.sleep"), \
         patch("build_knowledge.ingest_own_post") as mock_ingest_post, \
         patch("build_knowledge.ingest_own_conversation") as mock_ingest_conv, \
         patch("build_knowledge.InsightExtractor") as MockExtractor, \
         patch("build_knowledge.KnowledgeStore") as MockStore, \
         patch("build_knowledge.get_embedding_provider") as mock_embedder, \
         patch("build_knowledge.ClaudeLogParser") as MockParser, \
         patch("build_knowledge.Database") as MockDB, \
         patch("build_knowledge.load_config") as mock_config:
        yield SimpleNamespace(
            config=mock_config,
            db=MockDB.return_value,
            parser=MockParser.return_value,
            store=MockStore.return_value,
            extractor=MockExtractor.return_value,
            ingest_post=mock_ingest_post,
            ingest_conv=mock_ingest_conv,
        )


class TestMain:
    def test_embeddings_not_configured(self, caplog):
        with patch("build_knowledge.load_config") as mock_config:
            mock_config.return_value = _make_config(embeddings_enabled=False)
            from build_knowledge import main
            with pytest.raises(SystemExit):
                main()
            assert "embeddings not configured" in caplog.text

    def test_skips_existing_posts(self, build_mocks):
        build_mocks.config.return_value = _make_config()
        # DB returns 2 published posts
        build_mocks.db.conn.execute.return_value.fetchall.return_value = [
            {"id": 1, "content": "Post 1", "published_url": "https://x.com/status/1"},
            {"id": 2, "content": "Post 2", "published_url": "https://x.com/status/2"},
        ]
        # First already exists, second doesn't
        build_mocks.store.exists.side_effect = [True, False]
        # No conversations
        build_mocks.parser.get_messages_since.return_value = []

        from build_knowledge import main
        main()

        # Only second post ingested
        assert build_mocks.ingest_post.call_count == 1

    def test_filters_short_conversations(self, build_mocks):
        build_mocks.config.return_value = _make_config()
        build_mocks.db.conn.execute.return_value.fetchall.return_value = []
        build_mocks.parser.get_messages_since.return_value = [
            _make_message(uuid="uuid-short", prompt_text="hi"),  # < 100 chars
            _make_message(uuid="uuid-long", prompt_text="A" * 150),  # >= 100 chars
        ]
        build_mocks.store.exists.return_value = False
        build_mocks.ingest_conv.return_value = True

        from build_knowledge import main
        main()

        # Only the long conversation ingested
        assert build_mocks.ingest_conv.call_count == 1

    def test_conversations_capped_at_50(self, build_mocks):
        build_mocks.config.return_value = _make_config()
        build_mocks.db.conn.execute.return_value.fetchall.return_value = []
        # 60 substantial messages
        build_mocks.parser.get_messages_since.return_value = [
            _make_message(uuid=f"uuid-{i}", prompt_text="A" * 150)
            for i in range(60)
        ]
        build_mocks.store.exists.return_value = False
        build_mocks.ingest_conv.return_value = True

        from build_knowledge import main
        main()

        assert build_mocks.ingest_conv.call_count == 50

    def test_ingest_error_continues(self, build_mocks):
        build_mocks.config.return_value = _make_config()
        build_mocks.db.conn.execute.return_value.fetchall.return_value = [
            {"id": 1, "content": "Post 1", "published_url": "https://x.com/status/1"},
            {"id": 2, "content": "Post 2", "published_url": "https://x.com/status/2"},
        ]
        build_mocks.store.exists.return_value = False
        build_mocks.ingest_post.side_effect = [InsightExtractionError("API error"), None]
        build_mocks.parser.get_messages_since.return_value = []

        from build_knowledge import main
        main()

        # Both posts attempted despite first failing
        assert build_mocks.ingest_post.call_count == 2

    def test_empty_database_and_no_conversations(self, build_mocks):
        build_mocks.config.return_value = _make_config()
        build_mocks.db.conn.execute.return_value.fetchall.return_value = []
        build_mocks.parser.get_messages_since.return_value = []

        from build_knowledge import main
        main()

        build_mocks.ingest_post.assert_not_called()
        build_mocks.ingest_conv.assert_not_called()

    def test_skips_existing_conversations(self, build_mocks):
        build_mocks.config.return_value = _make_config()
        build_mocks.db.conn.execute.return_value.fetchall.return_value = []
        build_mocks.parser.get_messages_since.return_value = [
            _make_message(uuid="uuid-exists", prompt_text="A" * 150),
            _make_message(uuid="uuid-new", prompt_text="B" * 150),
        ]
        # First exists, second doesn't
        build_mocks.store.exists.side_effect = [True, False]
        build_mocks.ingest_conv.return_value = True

        from build_knowledge import main
        main()

        # Only second conversation ingested
        assert build_mocks.ingest_conv.call_count == 1
        build_mocks.ingest_conv.assert_called_once_with(
            store=build_mocks.store,
            extractor=build_mocks.extractor,
            message_uuid="uuid-new",
            prompt="B" * 150,
            project_path="/project"
        )

    def test_conversation_ingest_error_continues(self, build_mocks):
        build_mocks.config.return_value = _make_config()
        build_mocks.db.conn.execute.return_value.fetchall.return_value = []
        build_mocks.parser.get_messages_since.return_value = [
            _make_message(uuid="uuid-1", prompt_text="A" * 150),
            _make_message(uuid="uuid-2", prompt_text="B" * 150),
        ]
        build_mocks.store.exists.return_value = False
        # First fails, second succeeds
        build_mocks.ingest_conv.side_effect = [InsightExtractionError("Extraction failed"), True]

        from build_knowledge import main
        main()

        # Both conversations attempted despite first failing
        assert build_mocks.ingest_conv.call_count == 2

    def test_conversation_ingest_returns_false(self, build_mocks):
        build_mocks.config.return_value = _make_config()
        build_mocks.db.conn.execute.return_value.fetchall.return_value = []
        build_mocks.parser.get_messages_since.return_value = [
            _make_message(uuid="uuid-1", prompt_text="A" * 150),
            _make_message(uuid="uuid-2", prompt_text="B" * 150),
        ]
        build_mocks.store.exists.return_value = False
        # First returns False (not ingested), second returns True
        build_mocks.ingest_conv.side_effect = [False, True]

        from build_knowledge import main
        main()

        # Both attempted
        assert build_mocks.ingest_conv.call_count == 2

    def test_rate_limiting_called_after_post_ingest(self, build_mocks):
        build_mocks.config.return_value = _make_config()
        build_mocks.db.conn.execute.return_value.fetchall.return_value = [
            {"id": 1, "content": "Post 1", "published_url": "https://x.com/status/1"},
        ]
        build_mocks.store.exists.return_value = False
        build_mocks.parser.get_messages_since.return_value = []

        with patch("build_knowledge.time.sleep") as mock_sleep:
            from build_knowledge import main
            main()

            # Should call sleep once after ingesting the post
            mock_sleep.assert_called_once_with(25)  # API_DELAY_SECONDS

    def test_rate_limiting_called_after_conversation_ingest(self, build_mocks):
        build_mocks.config.return_value = _make_config()
        build_mocks.db.conn.execute.return_value.fetchall.return_value = []
        build_mocks.parser.get_messages_since.return_value = [
            _make_message(uuid="uuid-1", prompt_text="A" * 150),
        ]
        build_mocks.store.exists.return_value = False
        build_mocks.ingest_conv.return_value = True

        with patch("build_knowledge.time.sleep") as mock_sleep:
            from build_knowledge import main
            main()

            # Should call sleep once after ingesting the conversation
            mock_sleep.assert_called_once_with(25)  # API_DELAY_SECONDS

    def test_rate_limiting_called_on_error(self, build_mocks):
        build_mocks.config.return_value = _make_config()
        build_mocks.db.conn.execute.return_value.fetchall.return_value = [
            {"id": 1, "content": "Post 1", "published_url": "https://x.com/status/1"},
        ]
        build_mocks.store.exists.return_value = False
        build_mocks.ingest_post.side_effect = EmbeddingError("API error")
        build_mocks.parser.get_messages_since.return_value = []

        with patch("build_knowledge.time.sleep") as mock_sleep:
            from build_knowledge import main
            main()

            # Should still call sleep even on error
            mock_sleep.assert_called_once_with(25)  # API_DELAY_SECONDS

    def test_database_schema_initialized(self, build_mocks):
        build_mocks.config.return_value = _make_config()
        build_mocks.db.conn.execute.return_value.fetchall.return_value = []
        build_mocks.parser.get_messages_since.return_value = []

        from build_knowledge import main
        main()

        # Verify Database was initialized and connected
        build_mocks.db.connect.assert_called_once()
        # Verify schema initialization was called
        assert build_mocks.db.init_schema.call_count == 1
        # Verify the schema path ends with schema.sql
        schema_path = build_mocks.db.init_schema.call_args[0][0]
        assert schema_path.endswith("schema.sql")

    def test_successful_conversation_ingestion_increments_counter(self, build_mocks):
        import logging
        build_mocks.config.return_value = _make_config()
        build_mocks.db.conn.execute.return_value.fetchall.return_value = []
        build_mocks.parser.get_messages_since.return_value = [
            _make_message(uuid="uuid-1", prompt_text="A" * 150),
            _make_message(uuid="uuid-2", prompt_text="B" * 150),
            _make_message(uuid="uuid-3", prompt_text="C" * 150),
        ]
        build_mocks.store.exists.return_value = False
        # First returns False, second and third return True
        build_mocks.ingest_conv.side_effect = [False, True, True]

        # Capture logs by patching logging.getLogger
        mock_logger = MagicMock()
        with patch("logging.getLogger", return_value=mock_logger):
            from build_knowledge import main
            main()

            # Verify logger.info was called with the ingested count
            info_calls = [str(call[0][0]) for call in mock_logger.info.call_args_list]
            assert any("Ingested 2 new conversations" in call for call in info_calls)
