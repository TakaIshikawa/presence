"""Tests for build_knowledge.py orchestration and filtering logic."""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


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
        build_mocks.ingest_post.side_effect = [Exception("API error"), None]
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
