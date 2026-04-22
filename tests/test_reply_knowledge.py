"""Tests for knowledge-augmented reply drafting."""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from engagement.reply_drafter import ReplyDrafter, ReplyDraft
from knowledge.store import KnowledgeStore, KnowledgeItem
from storage.db import Database


# --- ReplyDraft dataclass ---


class TestReplyDraft:
    def test_reply_draft_has_text_and_knowledge_ids(self):
        draft = ReplyDraft(
            reply_text="Great question! Here's what I learned...",
            knowledge_ids=[(1, 0.85), (3, 0.72)]
        )
        assert draft.reply_text == "Great question! Here's what I learned..."
        assert len(draft.knowledge_ids) == 2
        assert draft.knowledge_ids[0] == (1, 0.85)


# --- ReplyDrafter with knowledge_store ---


class TestReplyDrafterWithKnowledge:
    @pytest.fixture
    def mock_embedder(self):
        """Mock embedder that returns simple vectors."""
        embedder = MagicMock()
        embedder.embed.return_value = [0.1] * 1536
        return embedder

    @pytest.fixture
    def in_memory_db(self):
        """Create an in-memory database with schema."""
        db = Database(db_path=":memory:")
        db.connect()
        # Initialize schema
        schema = """
        CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            source_url TEXT,
            author TEXT,
            content TEXT NOT NULL,
            insight TEXT,
            embedding BLOB,
            attribution_required INTEGER DEFAULT 1,
            license TEXT DEFAULT 'attribution_required',
            approved INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_type, source_id)
        );
        CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inbound_tweet_id TEXT UNIQUE NOT NULL,
            draft_text TEXT
        );
        CREATE TABLE reply_knowledge_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reply_queue_id INTEGER,
            knowledge_id INTEGER,
            relevance_score REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
        db.conn.executescript(schema)
        db.conn.commit()
        yield db
        db.close()

    @pytest.fixture
    def knowledge_store(self, in_memory_db, mock_embedder):
        """Create a KnowledgeStore with test data."""
        store = KnowledgeStore(in_memory_db.conn, mock_embedder)

        # Add some test knowledge items
        item1 = KnowledgeItem(
            id=None,
            source_type="own_post",
            source_id="tweet_123",
            source_url="https://x.com/user/status/123",
            author="taka",
            content="Testing in production is underrated. Found 3 bugs in first hour.",
            insight="Testing in prod catches real-world issues quickly",
            embedding=None,
            attribution_required=False,
            approved=True,
            created_at=None,
        )

        item2 = KnowledgeItem(
            id=None,
            source_type="own_conversation",
            source_id="msg_456",
            source_url=None,
            author="taka",
            content="Long discussion about distributed systems and eventual consistency",
            insight="Eventual consistency requires careful UX design to handle conflicts gracefully",
            embedding=None,
            attribution_required=False,
            approved=True,
            created_at=None,
        )

        store.add_item(item1)
        store.add_item(item2)

        return store

    def test_init_accepts_knowledge_store(self, knowledge_store):
        with patch("engagement.reply_drafter.anthropic.Anthropic"):
            drafter = ReplyDrafter(
                api_key="sk-test",
                model="test-model",
                knowledge_store=knowledge_store
            )
            assert drafter.knowledge_store is knowledge_store

    def test_init_works_without_knowledge_store(self):
        with patch("engagement.reply_drafter.anthropic.Anthropic"):
            drafter = ReplyDrafter(
                api_key="sk-test",
                model="test-model",
                knowledge_store=None
            )
            assert drafter.knowledge_store is None

    def test_retrieve_reply_context_returns_empty_when_no_store(self):
        with patch("engagement.reply_drafter.anthropic.Anthropic"):
            drafter = ReplyDrafter(
                api_key="sk-test",
                model="test-model",
                knowledge_store=None
            )
            results = drafter._retrieve_reply_context("our post", "their reply")
            assert results == []

    def test_retrieve_reply_context_searches_knowledge(self, knowledge_store):
        with patch("engagement.reply_drafter.anthropic.Anthropic"):
            drafter = ReplyDrafter(
                api_key="sk-test",
                model="test-model",
                knowledge_store=knowledge_store
            )
            # Mock search to return a result
            with patch.object(knowledge_store, 'search_similar') as mock_search:
                mock_item = MagicMock()
                mock_item.id = 1
                mock_item.insight = "Testing insight"
                mock_search.return_value = [(mock_item, 0.85)]

                results = drafter._retrieve_reply_context("Testing question", "their reply")

                # Verify search was called with correct params
                mock_search.assert_called_once()
                call_kwargs = mock_search.call_args[1]
                assert call_kwargs['source_types'] == ['own_post', 'own_conversation']
                assert call_kwargs['limit'] == 3
                assert call_kwargs['min_similarity'] == 0.45

                # Verify results
                assert len(results) == 1
                assert results[0][1] == 0.85

    def test_build_knowledge_section_empty(self):
        with patch("engagement.reply_drafter.anthropic.Anthropic"):
            drafter = ReplyDrafter(api_key="sk-test", model="test-model")
            section = drafter._build_knowledge_section([])
            assert section == ""

    def test_build_knowledge_section_formats_insights(self):
        with patch("engagement.reply_drafter.anthropic.Anthropic"):
            drafter = ReplyDrafter(api_key="sk-test", model="test-model")

            item1 = MagicMock()
            item1.insight = "Testing in prod is valuable"
            item1.content = "Long content..."

            item2 = MagicMock()
            item2.insight = None
            item2.content = "A" * 200  # Long content

            section = drafter._build_knowledge_section([(item1, 0.9), (item2, 0.7)])

            assert "## Your Relevant Past Insights" in section
            assert "Testing in prod is valuable" in section
            # Should truncate content to 150 chars when no insight
            assert len([line for line in section.split('\n') if line.startswith('- ')]) == 2

    def test_draft_backward_compatible_returns_string(self, knowledge_store):
        with patch("engagement.reply_drafter.anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            mock_response = MagicMock()
            mock_response.content = [MagicMock(text="Great point!")]
            mock_client.messages.create.return_value = mock_response

            drafter = ReplyDrafter(
                api_key="sk-test",
                model="test-model",
                knowledge_store=knowledge_store
            )

            result = drafter.draft("our post", "their reply", "them", "me")

            # Should return just the string for backward compatibility
            assert isinstance(result, str)
            assert result == "Great point!"

    def test_draft_with_lineage_returns_reply_draft(self, knowledge_store):
        with patch("engagement.reply_drafter.anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            mock_response = MagicMock()
            mock_response.content = [MagicMock(text="Great point!")]
            mock_client.messages.create.return_value = mock_response

            drafter = ReplyDrafter(
                api_key="sk-test",
                model="test-model",
                knowledge_store=knowledge_store
            )

            # Mock knowledge retrieval to return items
            with patch.object(drafter, '_retrieve_reply_context') as mock_retrieve:
                mock_item = MagicMock()
                mock_item.id = 5
                mock_item.insight = "Test insight"
                mock_retrieve.return_value = [(mock_item, 0.88)]

                result = drafter.draft_with_lineage("our post", "their reply", "them", "me")

                assert isinstance(result, ReplyDraft)
                assert result.reply_text == "Great point!"
                assert len(result.knowledge_ids) == 1
                assert result.knowledge_ids[0] == (5, 0.88)

    def test_draft_with_lineage_includes_knowledge_in_prompt(self, knowledge_store):
        with patch("engagement.reply_drafter.anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            mock_response = MagicMock()
            mock_response.content = [MagicMock(text="Reply text")]
            mock_client.messages.create.return_value = mock_response

            drafter = ReplyDrafter(
                api_key="sk-test",
                model="test-model",
                knowledge_store=knowledge_store
            )

            # Mock knowledge retrieval
            with patch.object(drafter, '_retrieve_reply_context') as mock_retrieve:
                mock_item = MagicMock()
                mock_item.id = 1
                mock_item.insight = "Testing in production reveals real issues"
                mock_retrieve.return_value = [(mock_item, 0.85)]

                drafter.draft_with_lineage("our post", "their reply", "them", "me")

                # Check that knowledge section was included in prompt
                prompt = mock_client.messages.create.call_args[1]["messages"][0]["content"]
                assert "## Your Relevant Past Insights" in prompt
                assert "Testing in production reveals real issues" in prompt

    def test_draft_with_lineage_no_knowledge_when_store_is_none(self):
        with patch("engagement.reply_drafter.anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            mock_response = MagicMock()
            mock_response.content = [MagicMock(text="Reply text")]
            mock_client.messages.create.return_value = mock_response

            drafter = ReplyDrafter(
                api_key="sk-test",
                model="test-model",
                knowledge_store=None
            )

            result = drafter.draft_with_lineage("our post", "their reply", "them", "me")

            # Should have no knowledge IDs
            assert result.knowledge_ids == []

            # Prompt should not include knowledge section
            prompt = mock_client.messages.create.call_args[1]["messages"][0]["content"]
            assert "## Your Relevant Past Insights" not in prompt

    def test_draft_proactive_includes_conversation_context(self):
        with patch("engagement.reply_drafter.anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            mock_response = MagicMock()
            mock_response.content = [MagicMock(text="Reply text")]
            mock_client.messages.create.return_value = mock_response

            drafter = ReplyDrafter(api_key="sk-test", model="test-model")
            drafter.draft_proactive(
                "their tweet",
                "them",
                "me",
                conversation_context={
                    "parent_post_text": "parent context",
                    "quoted_text": "quoted context",
                },
            )

            prompt = mock_client.messages.create.call_args[1]["messages"][0]["content"]
            assert "## Available Conversation Context" in prompt
            assert "Parent post text: parent context" in prompt
            assert "Quoted post text: quoted context" in prompt


# --- Database integration ---


class TestDatabaseKnowledgeLinks:
    @pytest.fixture
    def in_memory_db(self):
        """Create an in-memory database with schema."""
        db = Database(db_path=":memory:")
        db.connect()
        schema = """
        CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inbound_tweet_id TEXT UNIQUE NOT NULL,
            draft_text TEXT
        );
        CREATE TABLE knowledge (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            content TEXT NOT NULL,
            approved INTEGER DEFAULT 0,
            UNIQUE(source_type, source_id)
        );
        CREATE TABLE reply_knowledge_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reply_queue_id INTEGER,
            knowledge_id INTEGER,
            relevance_score REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
        db.conn.executescript(schema)
        db.conn.commit()
        yield db
        db.close()

    def test_insert_reply_knowledge_links(self, in_memory_db):
        # Insert a reply and knowledge items
        cursor = in_memory_db.conn.execute(
            "INSERT INTO reply_queue (inbound_tweet_id, draft_text) VALUES (?, ?)",
            ("tweet_123", "Draft reply")
        )
        reply_id = cursor.lastrowid

        cursor = in_memory_db.conn.execute(
            "INSERT INTO knowledge (source_type, source_id, content) VALUES (?, ?, ?)",
            ("own_post", "k1", "Knowledge 1")
        )
        k1_id = cursor.lastrowid

        cursor = in_memory_db.conn.execute(
            "INSERT INTO knowledge (source_type, source_id, content) VALUES (?, ?, ?)",
            ("own_post", "k2", "Knowledge 2")
        )
        k2_id = cursor.lastrowid

        # Insert links
        knowledge_ids = [(k1_id, 0.85), (k2_id, 0.72)]
        in_memory_db.insert_reply_knowledge_links(reply_id, knowledge_ids)

        # Verify links were inserted
        cursor = in_memory_db.conn.execute(
            "SELECT knowledge_id, relevance_score FROM reply_knowledge_links WHERE reply_queue_id = ?",
            (reply_id,)
        )
        links = cursor.fetchall()

        assert len(links) == 2
        assert (links[0][0], links[0][1]) == (k1_id, 0.85)
        assert (links[1][0], links[1][1]) == (k2_id, 0.72)

    def test_insert_reply_knowledge_links_empty_list(self, in_memory_db):
        # Insert a reply
        cursor = in_memory_db.conn.execute(
            "INSERT INTO reply_queue (inbound_tweet_id, draft_text) VALUES (?, ?)",
            ("tweet_456", "Draft")
        )
        reply_id = cursor.lastrowid

        # Insert empty list (should be no-op)
        in_memory_db.insert_reply_knowledge_links(reply_id, [])

        # Verify no links were inserted
        cursor = in_memory_db.conn.execute(
            "SELECT COUNT(*) FROM reply_knowledge_links WHERE reply_queue_id = ?",
            (reply_id,)
        )
        count = cursor.fetchone()[0]
        assert count == 0

    def test_insert_reply_knowledge_links_commits(self, in_memory_db):
        # Insert reply and knowledge
        cursor = in_memory_db.conn.execute(
            "INSERT INTO reply_queue (inbound_tweet_id, draft_text) VALUES (?, ?)",
            ("tweet_789", "Draft")
        )
        reply_id = cursor.lastrowid

        cursor = in_memory_db.conn.execute(
            "INSERT INTO knowledge (source_type, source_id, content) VALUES (?, ?, ?)",
            ("own_post", "k3", "Knowledge 3")
        )
        k_id = cursor.lastrowid

        # Insert link
        in_memory_db.insert_reply_knowledge_links(reply_id, [(k_id, 0.9)])

        # Query to verify the link was inserted and committed
        cursor = in_memory_db.conn.execute(
            "SELECT COUNT(*) FROM reply_knowledge_links WHERE reply_queue_id = ?",
            (reply_id,)
        )
        count = cursor.fetchone()[0]
        assert count == 1
