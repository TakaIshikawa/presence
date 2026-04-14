"""Tests for content repurposing functionality."""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone, timedelta

from synthesis.repurposer import ContentRepurposer, RepurposeCandidate, RepurposeResult
from storage.db import Database


@pytest.fixture
def db():
    """Create an in-memory test database."""
    db = Database(":memory:")
    db.connect()
    db.init_schema("./schema.sql")
    yield db
    db.close()


@pytest.fixture
def mock_anthropic():
    """Mock Anthropic client."""
    with patch("synthesis.repurposer.anthropic.Anthropic") as mock:
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="TWEET 1:\nExpanded content\n\nTWEET 2:\nMore details")]
        mock.return_value.messages.create.return_value = mock_response
        yield mock


class TestDatabaseMethods:
    def test_get_repurpose_candidates_empty_db(self, db):
        """Test get_repurpose_candidates with no data."""
        candidates = db.get_repurpose_candidates()
        assert candidates == []

    def test_get_repurpose_candidates_with_high_engagement(self, db):
        """Test finding candidates with high engagement scores."""
        # Insert a published post
        content_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=["abc123"],
            source_messages=["msg1"],
            content="Test post that resonated",
            eval_score=8.0,
            eval_feedback="Good",
        )

        # Mark as published
        now = datetime.now(timezone.utc).isoformat()
        db.mark_published(content_id, "https://x.com/test/123", tweet_id="123")

        # Insert engagement data
        db.insert_engagement(
            content_id=content_id,
            tweet_id="123",
            like_count=20,
            retweet_count=5,
            reply_count=3,
            quote_count=1,
            engagement_score=15.0,
        )

        # Find candidates
        candidates = db.get_repurpose_candidates(min_engagement=10.0)

        assert len(candidates) == 1
        assert candidates[0]["id"] == content_id
        assert candidates[0]["content"] == "Test post that resonated"
        assert candidates[0]["engagement_score"] == 15.0

    def test_get_repurpose_candidates_with_resonated_quality(self, db):
        """Test finding candidates marked as resonated via auto_quality."""
        # Insert post with low engagement but marked as resonated
        content_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=["abc123"],
            source_messages=["msg1"],
            content="Auto-classified as resonated",
            eval_score=7.5,
            eval_feedback="Good",
        )

        db.mark_published(content_id, "https://x.com/test/456", tweet_id="456")

        # Set auto_quality to resonated
        db.conn.execute(
            "UPDATE generated_content SET auto_quality = 'resonated' WHERE id = ?",
            (content_id,)
        )
        db.conn.commit()

        # Insert low engagement data
        db.insert_engagement(
            content_id=content_id,
            tweet_id="456",
            like_count=5,
            retweet_count=1,
            reply_count=0,
            quote_count=0,
            engagement_score=6.0,
        )

        # Should still find it due to auto_quality
        candidates = db.get_repurpose_candidates(min_engagement=10.0)
        assert len(candidates) == 1
        assert candidates[0]["id"] == content_id

    def test_get_repurpose_candidates_excludes_already_repurposed(self, db):
        """Test that already repurposed content is excluded."""
        # Insert original post
        original_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=["abc123"],
            source_messages=["msg1"],
            content="Original post",
            eval_score=8.0,
            eval_feedback="Good",
        )
        db.mark_published(original_id, "https://x.com/test/789", tweet_id="789")
        db.insert_engagement(original_id, "789", 20, 5, 3, 1, 15.0)

        # Repurpose it
        repurposed_id = db.insert_repurposed_content(
            content_type="x_thread",
            source_content_id=original_id,
            content="Expanded thread",
            eval_score=7.5,
            eval_feedback="Good expansion",
        )

        # Should not appear in candidates
        candidates = db.get_repurpose_candidates(min_engagement=10.0)
        assert len(candidates) == 0

    def test_get_repurpose_candidates_respects_age_limit(self, db):
        """Test that old posts are excluded."""
        # Insert old post (beyond max_age_days)
        old_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=["old123"],
            source_messages=["msg1"],
            content="Old post",
            eval_score=8.0,
            eval_feedback="Good",
        )

        # Set published_at to 20 days ago
        old_date = (datetime.now(timezone.utc) - timedelta(days=20)).isoformat()
        db.conn.execute(
            "UPDATE generated_content SET published = 1, published_at = ?, tweet_id = '999' WHERE id = ?",
            (old_date, old_id)
        )
        db.conn.commit()

        db.insert_engagement(old_id, "999", 20, 5, 3, 1, 15.0)

        # Should not find it with max_age_days=14
        candidates = db.get_repurpose_candidates(min_engagement=10.0, max_age_days=14)
        assert len(candidates) == 0

    def test_insert_repurposed_content(self, db):
        """Test inserting repurposed content."""
        # Insert original
        original_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=["abc"],
            source_messages=["msg"],
            content="Original",
            eval_score=8.0,
            eval_feedback="Good",
        )

        # Insert repurposed content
        repurposed_id = db.insert_repurposed_content(
            content_type="x_thread",
            source_content_id=original_id,
            content="TWEET 1:\nExpanded\n\nTWEET 2:\nMore details",
            eval_score=7.5,
            eval_feedback="Good thread",
        )

        # Verify it was inserted correctly
        cursor = db.conn.execute(
            "SELECT * FROM generated_content WHERE id = ?", (repurposed_id,)
        )
        row = cursor.fetchone()

        assert row is not None
        assert row["content_type"] == "x_thread"
        assert row["repurposed_from"] == original_id
        assert row["eval_score"] == 7.5
        assert "TWEET 1:" in row["content"]


class TestContentRepurposer:
    def test_find_candidates_empty(self, db, mock_anthropic):
        """Test find_candidates with no eligible content."""
        repurposer = ContentRepurposer(api_key="test-key", model="test-model", db=db)
        candidates = repurposer.find_candidates()
        assert candidates == []

    def test_find_candidates_maps_types_correctly(self, db, mock_anthropic):
        """Test that candidates are mapped to correct target types."""
        # Insert x_post -> should map to x_thread
        post_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=["a"],
            source_messages=["m"],
            content="Post",
            eval_score=8.0,
            eval_feedback="Good",
        )
        db.mark_published(post_id, "https://x.com/test/1", tweet_id="1")
        db.insert_engagement(post_id, "1", 20, 5, 3, 1, 15.0)

        # Insert x_thread -> should map to blog_seed
        thread_id = db.insert_generated_content(
            content_type="x_thread",
            source_commits=["b"],
            source_messages=["n"],
            content="Thread",
            eval_score=8.0,
            eval_feedback="Good",
        )
        db.mark_published(thread_id, "https://x.com/test/2", tweet_id="2")
        db.insert_engagement(thread_id, "2", 30, 8, 5, 2, 25.0)

        repurposer = ContentRepurposer(api_key="test-key", model="test-model", db=db)
        candidates = repurposer.find_candidates()

        assert len(candidates) == 2

        # Should be ordered by engagement score descending (thread first)
        assert candidates[0].content_id == thread_id
        assert candidates[0].original_type == "x_thread"
        assert candidates[0].target_type == "blog_seed"

        assert candidates[1].content_id == post_id
        assert candidates[1].original_type == "x_post"
        assert candidates[1].target_type == "x_thread"

    def test_find_candidates_excludes_already_repurposed_set(self, db, mock_anthropic):
        """Test that already_repurposed set is respected."""
        post_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=["a"],
            source_messages=["m"],
            content="Post",
            eval_score=8.0,
            eval_feedback="Good",
        )
        db.mark_published(post_id, "https://x.com/test/1", tweet_id="1")
        db.insert_engagement(post_id, "1", 20, 5, 3, 1, 15.0)

        repurposer = ContentRepurposer(api_key="test-key", model="test-model", db=db)

        # Without exclusion
        candidates = repurposer.find_candidates()
        assert len(candidates) == 1

        # With exclusion
        candidates = repurposer.find_candidates(already_repurposed={post_id})
        assert len(candidates) == 0

    def test_expand_post_to_thread(self, db, mock_anthropic):
        """Test expanding a post into a thread."""
        repurposer = ContentRepurposer(api_key="test-key", model="test-model", db=db)

        candidate = RepurposeCandidate(
            content_id=1,
            original_content="Original post about debugging",
            original_type="x_post",
            engagement_score=15.0,
            target_type="x_thread",
        )

        result = repurposer.expand_post_to_thread(candidate)

        assert result.source_id == 1
        assert result.target_type == "x_thread"
        assert "TWEET 1:" in result.content
        assert len(result.content) > 0
        assert "Original post about debugging" in result.generation_prompt

        # Verify Anthropic was called
        mock_anthropic.return_value.messages.create.assert_called_once()
        call_args = mock_anthropic.return_value.messages.create.call_args
        assert call_args[1]["model"] == "test-model"
        assert call_args[1]["max_tokens"] == 2000

    def test_expand_to_blog_seed(self, db, mock_anthropic):
        """Test expanding to blog seed."""
        # Mock different response for blog seed
        mock_anthropic.return_value.messages.create.return_value.content[0].text = (
            "TITLE: Test Title\n\nOUTLINE:\n1. Intro\n2. Details\n\nDRAFT OPENING:\nThis is the opening."
        )

        repurposer = ContentRepurposer(api_key="test-key", model="test-model", db=db)

        candidate = RepurposeCandidate(
            content_id=2,
            original_content="Thread about performance",
            original_type="x_thread",
            engagement_score=25.0,
            target_type="blog_seed",
        )

        result = repurposer.expand_to_blog_seed(candidate)

        assert result.source_id == 2
        assert result.target_type == "blog_seed"
        assert "TITLE:" in result.content
        assert "OUTLINE:" in result.content
        assert len(result.content) > 0
        assert "Thread about performance" in result.generation_prompt

        # Verify Anthropic was called with correct max_tokens
        call_args = mock_anthropic.return_value.messages.create.call_args
        assert call_args[1]["max_tokens"] == 2500


class TestEdgeCases:
    def test_get_repurpose_candidates_multiple_engagement_records(self, db):
        """Test that only the latest engagement record is used."""
        content_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=["abc"],
            source_messages=["msg"],
            content="Post",
            eval_score=8.0,
            eval_feedback="Good",
        )
        db.mark_published(content_id, "https://x.com/test/1", tweet_id="1")

        # Insert older engagement record
        db.insert_engagement(content_id, "1", 5, 1, 0, 0, 3.0)

        # Insert newer engagement record
        db.insert_engagement(content_id, "1", 20, 5, 3, 1, 15.0)

        # Should use the latest (highest) engagement score
        candidates = db.get_repurpose_candidates(min_engagement=10.0)
        assert len(candidates) == 1
        assert candidates[0]["engagement_score"] == 15.0

    def test_insert_repurposed_content_sets_empty_source_arrays(self, db):
        """Test that repurposed content has empty source_commits and source_messages."""
        original_id = 1
        repurposed_id = db.insert_repurposed_content(
            content_type="x_thread",
            source_content_id=original_id,
            content="Repurposed",
            eval_score=7.0,
            eval_feedback="OK",
        )

        cursor = db.conn.execute(
            "SELECT source_commits, source_messages FROM generated_content WHERE id = ?",
            (repurposed_id,)
        )
        row = cursor.fetchone()

        assert row["source_commits"] == "[]"
        assert row["source_messages"] == "[]"
