"""Tests for content calendar topic extraction and planning."""

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from storage.db import Database
from evaluation.topic_extractor import TopicExtractor, TOPIC_TAXONOMY


@pytest.fixture
def schema_path():
    """Path to the schema file."""
    import os
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, "schema.sql")


@pytest.fixture
def db(schema_path):
    """In-memory database for testing."""
    db = Database(":memory:")
    db.connect()
    db.init_schema(schema_path)
    yield db
    db.close()


@pytest.fixture
def sample_content(db):
    """Create sample published content for testing."""
    now = datetime.now(timezone.utc)

    # Create some sample content
    content_ids = []
    for i in range(5):
        cursor = db.conn.execute(
            """INSERT INTO generated_content
               (content_type, source_commits, source_messages, content,
                eval_score, eval_feedback, published, published_at)
               VALUES (?, '[]', '[]', ?, 8.0, 'Good', 1, ?)""",
            (
                "x_post",
                f"Sample post about testing patterns #{i}",
                (now - timedelta(days=i)).isoformat()
            )
        )
        content_ids.append(cursor.lastrowid)

    db.conn.commit()
    return content_ids


# --- TopicExtractor Tests ---

class TestTopicExtractor:
    """Test TopicExtractor class."""

    def test_extract_topics_with_mocked_api(self):
        """Test topic extraction with mocked Anthropic API."""
        extractor = TopicExtractor(api_key="test-key")

        # Mock the API response
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text='[{"topic": "testing", "subtopic": "integration tests", "confidence": 0.9}]')]

        with patch.object(extractor.client.messages, 'create', return_value=mock_response):
            topics = extractor.extract_topics("A post about integration testing patterns")

            assert len(topics) == 1
            assert topics[0][0] == "testing"
            assert topics[0][1] == "integration tests"
            assert topics[0][2] == 0.9

    def test_extract_topics_with_markdown_json(self):
        """Test parsing JSON wrapped in markdown code blocks."""
        extractor = TopicExtractor(api_key="test-key")

        mock_response = MagicMock()
        mock_response.content = [MagicMock(text='```json\n[{"topic": "architecture", "subtopic": "microservices", "confidence": 0.8}]\n```')]

        with patch.object(extractor.client.messages, 'create', return_value=mock_response):
            topics = extractor.extract_topics("A post about microservices architecture")

            assert len(topics) == 1
            assert topics[0][0] == "architecture"

    def test_extract_topics_invalid_topic_fallback(self):
        """Test that invalid topics fall back to 'other'."""
        extractor = TopicExtractor(api_key="test-key")

        mock_response = MagicMock()
        mock_response.content = [MagicMock(text='[{"topic": "invalid-topic", "subtopic": "test", "confidence": 0.9}]')]

        with patch.object(extractor.client.messages, 'create', return_value=mock_response):
            topics = extractor.extract_topics("Some content")

            assert len(topics) == 1
            assert topics[0][0] == "other"  # Invalid topic should fall back

    def test_extract_topics_error_handling(self):
        """Test error handling when API call fails."""
        extractor = TopicExtractor(api_key="test-key")

        with patch.object(extractor.client.messages, 'create', side_effect=Exception("API error")):
            topics = extractor.extract_topics("Some content")

            assert len(topics) == 1
            assert topics[0][0] == "other"
            assert topics[0][2] == 0.5  # Low confidence on error

    def test_batch_extract(self):
        """Test batch extraction processes all items."""
        extractor = TopicExtractor(api_key="test-key")

        mock_response = MagicMock()
        mock_response.content = [MagicMock(text='[{"topic": "testing", "subtopic": "unit tests", "confidence": 0.8}]')]

        contents = ["Post 1", "Post 2", "Post 3"]

        with patch.object(extractor.client.messages, 'create', return_value=mock_response):
            results = extractor.batch_extract(contents)

            assert len(results) == 3
            for topics in results:
                assert len(topics) == 1
                assert topics[0][0] == "testing"

    def test_confidence_clamping(self):
        """Test that confidence scores are clamped to [0, 1]."""
        extractor = TopicExtractor(api_key="test-key")

        mock_response = MagicMock()
        mock_response.content = [MagicMock(text='[{"topic": "testing", "subtopic": "test", "confidence": 1.5}]')]

        with patch.object(extractor.client.messages, 'create', return_value=mock_response):
            topics = extractor.extract_topics("Some content")

            assert topics[0][2] <= 1.0  # Confidence should be clamped


# --- Database Method Tests ---

class TestDatabaseTopicMethods:
    """Test database methods for topic management."""

    def test_insert_content_topics(self, db):
        """Test inserting topic entries."""
        # Create a content item
        cursor = db.conn.execute(
            """INSERT INTO generated_content
               (content_type, source_commits, source_messages, content,
                eval_score, eval_feedback)
               VALUES ('x_post', '[]', '[]', 'Test', 8.0, 'Good')"""
        )
        content_id = cursor.lastrowid
        db.conn.commit()

        # Insert topics
        topics = [
            ("testing", "unit tests", 0.9),
            ("architecture", "design patterns", 0.7)
        ]
        topic_ids = db.insert_content_topics(content_id, topics)

        assert len(topic_ids) == 2

        # Verify topics were stored
        cursor = db.conn.execute(
            "SELECT topic, subtopic, confidence FROM content_topics WHERE content_id = ?",
            (content_id,)
        )
        rows = cursor.fetchall()

        assert len(rows) == 2
        assert rows[0][0] == "testing"
        assert rows[0][1] == "unit tests"
        assert rows[0][2] == 0.9

    def test_get_topic_frequency(self, db, sample_content):
        """Test topic frequency calculation."""
        # Add topics to sample content
        for i, content_id in enumerate(sample_content):
            topic = "testing" if i % 2 == 0 else "architecture"
            db.insert_content_topics(content_id, [(topic, "", 1.0)])

        frequencies = db.get_topic_frequency(days=30)

        assert len(frequencies) == 2
        # Should be ordered by count descending
        assert frequencies[0]["count"] == 3  # testing appears 3 times
        assert frequencies[1]["count"] == 2  # architecture appears 2 times

    def test_get_topic_gaps(self, db, sample_content):
        """Test gap detection for topics not covered recently."""
        now = datetime.now(timezone.utc)

        # Add only "testing" topic to recent content
        db.insert_content_topics(sample_content[0], [("testing", "", 1.0)])

        # Add "architecture" to old content (15 days ago)
        cursor = db.conn.execute(
            """INSERT INTO generated_content
               (content_type, source_commits, source_messages, content,
                eval_score, eval_feedback, published, published_at)
               VALUES ('x_post', '[]', '[]', 'Old post', 8.0, 'Good', 1, ?)""",
            ((now - timedelta(days=15)).isoformat(),)
        )
        old_content_id = cursor.lastrowid
        db.conn.commit()
        db.insert_content_topics(old_content_id, [("architecture", "", 1.0)])

        # Check gaps with 7-day threshold
        gaps = db.get_topic_gaps(days=30, min_gap_days=7)

        # Architecture should be in gaps (last seen 15 days ago)
        assert "architecture" in gaps
        # Testing should NOT be in gaps (seen recently)
        assert "testing" not in gaps
        # Other topics should be in gaps (never seen)
        assert "debugging" in gaps

    def test_insert_planned_topic(self, db):
        """Test inserting a planned topic."""
        topic_id = db.insert_planned_topic(
            topic="testing",
            angle="property-based testing patterns",
            target_date="2026-05-01"
        )

        assert topic_id > 0

        # Verify it was stored
        cursor = db.conn.execute(
            "SELECT topic, angle, target_date, status FROM planned_topics WHERE id = ?",
            (topic_id,)
        )
        row = cursor.fetchone()

        assert row[0] == "testing"
        assert row[1] == "property-based testing patterns"
        assert row[2] == "2026-05-01"
        assert row[3] == "planned"

    def test_get_planned_topics(self, db):
        """Test retrieving planned topics by status."""
        # Insert some planned topics
        db.insert_planned_topic(topic="testing", target_date="2026-05-01")
        db.insert_planned_topic(topic="architecture", target_date="2026-05-02")

        # Mark one as generated
        db.conn.execute(
            "UPDATE planned_topics SET status = 'generated' WHERE topic = 'testing'"
        )
        db.conn.commit()

        # Get planned topics
        planned = db.get_planned_topics(status="planned")
        assert len(planned) == 1
        assert planned[0]["topic"] == "architecture"

        # Get generated topics
        generated = db.get_planned_topics(status="generated")
        assert len(generated) == 1
        assert generated[0]["topic"] == "testing"

    def test_mark_planned_topic_generated(self, db):
        """Test linking a planned topic to generated content."""
        # Create a planned topic
        planned_id = db.insert_planned_topic(topic="testing")

        # Create content
        cursor = db.conn.execute(
            """INSERT INTO generated_content
               (content_type, source_commits, source_messages, content,
                eval_score, eval_feedback)
               VALUES ('x_post', '[]', '[]', 'Test', 8.0, 'Good')"""
        )
        content_id = cursor.lastrowid
        db.conn.commit()

        # Mark as generated
        db.mark_planned_topic_generated(planned_id, content_id)

        # Verify status and link
        cursor = db.conn.execute(
            "SELECT status, content_id FROM planned_topics WHERE id = ?",
            (planned_id,)
        )
        row = cursor.fetchone()

        assert row[0] == "generated"
        assert row[1] == content_id

    def test_get_content_without_topics(self, db, sample_content):
        """Test finding content without topic entries."""
        # Add topics to some content
        db.insert_content_topics(sample_content[0], [("testing", "", 1.0)])
        db.insert_content_topics(sample_content[1], [("architecture", "", 1.0)])

        # Get content without topics
        without_topics = db.get_content_without_topics()

        # Should return 3 items (out of 5 sample content)
        assert len(without_topics) == 3

        # Should not include the two with topics
        returned_ids = {item["id"] for item in without_topics}
        assert sample_content[0] not in returned_ids
        assert sample_content[1] not in returned_ids


# --- Integration Tests ---

class TestContentCalendarIntegration:
    """Integration tests for the complete workflow."""

    def test_topic_extraction_and_frequency_workflow(self, db):
        """Test complete workflow: create content, extract topics, check frequency."""
        now = datetime.now(timezone.utc)

        # Create content
        content_ids = []
        for i in range(3):
            cursor = db.conn.execute(
                """INSERT INTO generated_content
                   (content_type, source_commits, source_messages, content,
                    eval_score, eval_feedback, published, published_at)
                   VALUES ('x_post', '[]', '[]', ?, 8.0, 'Good', 1, ?)""",
                (f"Post about testing #{i}", now.isoformat())
            )
            content_ids.append(cursor.lastrowid)

        db.conn.commit()

        # Extract and insert topics
        for content_id in content_ids:
            db.insert_content_topics(content_id, [("testing", "integration tests", 0.9)])

        # Check frequency
        frequencies = db.get_topic_frequency(days=30)

        assert len(frequencies) == 1
        assert frequencies[0]["topic"] == "testing"
        assert frequencies[0]["count"] == 3

    def test_planned_topic_lifecycle(self, db):
        """Test complete lifecycle: plan topic, generate content, link."""
        now = datetime.now(timezone.utc)

        # Plan a topic
        planned_id = db.insert_planned_topic(
            topic="performance",
            angle="query optimization",
            target_date="2026-05-01"
        )

        # Generate content with published_at timestamp
        cursor = db.conn.execute(
            """INSERT INTO generated_content
               (content_type, source_commits, source_messages, content,
                eval_score, eval_feedback, published, published_at)
               VALUES ('x_post', '[]', '[]', 'Post about query optimization', 8.0, 'Good', 1, ?)""",
            (now.isoformat(),)
        )
        content_id = cursor.lastrowid
        db.conn.commit()

        # Link and mark as generated
        db.mark_planned_topic_generated(planned_id, content_id)

        # Extract topic from content
        db.insert_content_topics(content_id, [("performance", "query optimization", 0.9)])

        # Verify workflow
        planned = db.get_planned_topics(status="generated")
        assert len(planned) == 1
        assert planned[0]["content_id"] == content_id

        # Verify topic appears in frequency
        frequencies = db.get_topic_frequency(days=30)
        assert any(f["topic"] == "performance" for f in frequencies)
