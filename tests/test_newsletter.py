"""Tests for newsletter assembly and delivery."""

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests

from output.newsletter import (
    NewsletterAssembler,
    NewsletterContent,
    NewsletterResult,
    ButtondownClient,
)


# --- Helpers ---


def _insert_published_content(db, content_type, content, days_ago=1, url=None):
    """Insert a published post with a recent published_at timestamp."""
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=["sha-1"],
        source_messages=["msg-1"],
        content=content,
        eval_score=8.0,
        eval_feedback="Good",
    )
    published_at = (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()
    db.conn.execute(
        "UPDATE generated_content SET published = 1, published_at = ?, published_url = ? WHERE id = ?",
        (published_at, url or f"https://example.com/{content_id}", content_id),
    )
    db.conn.commit()
    return content_id


# --- NewsletterAssembler ---


class TestNewsletterAssembler:
    def test_assembles_with_all_content_types(self, db):
        """Newsletter includes blog, threads, and posts."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)

        _insert_published_content(
            db, "blog_post",
            "TITLE: Test Blog Post\n\nThis is the blog content.\n\nMore content here.",
            days_ago=2,
            url="https://takaishikawa.com/blog/test-blog-post.html",
        )
        _insert_published_content(
            db, "x_thread",
            "TWEET 1:\nFirst tweet content\n\nTWEET 2:\nSecond tweet content",
            days_ago=3,
        )
        _insert_published_content(db, "x_post", "A great post about AI.", days_ago=1)

        assembler = NewsletterAssembler(db)
        content = assembler.assemble(week_start, now)

        assert content.subject != ""
        assert "Test Blog Post" in content.body_markdown
        assert "First tweet content" in content.body_markdown
        assert "A great post about AI" in content.body_markdown
        assert len(content.source_content_ids) == 3

    def test_assembles_without_blog_post(self, db):
        """Newsletter works without a blog post."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)

        _insert_published_content(db, "x_post", "Post without blog.", days_ago=1)

        assembler = NewsletterAssembler(db)
        content = assembler.assemble(week_start, now)

        assert content.subject != ""
        assert "Post without blog" in content.body_markdown
        assert "This Week's Post" not in content.body_markdown

    def test_empty_week_returns_empty(self, db):
        """No published content produces empty newsletter."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)

        assembler = NewsletterAssembler(db)
        content = assembler.assemble(week_start, now)

        assert content.subject == ""
        assert content.body_markdown == ""
        assert content.source_content_ids == []

    def test_excludes_content_outside_range(self, db):
        """Content from before the week range is excluded."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)

        # Content from 10 days ago — outside the 7-day window
        _insert_published_content(db, "x_post", "Old post.", days_ago=10)

        assembler = NewsletterAssembler(db)
        content = assembler.assemble(week_start, now)

        assert content.body_markdown == ""

    def test_limits_posts_count(self, db):
        """At most 3 posts are included."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)

        for i in range(5):
            _insert_published_content(db, "x_post", f"Post number {i}.", days_ago=i + 1)

        assembler = NewsletterAssembler(db)
        content = assembler.assemble(week_start, now)

        # Should have at most 3 post content IDs (blogs and threads contribute separately)
        post_mentions = content.body_markdown.count("Post number")
        assert post_mentions <= 3


class TestNewsletterAssemblerHelpers:
    def test_extract_blog_title(self):
        assert NewsletterAssembler._extract_blog_title(
            "TITLE: My Blog Post\n\nContent here"
        ) == "My Blog Post"

    def test_extract_blog_title_missing(self):
        assert NewsletterAssembler._extract_blog_title("No title here") == "This Week's Post"

    def test_extract_first_tweet(self):
        content = "TWEET 1:\nFirst tweet text\n\nTWEET 2:\nSecond tweet"
        assert NewsletterAssembler._extract_first_tweet(content) == "First tweet text"

    def test_extract_first_tweet_no_markers(self):
        """Fallback to first non-empty line when no TWEET markers."""
        content = "This is a regular post.\nSecond line."
        assert NewsletterAssembler._extract_first_tweet(content) == "This is a regular post."

    def test_extract_first_tweet_all_tweet_prefix(self):
        """Fallback to content[:100] when all lines start with TWEET."""
        content = "TWEET: line 1\nTWEET: line 2\nTWEET: line 3"
        result = NewsletterAssembler._extract_first_tweet(content)
        assert result == content[:100]
        assert len(result) <= 100

    def test_extract_blog_excerpt(self):
        content = "TITLE: Title\n\n## Header\n\nFirst paragraph.\n\nSecond paragraph."
        excerpt = NewsletterAssembler._extract_blog_excerpt(content, max_lines=2)
        assert "First paragraph" in excerpt
        assert "TITLE" not in excerpt
        assert "Header" not in excerpt


# --- ButtondownClient ---


class TestButtondownClient:
    @patch("output.newsletter.requests.Session")
    def test_send_success(self, MockSession):
        mock_session = MockSession.return_value
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "id": "issue-123",
            "absolute_url": "https://buttondown.com/issue/123",
        }
        mock_session.post.return_value = mock_response

        client = ButtondownClient("test-api-key")
        client.session = mock_session
        result = client.send("Subject", "Body text")

        assert result.success is True
        assert result.issue_id == "issue-123"
        assert result.url == "https://buttondown.com/issue/123"

    @patch("output.newsletter.requests.Session")
    def test_send_failure(self, MockSession):
        mock_session = MockSession.return_value
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Bad request"
        mock_session.post.return_value = mock_response

        client = ButtondownClient("test-api-key")
        client.session = mock_session
        result = client.send("Subject", "Body")

        assert result.success is False
        assert "400" in result.error

    @patch("output.newsletter.requests.Session")
    def test_send_draft_mode(self, MockSession):
        mock_session = MockSession.return_value
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"id": "draft-1", "absolute_url": ""}
        mock_session.post.return_value = mock_response

        client = ButtondownClient("test-api-key")
        client.session = mock_session
        client.send("Subject", "Body", publish=False)

        call_kwargs = mock_session.post.call_args
        assert call_kwargs[1]["json"]["status"] == "draft"

    @patch("output.newsletter.requests.Session")
    def test_send_request_exception(self, MockSession):
        """Test send() handles RequestException correctly."""
        mock_session = MockSession.return_value
        mock_session.post.side_effect = requests.RequestException("Network error")

        client = ButtondownClient("test-api-key")
        client.session = mock_session
        result = client.send("Subject", "Body")

        assert result.success is False
        assert "Network error" in result.error

    @patch("output.newsletter.requests.Session")
    def test_get_subscriber_count_success(self, MockSession):
        """Test get_subscriber_count() with successful response."""
        mock_session = MockSession.return_value
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"count": 150}
        mock_session.get.return_value = mock_response

        client = ButtondownClient("test-api-key")
        client.session = mock_session
        count = client.get_subscriber_count()

        assert count == 150
        mock_session.get.assert_called_once_with(
            f"{ButtondownClient.BASE_URL}/subscribers",
            params={"type": "regular"},
            timeout=30,
        )

    @patch("output.newsletter.requests.Session")
    def test_get_subscriber_count_http_error(self, MockSession):
        """Test get_subscriber_count() returns 0 on HTTP error."""
        mock_session = MockSession.return_value
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_session.get.return_value = mock_response

        client = ButtondownClient("test-api-key")
        client.session = mock_session
        count = client.get_subscriber_count()

        assert count == 0

    @patch("output.newsletter.requests.Session")
    def test_get_subscriber_count_request_exception(self, MockSession):
        """Test get_subscriber_count() returns 0 on RequestException."""
        mock_session = MockSession.return_value
        mock_session.get.side_effect = requests.RequestException("Timeout")

        client = ButtondownClient("test-api-key")
        client.session = mock_session
        count = client.get_subscriber_count()

        assert count == 0


# --- DB Methods ---


class TestNewsletterDB:
    def test_insert_and_get_last_send(self, db):
        db.insert_newsletter_send(
            issue_id="issue-1",
            subject="Test Subject",
            content_ids=[1, 2, 3],
            subscriber_count=42,
        )

        last = db.get_last_newsletter_send()
        assert last is not None
        assert (datetime.now(timezone.utc) - last).total_seconds() < 60

    def test_get_last_send_empty(self, db):
        assert db.get_last_newsletter_send() is None

    def test_get_published_content_in_range(self, db):
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)

        # Content within range
        _insert_published_content(db, "x_post", "In range.", days_ago=2)
        # Content outside range
        _insert_published_content(db, "x_post", "Out of range.", days_ago=10)

        results = db.get_published_content_in_range("x_post", week_start, now)
        assert len(results) == 1
        assert results[0]["content"] == "In range."
