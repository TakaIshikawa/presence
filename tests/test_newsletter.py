"""Tests for newsletter assembly and delivery."""

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse
from unittest.mock import MagicMock, patch

import pytest
import requests

from output.newsletter import (
    NewsletterAssembler,
    NewsletterContent,
    NewsletterLinkClick,
    NewsletterMetrics,
    NewsletterSubscriberMetrics,
    NewsletterResult,
    ButtondownClient,
    normalize_newsletter_link_url,
)


# --- Helpers ---


def _insert_published_content(
    db,
    content_type,
    content,
    days_ago=1,
    url=None,
    published_at=None,
):
    """Insert a published post with a recent published_at timestamp."""
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=["sha-1"],
        source_messages=["msg-1"],
        content=content,
        eval_score=8.0,
        eval_feedback="Good",
    )
    published_at = (
        published_at or (datetime.now(timezone.utc) - timedelta(days=days_ago))
    )
    db.conn.execute(
        "UPDATE generated_content SET published = 1, published_at = ?, published_url = ? WHERE id = ?",
        (published_at.isoformat(), url or f"https://example.com/{content_id}", content_id),
    )
    db.conn.commit()
    return content_id


def _set_content_format(db, content_id, content_format):
    db.conn.execute(
        "UPDATE generated_content SET content_format = ? WHERE id = ?",
        (content_format, content_id),
    )
    db.conn.commit()


def _first_markdown_url(markdown: str) -> str:
    match = re.search(r"\]\(([^)]+)\)", markdown)
    assert match is not None
    return match.group(1)


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

    def test_limits_threads_to_two(self, db):
        """At most 2 threads are included."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)

        for i in range(4):
            _insert_published_content(
                db, "x_thread",
                f"TWEET 1:\nThread number {i} first tweet\n\nTWEET 2:\nSecond tweet",
                days_ago=i + 1,
            )

        assembler = NewsletterAssembler(db)
        content = assembler.assemble(week_start, now)

        # Count "Thread number" occurrences (not just "Thread" which includes "## Threads")
        thread_mentions = content.body_markdown.count("Thread number")
        assert thread_mentions == 2

    def test_subject_format(self, db):
        """Subject line follows the expected format."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)

        _insert_published_content(db, "x_post", "Test post.", days_ago=1)

        assembler = NewsletterAssembler(db)
        content = assembler.assemble(week_start, now)

        expected_date = week_start.strftime("%b %d")
        assert content.subject == f"Building with AI — Week of {expected_date}"

    def test_generates_ranked_subject_candidates(self, db):
        """Assembled newsletters include scored subject alternatives."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)

        _insert_published_content(
            db,
            "blog_post",
            "TITLE: Shipping Better AI Tools\n\nSpecific release notes.",
            published_at=now - timedelta(days=1),
        )

        content = NewsletterAssembler(db).assemble(week_start, now)

        assert len(content.subject_candidates) >= 3
        assert content.subject_candidates == sorted(
            content.subject_candidates,
            key=lambda item: (-item.score, item.subject.lower()),
        )
        assert any(
            candidate.subject == "Shipping Better AI Tools"
            for candidate in content.subject_candidates
        )
        assert all(candidate.score > 0 for candidate in content.subject_candidates)

    def test_history_biases_candidate_order_without_overriding_heuristics(self, db):
        """Historical winners can move a close subject ahead of the fallback."""
        now = datetime(2026, 4, 23, tzinfo=timezone.utc)
        week_start = now - timedelta(days=7)
        assembler = NewsletterAssembler(db)
        subject_context = {
            "blog_titles": [],
            "thread_hooks": ["Launch notes"],
            "post_hooks": [],
            "content_types": [],
        }
        fallback_subject = "Building with AI — Week of Apr 16"

        history_rows = [
            {
                "subject": "Launch notes that landed",
                "open_rate": 0.58,
                "click_rate": 0.13,
                "unsubscribes": 0,
                "subscriber_count": 100,
                "sent_at": "2026-03-20T00:00:00+00:00",
            },
            {
                "subject": "Launch notes for builders",
                "open_rate": 0.53,
                "click_rate": 0.1,
                "unsubscribes": 0,
                "subscriber_count": 100,
                "sent_at": "2026-03-13T00:00:00+00:00",
            },
            {
                "subject": "Launch notes people read",
                "open_rate": 0.49,
                "click_rate": 0.08,
                "unsubscribes": 0,
                "subscriber_count": 100,
                "sent_at": "2026-03-06T00:00:00+00:00",
            },
            {
                "subject": "Plain weekly digest",
                "open_rate": 0.18,
                "click_rate": 0.01,
                "unsubscribes": 0,
                "subscriber_count": 100,
                "sent_at": "2026-02-27T00:00:00+00:00",
            },
        ]

        with patch.object(
            db,
            "get_newsletter_subject_performance",
            return_value=history_rows,
        ):
            ranked_with_history = assembler.generate_subject_candidates(
                week_start,
                now,
                subject_context=subject_context,
                fallback_subject=fallback_subject,
            )

        ranked_without_history = assembler.generate_subject_candidates(
            week_start,
            now,
            subject_context=subject_context,
            fallback_subject=fallback_subject,
            subject_history=[],
        )

        assert ranked_without_history[0].subject == fallback_subject
        assert ranked_with_history[0].subject == "This week: Launch notes"
        assert ranked_with_history[0].score > ranked_without_history[0].score
        assert ranked_with_history[0].metadata["history"]["matched_tokens"]
        assert "history match" in ranked_with_history[0].rationale

    def test_includes_site_url_footer(self, db):
        """Newsletter footer includes site URL."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)

        _insert_published_content(db, "x_post", "Test post.", days_ago=1)

        assembler = NewsletterAssembler(db)
        content = assembler.assemble(week_start, now)

        assert "takaishikawa.com" in content.body_markdown
        assert "*Shipped from [takaishikawa.com](https://takaishikawa.com)*" in content.body_markdown

    def test_blog_post_section_includes_read_link(self, db):
        """Blog post section includes 'Read the full post' link."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)

        _insert_published_content(
            db, "blog_post",
            "TITLE: Test Post\n\nSome content here.",
            days_ago=1,
            url="https://takaishikawa.com/blog/test.html",
        )

        assembler = NewsletterAssembler(db)
        content = assembler.assemble(week_start, now)

        assert "[Read the full post](https://takaishikawa.com/blog/test.html)" in content.body_markdown

    def test_internal_links_get_utm_and_content_id(self, db):
        """Internal content links are rewritten with newsletter attribution."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)
        expected_campaign = f"weekly-{now.strftime('%Y%m%d')}"
        content_id = _insert_published_content(
            db,
            "blog_post",
            "TITLE: Test Post\n\nSome content here.",
            url="https://takaishikawa.com/blog/test.html",
            published_at=now - timedelta(days=1),
        )

        assembler = NewsletterAssembler(
            db,
            utm_source="newsletter",
            utm_medium="email",
            utm_campaign_template="weekly-{week_end_compact}",
        )
        content = assembler.assemble(week_start, now)

        url = _first_markdown_url(content.body_markdown)
        query = parse_qs(urlparse(url).query)
        assert query["utm_source"] == ["newsletter"]
        assert query["utm_medium"] == ["email"]
        assert query["utm_campaign"] == [expected_campaign]
        assert query["content_id"] == [str(content_id)]
        assert normalize_newsletter_link_url(url) == (
            "https://takaishikawa.com/blog/test.html"
        )
        assert content.metadata == {"utm_campaign": expected_campaign}

    def test_internal_links_preserve_existing_query_strings(self, db):
        """UTM parameters are appended without dropping existing query params."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)
        expected_campaign = f"weekly-{week_start.strftime('%Y-%m-%d')}"
        content_id = _insert_published_content(
            db,
            "blog_post",
            "TITLE: Test Post\n\nSome content here.",
            url="https://takaishikawa.com/blog/test.html?ref=site#notes",
            published_at=now - timedelta(days=1),
        )

        assembler = NewsletterAssembler(
            db,
            utm_source="newsletter",
            utm_medium="email",
            utm_campaign_template="weekly-{week_start}",
        )
        content = assembler.assemble(week_start, now)

        url = _first_markdown_url(content.body_markdown)
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        assert query["ref"] == ["site"]
        assert query["utm_campaign"] == [expected_campaign]
        assert query["content_id"] == [str(content_id)]
        assert parsed.fragment == "notes"

    def test_utm_rewrite_skips_external_links(self, db):
        """External published URLs are not modified for attribution."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)
        expected_campaign = f"weekly-{now.strftime('%Y-%m-%d')}"
        _insert_published_content(
            db,
            "x_post",
            "External post.",
            url="https://x.com/taka/status/123?ref=feed",
            published_at=now - timedelta(days=1),
        )

        assembler = NewsletterAssembler(
            db,
            utm_source="newsletter",
            utm_medium="email",
            utm_campaign_template="weekly-{week_end}",
        )
        content = assembler.assemble(week_start, now)

        assert "https://x.com/taka/status/123?ref=feed" in content.body_markdown
        assert "utm_campaign" not in content.body_markdown
        assert content.metadata == {"utm_campaign": expected_campaign}

    def test_utm_rewrite_disabled_by_default(self, db):
        """Default assembler behavior leaves existing links untouched."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)
        _insert_published_content(
            db,
            "blog_post",
            "TITLE: Test Post\n\nSome content here.",
            days_ago=1,
            url="https://takaishikawa.com/blog/test.html?ref=site",
        )

        content = NewsletterAssembler(db).assemble(week_start, now)

        assert "[Test Post](https://takaishikawa.com/blog/test.html?ref=site)" in content.body_markdown
        assert "utm_campaign" not in content.body_markdown
        assert content.metadata == {}

    def test_custom_site_url(self, db):
        """Custom site_url is used in footer."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)

        _insert_published_content(db, "x_post", "Test post.", days_ago=1)

        assembler = NewsletterAssembler(db, site_url="https://example.org")
        content = assembler.assemble(week_start, now)

        assert "example.org" in content.body_markdown
        assert "*Shipped from [takaishikawa.com](https://example.org)*" in content.body_markdown

    def test_assembles_threads_only(self, db):
        """Newsletter with only threads generates correct output."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)

        _insert_published_content(
            db, "x_thread",
            "TWEET 1:\nFirst thread content\n\nTWEET 2:\nSecond tweet",
            days_ago=1,
        )

        assembler = NewsletterAssembler(db)
        content = assembler.assemble(week_start, now)

        assert content.subject != ""
        assert "## Threads" in content.body_markdown
        assert "First thread content" in content.body_markdown
        assert "## This Week's Post" not in content.body_markdown
        assert "## Posts" not in content.body_markdown

    def test_content_ids_collected_across_types(self, db):
        """source_content_ids includes IDs from blog, threads, and posts."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)

        blog_id = _insert_published_content(
            db, "blog_post",
            "TITLE: Blog\n\nContent here.",
            days_ago=1,
        )
        thread_id = _insert_published_content(
            db, "x_thread",
            "TWEET 1:\nThread content\n\nTWEET 2:\nMore",
            days_ago=2,
        )
        post_id = _insert_published_content(db, "x_post", "Post content.", days_ago=3)

        assembler = NewsletterAssembler(db)
        content = assembler.assemble(week_start, now)

        assert len(content.source_content_ids) == 3
        assert blog_id in content.source_content_ids
        assert thread_id in content.source_content_ids
        assert post_id in content.source_content_ids

    def test_suppresses_recently_featured_content_across_types(self, db):
        """Recent newsletter source IDs are filtered before section selection."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)

        repeated_blog_id = _insert_published_content(
            db,
            "blog_post",
            "TITLE: Repeated Blog\n\nOld top blog.",
            days_ago=1,
        )
        fresh_blog_id = _insert_published_content(
            db,
            "blog_post",
            "TITLE: Fresh Blog\n\nNew blog.",
            days_ago=2,
        )
        repeated_thread_id = _insert_published_content(
            db,
            "x_thread",
            "TWEET 1:\nRepeated thread hook\n\nTWEET 2:\nMore",
            days_ago=1,
        )
        fresh_thread_id = _insert_published_content(
            db,
            "x_thread",
            "TWEET 1:\nFresh thread hook\n\nTWEET 2:\nMore",
            days_ago=2,
        )
        repeated_post_id = _insert_published_content(
            db,
            "x_post",
            "Repeated post.",
            days_ago=1,
        )
        fresh_post_id = _insert_published_content(
            db,
            "x_post",
            "Fresh post.",
            days_ago=2,
        )
        send_id = db.insert_newsletter_send(
            issue_id="issue-previous",
            subject="Previous",
            content_ids=[repeated_blog_id, repeated_thread_id, repeated_post_id],
            subscriber_count=100,
        )
        db.conn.execute(
            "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
            ((now - timedelta(days=7)).isoformat(), send_id),
        )
        db.conn.commit()

        content = NewsletterAssembler(db).assemble(week_start, now)

        assert repeated_blog_id not in content.source_content_ids
        assert repeated_thread_id not in content.source_content_ids
        assert repeated_post_id not in content.source_content_ids
        assert fresh_blog_id in content.source_content_ids
        assert fresh_thread_id in content.source_content_ids
        assert fresh_post_id in content.source_content_ids
        assert "Repeated Blog" not in content.body_markdown
        assert "Repeated thread hook" not in content.body_markdown
        assert "Repeated post." not in content.body_markdown
        assert "Fresh Blog" in content.body_markdown
        assert "Fresh thread hook" in content.body_markdown
        assert "Fresh post." in content.body_markdown
        assert content.metadata["suppressed_content_ids"] == [
            repeated_blog_id,
            repeated_thread_id,
            repeated_post_id,
        ]
        assert content.metadata["repeat_lookback_weeks"] == 8

    def test_repeat_lookback_zero_disables_suppression(self, db):
        """A zero lookback preserves previous section selection behavior."""
        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)

        repeated_post_id = _insert_published_content(
            db,
            "x_post",
            "Repeated post remains eligible.",
            days_ago=1,
        )
        fresh_post_id = _insert_published_content(
            db,
            "x_post",
            "Fresh post.",
            days_ago=2,
        )
        db.insert_newsletter_send(
            issue_id="issue-previous",
            subject="Previous",
            content_ids=[repeated_post_id],
            subscriber_count=100,
        )

        content = NewsletterAssembler(db, repeat_lookback_weeks=0).assemble(
            week_start,
            now,
        )

        assert content.source_content_ids[:2] == [repeated_post_id, fresh_post_id]
        assert "Repeated post remains eligible." in content.body_markdown
        assert "suppressed_content_ids" not in content.metadata

    def test_prefers_formats_from_prior_resonant_sends(self, db):
        """Current posts matching resonant source patterns are selected first."""
        historical_tip_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Historical tip",
            eval_score=8.0,
            eval_feedback="Good",
            content_format="tip",
        )
        db.insert_newsletter_send(
            issue_id="issue-resonated",
            subject="Resonated",
            content_ids=[historical_tip_id],
            subscriber_count=100,
            status="resonated",
        )

        now = datetime.now(timezone.utc)
        week_start = now - timedelta(days=7)
        newer_observation_id = _insert_published_content(
            db,
            "x_post",
            "Newer observation post.",
            days_ago=1,
        )
        preferred_tip_id = _insert_published_content(
            db,
            "x_post",
            "Preferred tip post.",
            days_ago=2,
        )
        _set_content_format(db, newer_observation_id, "observation")
        _set_content_format(db, preferred_tip_id, "tip")

        assembler = NewsletterAssembler(db)
        content = assembler.assemble(week_start, now)

        assert content.source_content_ids[:2] == [preferred_tip_id, newer_observation_id]
        assert content.body_markdown.index("Preferred tip post") < content.body_markdown.index(
            "Newer observation post"
        )


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

    def test_extract_blog_excerpt_only_title_and_headers(self):
        """Content with only title and headers returns empty string."""
        content = "TITLE: My Title\n\n## Header 1\n\n## Header 2\n\n### Subheader"
        excerpt = NewsletterAssembler._extract_blog_excerpt(content, max_lines=3)
        assert excerpt == ""

    def test_extract_blog_excerpt_with_max_lines_one(self):
        """max_lines=1 returns only a single line."""
        content = "TITLE: Title\n\nFirst line.\n\nSecond line.\n\nThird line."
        excerpt = NewsletterAssembler._extract_blog_excerpt(content, max_lines=1)
        assert excerpt == "First line."

    def test_extract_first_tweet_multiline_content(self):
        """First tweet can span multiple lines."""
        content = "TWEET 1:\nThis is a tweet\nthat spans multiple\nlines of text\n\nTWEET 2:\nSecond tweet"
        result = NewsletterAssembler._extract_first_tweet(content)
        assert "This is a tweet" in result
        assert "that spans multiple" in result
        assert "lines of text" in result
        assert "TWEET 2" not in result

    def test_extract_blog_title_with_whitespace(self):
        """Title with extra whitespace is properly stripped."""
        content = "TITLE:   Spaced Title  \n\nContent here"
        title = NewsletterAssembler._extract_blog_title(content)
        assert title == "Spaced Title"


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

    @patch("output.newsletter.requests.Session")
    def test_get_email_analytics_success(self, MockSession):
        """Test get_email_analytics() parses Buttondown metrics."""
        mock_session = MockSession.return_value
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "opens": 50,
            "clicks": 12,
            "unsubscriptions": 2,
        }
        mock_session.get.return_value = mock_response

        client = ButtondownClient("test-api-key")
        client.session = mock_session
        metrics = client.get_email_analytics("issue-123")

        assert metrics == NewsletterMetrics(
            issue_id="issue-123",
            opens=50,
            clicks=12,
            unsubscribes=2,
        )
        mock_session.get.assert_called_once_with(
            f"{ButtondownClient.BASE_URL}/emails/issue-123/analytics",
            timeout=30,
        )

    @patch("output.newsletter.requests.Session")
    def test_get_email_analytics_defaults_missing_counts(self, MockSession):
        """Missing Buttondown analytics fields are treated as zero."""
        mock_session = MockSession.return_value
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_session.get.return_value = mock_response

        client = ButtondownClient("test-api-key")
        client.session = mock_session
        metrics = client.get_email_analytics("issue-123")

        assert metrics.opens == 0
        assert metrics.clicks == 0
        assert metrics.unsubscribes == 0

    @patch("output.newsletter.requests.Session")
    def test_get_email_analytics_parses_link_clicks(self, MockSession):
        """Buttondown per-link metrics are normalized and merged by destination."""
        mock_session = MockSession.return_value
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "opens": 50,
            "clicks": 12,
            "unsubscriptions": 2,
            "links": [
                {
                    "url": "https://example.com/post?utm_source=buttondown&id=1",
                    "clicks": 3,
                    "unique_clicks": 2,
                },
                {
                    "url": "https://EXAMPLE.com/post?id=1&utm_campaign=weekly",
                    "click_count": 4,
                    "unique_click_count": 1,
                },
                {
                    "url": "https://example.com/post?id=2",
                    "clicks": 5,
                },
            ],
        }
        mock_session.get.return_value = mock_response

        client = ButtondownClient("test-api-key")
        client.session = mock_session
        metrics = client.get_email_analytics("issue-123")

        assert metrics.link_clicks == [
            NewsletterLinkClick(
                url="https://example.com/post?id=1",
                clicks=7,
                unique_clicks=3,
                raw_url="https://example.com/post?utm_source=buttondown&id=1",
                raw_metrics={
                    "sources": [
                        {
                            "url": "https://example.com/post?utm_source=buttondown&id=1",
                            "clicks": 3,
                            "unique_clicks": 2,
                        },
                        {
                            "url": "https://EXAMPLE.com/post?id=1&utm_campaign=weekly",
                            "click_count": 4,
                            "unique_click_count": 1,
                        },
                    ]
                },
            ),
            NewsletterLinkClick(
                url="https://example.com/post?id=2",
                clicks=5,
                unique_clicks=None,
                raw_url="https://example.com/post?id=2",
                raw_metrics={
                    "sources": [
                        {
                            "url": "https://example.com/post?id=2",
                            "clicks": 5,
                        }
                    ]
                },
            ),
        ]

    def test_normalize_newsletter_link_url_strips_tracking_params(self):
        url = (
            "https://Example.com:443/path?utm_source=newsletter"
            "&content_id=12&ref=archive&gclid=abc#section"
        )

        assert normalize_newsletter_link_url(url) == (
            "https://example.com/path?ref=archive#section"
        )

    @patch("output.newsletter.requests.Session")
    def test_get_email_analytics_http_error(self, MockSession):
        """HTTP failures return None for analytics fetches."""
        mock_session = MockSession.return_value
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_session.get.return_value = mock_response

        client = ButtondownClient("test-api-key")
        client.session = mock_session

        assert client.get_email_analytics("missing") is None

    @patch("output.newsletter.requests.Session")
    def test_get_email_analytics_request_exception(self, MockSession):
        """RequestException returns None for analytics fetches."""
        mock_session = MockSession.return_value
        mock_session.get.side_effect = requests.RequestException("Timeout")

        client = ButtondownClient("test-api-key")
        client.session = mock_session

        assert client.get_email_analytics("issue-123") is None

    @patch("output.newsletter.requests.Session")
    def test_get_subscriber_metrics_success(self, MockSession):
        """Subscriber metrics include aggregate counts exposed by Buttondown."""
        mock_session = MockSession.return_value
        active_response = MagicMock()
        active_response.status_code = 200
        active_response.json.return_value = {
            "count": 125,
            "churn_rate": 0.08,
            "new_subscribers": 9,
            "net_subscriber_change": 6,
        }
        unsubscribed_response = MagicMock()
        unsubscribed_response.status_code = 200
        unsubscribed_response.json.return_value = {"count": 3}
        mock_session.get.side_effect = [active_response, unsubscribed_response]

        client = ButtondownClient("test-api-key")
        client.session = mock_session
        metrics = client.get_subscriber_metrics()

        assert metrics == NewsletterSubscriberMetrics(
            subscriber_count=125,
            active_subscriber_count=125,
            unsubscribes=3,
            churn_rate=0.08,
            new_subscribers=9,
            net_subscriber_change=6,
            raw_metrics={
                "count": 125,
                "churn_rate": 0.08,
                "new_subscribers": 9,
                "net_subscriber_change": 6,
                "unsubscribed_count": 3,
            },
        )
        assert mock_session.get.call_count == 2

    @patch("output.newsletter.requests.Session")
    def test_get_subscriber_metrics_http_error_logs_failure(self, MockSession, caplog):
        """HTTP failures return None and log a warning."""
        mock_session = MockSession.return_value
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_session.get.return_value = mock_response

        client = ButtondownClient("test-api-key")
        client.session = mock_session

        with caplog.at_level(logging.WARNING):
            assert client.get_subscriber_metrics() is None

        assert "Subscriber metrics fetch failed" in caplog.text


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

    def test_insert_newsletter_engagement(self, db):
        send_id = db.insert_newsletter_send(
            issue_id="issue-1",
            subject="Test Subject",
            content_ids=[1],
            subscriber_count=42,
        )

        engagement_id = db.insert_newsletter_engagement(
            newsletter_send_id=send_id,
            issue_id="issue-1",
            opens=10,
            clicks=3,
            unsubscribes=1,
        )

        row = db.conn.execute(
            """SELECT newsletter_send_id, issue_id, opens, clicks, unsubscribes, fetched_at
               FROM newsletter_engagement WHERE id = ?""",
            (engagement_id,),
        ).fetchone()
        assert row["newsletter_send_id"] == send_id
        assert row["issue_id"] == "issue-1"
        assert row["opens"] == 10
        assert row["clicks"] == 3
        assert row["unsubscribes"] == 1
        assert row["fetched_at"]

    def test_get_newsletter_sends_needing_metrics(self, db):
        send_id = db.insert_newsletter_send(
            issue_id="issue-1",
            subject="Needs Metrics",
            content_ids=[],
            subscriber_count=10,
        )
        db.insert_newsletter_send(
            issue_id="",
            subject="No Issue ID",
            content_ids=[],
            subscriber_count=10,
        )

        sends = db.get_newsletter_sends_needing_metrics(max_age_days=90)

        assert len(sends) == 1
        assert sends[0]["id"] == send_id
        assert sends[0]["issue_id"] == "issue-1"

    def test_get_newsletter_sends_needing_metrics_skips_fresh_snapshot(self, db):
        send_id = db.insert_newsletter_send(
            issue_id="issue-1",
            subject="Already Fetched",
            content_ids=[],
            subscriber_count=10,
        )
        db.insert_newsletter_engagement(
            newsletter_send_id=send_id,
            issue_id="issue-1",
            opens=10,
            clicks=3,
            unsubscribes=0,
        )

        sends = db.get_newsletter_sends_needing_metrics(max_age_days=90)

        assert sends == []
