"""Tests for publish_queue.py — scheduled post publishing from queue."""

import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock, call
from dataclasses import dataclass

import pytest

# Mock the atproto module before any imports
sys.modules['atproto'] = MagicMock()
sys.modules['atproto.exceptions'] = MagicMock(AtProtocolError=Exception)

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from storage.db import Database


@dataclass
class FakePostResult:
    success: bool
    url: str = ""
    tweet_id: str = ""
    error: str = ""
    uri: str = ""


class FakeXClient:
    def __init__(self, post_result=None, thread_result=None):
        self.post_result = post_result
        self.thread_result = thread_result or post_result
        self.posts = []
        self.threads = []

    def post(self, content):
        self.posts.append(content)
        return self.post_result

    def post_thread(self, tweets):
        self.threads.append(tweets)
        return self.thread_result


class FakeBlueskyClient:
    def __init__(self, post_result=None, thread_result=None):
        self.post_result = post_result
        self.thread_result = thread_result or post_result
        self.posts = []
        self.threads = []

    def post(self, content):
        self.posts.append(content)
        return self.post_result

    def post_thread(self, tweets):
        self.threads.append(tweets)
        return self.thread_result


class FakeCrossPoster:
    def __init__(self, bluesky_client=None):
        self.bluesky_client = bluesky_client

    def adapt_for_bluesky(self, text, content_type):
        return f"bsky:{text}"


def make_config(bluesky_enabled=True):
    config = MagicMock()
    config.x.api_key = "test_key"
    config.x.api_secret = "test_secret"
    config.x.access_token = "test_token"
    config.x.access_token_secret = "test_token_secret"
    if bluesky_enabled:
        config.bluesky.enabled = True
        config.bluesky.handle = "test.bsky.social"
        config.bluesky.app_password = "test_password"
    else:
        config.bluesky = None
    return config


# --- Test Fixtures ---


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
def base_time():
    """Fixed base time for testing scheduling."""
    return datetime(2026, 4, 17, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def populated_db(test_db, base_time):
    """Database with queue items at various scheduled times."""
    # Create content items first
    content_ids = []
    for i in range(5):
        content_id = test_db.conn.execute(
            """INSERT INTO generated_content
               (content, content_type, eval_score, published)
               VALUES (?, ?, ?, ?)""",
            (f"Test post content {i}", "x_post", 7.0, 0)
        ).lastrowid
        content_ids.append(content_id)
    test_db.conn.commit()

    # Create queue items with different scheduled times
    queue_items = [
        # Past due - should be processed
        {
            "content_id": content_ids[0],
            "scheduled_at": (base_time - timedelta(hours=2)).isoformat(),
            "platform": "x",
            "status": "queued"
        },
        # Just due - should be processed
        {
            "content_id": content_ids[1],
            "scheduled_at": base_time.isoformat(),
            "platform": "all",
            "status": "queued"
        },
        # Future - should NOT be processed
        {
            "content_id": content_ids[2],
            "scheduled_at": (base_time + timedelta(hours=1)).isoformat(),
            "platform": "x",
            "status": "queued"
        },
        # Way in future - should NOT be processed
        {
            "content_id": content_ids[3],
            "scheduled_at": (base_time + timedelta(days=1)).isoformat(),
            "platform": "bluesky",
            "status": "queued"
        },
        # Past due but already published - should NOT be processed
        {
            "content_id": content_ids[4],
            "scheduled_at": (base_time - timedelta(hours=3)).isoformat(),
            "platform": "x",
            "status": "published"
        },
    ]

    for item in queue_items:
        test_db.conn.execute(
            """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
               VALUES (?, ?, ?, ?)""",
            (item["content_id"], item["scheduled_at"], item["platform"], item["status"])
        )
    test_db.conn.commit()

    return test_db, content_ids


# --- Test Cases ---


def test_no_items_due_returns_empty(test_db, base_time):
    """Test that future items are not returned when querying for due items."""
    # Create content
    content_id = test_db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, ?, ?, ?)""",
        ("Future post", "x_post", 7.0, 0)
    ).lastrowid

    # Schedule for future
    future_time = base_time + timedelta(hours=2)
    test_db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (content_id, future_time.isoformat(), "x", "queued")
    )
    test_db.conn.commit()

    # Query at base_time - should return empty
    due_items = test_db.get_due_queue_items(base_time.isoformat())
    assert len(due_items) == 0


def test_items_scheduled_for_past_are_returned(test_db, base_time):
    """Test that items scheduled in the past are returned as due."""
    # Create content
    content_id = test_db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, ?, ?, ?)""",
        ("Past due post", "x_post", 7.0, 0)
    ).lastrowid

    # Schedule for past
    past_time = base_time - timedelta(hours=1)
    test_db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (content_id, past_time.isoformat(), "x", "queued")
    )
    test_db.conn.commit()

    # Query at base_time - should return the item
    due_items = test_db.get_due_queue_items(base_time.isoformat())
    assert len(due_items) == 1
    assert due_items[0]["content_id"] == content_id


def test_items_scheduled_for_exact_now_are_returned(test_db, base_time):
    """Test that items scheduled for exactly 'now' are returned as due."""
    # Create content
    content_id = test_db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, ?, ?, ?)""",
        ("Exact time post", "x_post", 7.0, 0)
    ).lastrowid

    # Schedule for exact base_time
    test_db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (content_id, base_time.isoformat(), "x", "queued")
    )
    test_db.conn.commit()

    # Query at base_time - should return the item
    due_items = test_db.get_due_queue_items(base_time.isoformat())
    assert len(due_items) == 1
    assert due_items[0]["content_id"] == content_id


def test_due_items_returned_in_scheduled_order(populated_db, base_time):
    """Test that due items are returned ordered by scheduled_at ASC."""
    test_db, content_ids = populated_db

    # Query for due items
    due_items = test_db.get_due_queue_items(base_time.isoformat())

    # Should return 2 items (past due and exact time)
    assert len(due_items) == 2

    # First should be the one scheduled 2 hours ago (earlier)
    assert due_items[0]["content_id"] == content_ids[0]
    # Second should be the one scheduled at base_time
    assert due_items[1]["content_id"] == content_ids[1]

    # Verify chronological order
    assert due_items[0]["scheduled_at"] < due_items[1]["scheduled_at"]


def test_empty_queue_is_idempotent(test_db, base_time):
    """Test that querying an empty queue multiple times works correctly."""
    # Query empty queue multiple times
    for _ in range(3):
        due_items = test_db.get_due_queue_items(base_time.isoformat())
        assert len(due_items) == 0


def test_published_items_not_returned(populated_db, base_time):
    """Test that items with status='published' are not returned."""
    test_db, content_ids = populated_db

    # The fifth item is past due but already published
    due_items = test_db.get_due_queue_items(base_time.isoformat())

    # Should only return queued items, not the published one
    returned_ids = [item["content_id"] for item in due_items]
    assert content_ids[4] not in returned_ids


def test_mark_queue_published_updates_status(test_db, base_time):
    """Test that mark_queue_published correctly updates queue item status."""
    # Create content and queue item
    content_id = test_db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, ?, ?, ?)""",
        ("Test post", "x_post", 7.0, 0)
    ).lastrowid

    queue_id = test_db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (content_id, base_time.isoformat(), "x", "queued")
    ).lastrowid
    test_db.conn.commit()

    # Mark as published
    test_db.mark_queue_published(queue_id)

    # Verify status updated
    row = test_db.conn.execute(
        "SELECT status, published_at FROM publish_queue WHERE id = ?",
        (queue_id,)
    ).fetchone()

    assert row["status"] == "published"
    assert row["published_at"] is not None
    # Verify it's a valid ISO timestamp
    datetime.fromisoformat(row["published_at"])


def test_mark_queue_failed_updates_with_error(test_db, base_time):
    """Test that mark_queue_failed correctly updates status and error message."""
    # Create content and queue item
    content_id = test_db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, ?, ?, ?)""",
        ("Test post", "x_post", 7.0, 0)
    ).lastrowid

    queue_id = test_db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (content_id, base_time.isoformat(), "x", "queued")
    ).lastrowid
    test_db.conn.commit()

    # Mark as failed with error
    error_msg = "X: API rate limit exceeded"
    test_db.mark_queue_failed(queue_id, error_msg)

    # Verify status and error updated
    row = test_db.conn.execute(
        "SELECT status, error FROM publish_queue WHERE id = ?",
        (queue_id,)
    ).fetchone()

    assert row["status"] == "failed"
    assert row["error"] == error_msg


def test_main_processes_due_items(test_db, base_time):
    """Test that main() processes all due items and marks them published."""
    # Create 3 posts scheduled for the past
    content_ids = []
    for i in range(3):
        content_id = test_db.conn.execute(
            """INSERT INTO generated_content
               (content, content_type, eval_score, published)
               VALUES (?, ?, ?, ?)""",
            (f"Post {i}", "x_post", 7.0, 0)
        ).lastrowid
        content_ids.append(content_id)

        test_db.conn.execute(
            """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
               VALUES (?, ?, ?, ?)""",
            (content_id, (base_time - timedelta(hours=i+1)).isoformat(), "x", "queued")
        )
    test_db.conn.commit()

    # Mock the dependencies
    mock_config = MagicMock()
    mock_config.x.api_key = "test_key"
    mock_config.x.api_secret = "test_secret"
    mock_config.x.access_token = "test_token"
    mock_config.x.access_token_secret = "test_token_secret"
    mock_config.bluesky = None

    @dataclass
    class PostResult:
        success: bool
        url: str = ""
        tweet_id: str = ""
        error: str = ""

    mock_x_client = MagicMock()
    mock_x_client.post.return_value = PostResult(
        success=True,
        url="https://x.com/test/status/123",
        tweet_id="123"
    )

    with patch("publish_queue.script_context") as mock_context, \
         patch("publish_queue.XClient", return_value=mock_x_client), \
         patch("publish_queue.update_monitoring"), \
         patch("publish_queue.datetime") as mock_datetime:

        # Freeze time to base_time
        mock_datetime.now.return_value = base_time
        mock_context.return_value.__enter__.return_value = (mock_config, test_db)
        mock_context.return_value.__exit__.return_value = False

        # Import and run main
        from publish_queue import main
        main()

    # Verify all 3 items were marked as published
    published_count = test_db.conn.execute(
        "SELECT COUNT(*) as cnt FROM publish_queue WHERE status = 'published'"
    ).fetchone()["cnt"]
    assert published_count == 3

    # Verify X client was called 3 times
    assert mock_x_client.post.call_count == 3


def test_main_handles_empty_queue(test_db, base_time):
    """Test that main() handles empty queue gracefully."""
    mock_config = MagicMock()
    mock_config.x.api_key = "test_key"
    mock_config.x.api_secret = "test_secret"
    mock_config.x.access_token = "test_token"
    mock_config.x.access_token_secret = "test_token_secret"
    mock_config.bluesky = None

    with patch("publish_queue.script_context") as mock_context, \
         patch("publish_queue.XClient") as mock_x_class, \
         patch("publish_queue.update_monitoring") as mock_monitor, \
         patch("publish_queue.datetime") as mock_datetime:

        mock_datetime.now.return_value = base_time
        mock_context.return_value.__enter__.return_value = (mock_config, test_db)
        mock_context.return_value.__exit__.return_value = False

        from publish_queue import main
        main()

    # Verify monitoring was still updated
    mock_monitor.assert_called_once_with("run-publish-queue")

    # Verify no posts were attempted
    mock_x_class.return_value.post.assert_not_called()


def test_main_handles_x_posting_failure(test_db, base_time):
    """Test that main() marks queue item as failed when X posting fails."""
    # Create one post
    content_id = test_db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, ?, ?, ?)""",
        ("Test post", "x_post", 7.0, 0)
    ).lastrowid

    queue_id = test_db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (content_id, base_time.isoformat(), "x", "queued")
    ).lastrowid
    test_db.conn.commit()

    # Mock X client to return failure
    mock_config = MagicMock()
    mock_config.x.api_key = "test_key"
    mock_config.x.api_secret = "test_secret"
    mock_config.x.access_token = "test_token"
    mock_config.x.access_token_secret = "test_token_secret"
    mock_config.bluesky = None

    @dataclass
    class PostResult:
        success: bool
        error: str = ""

    mock_x_client = MagicMock()
    mock_x_client.post.return_value = PostResult(
        success=False,
        error="Rate limit exceeded"
    )

    with patch("publish_queue.script_context") as mock_context, \
         patch("publish_queue.XClient", return_value=mock_x_client), \
         patch("publish_queue.update_monitoring"), \
         patch("publish_queue.datetime") as mock_datetime:

        mock_datetime.now.return_value = base_time
        mock_context.return_value.__enter__.return_value = (mock_config, test_db)
        mock_context.return_value.__exit__.return_value = False

        from publish_queue import main
        main()

    # Verify item was marked as failed
    row = test_db.conn.execute(
        "SELECT status, error FROM publish_queue WHERE id = ?",
        (queue_id,)
    ).fetchone()

    assert row["status"] == "failed"
    assert "Rate limit exceeded" in row["error"]


def test_main_processes_thread_content_type(test_db, base_time):
    """Test that main() correctly handles x_thread content type."""
    # Create thread content
    thread_content = "Tweet 1\n---\nTweet 2\n---\nTweet 3"
    content_id = test_db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, ?, ?, ?)""",
        (thread_content, "x_thread", 7.0, 0)
    ).lastrowid

    test_db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (content_id, base_time.isoformat(), "x", "queued")
    )
    test_db.conn.commit()

    # Mock dependencies
    mock_config = MagicMock()
    mock_config.x.api_key = "test_key"
    mock_config.x.api_secret = "test_secret"
    mock_config.x.access_token = "test_token"
    mock_config.x.access_token_secret = "test_token_secret"
    mock_config.bluesky = None

    @dataclass
    class PostResult:
        success: bool
        url: str = ""
        tweet_id: str = ""

    mock_x_client = MagicMock()
    mock_x_client.post_thread.return_value = PostResult(
        success=True,
        url="https://x.com/test/status/123",
        tweet_id="123"
    )

    with patch("publish_queue.script_context") as mock_context, \
         patch("publish_queue.XClient", return_value=mock_x_client), \
         patch("publish_queue.parse_thread_content") as mock_parse, \
         patch("publish_queue.update_monitoring"), \
         patch("publish_queue.datetime") as mock_datetime:

        mock_datetime.now.return_value = base_time
        mock_parse.return_value = ["Tweet 1", "Tweet 2", "Tweet 3"]
        mock_context.return_value.__enter__.return_value = (mock_config, test_db)
        mock_context.return_value.__exit__.return_value = False

        from publish_queue import main
        main()

    # Verify parse_thread_content was called
    mock_parse.assert_called_once_with(thread_content)

    # Verify post_thread was called instead of post
    mock_x_client.post_thread.assert_called_once()
    mock_x_client.post.assert_not_called()


def test_main_cross_posts_to_bluesky_when_platform_all(test_db, base_time):
    """Test that main() cross-posts to Bluesky when platform='all' and Bluesky is enabled."""
    # Create content
    content_id = test_db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, ?, ?, ?)""",
        ("Test post", "x_post", 7.0, 0)
    ).lastrowid

    test_db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (content_id, base_time.isoformat(), "all", "queued")
    )
    test_db.conn.commit()

    # Mock config with Bluesky enabled
    mock_config = MagicMock()
    mock_config.x.api_key = "test_key"
    mock_config.x.api_secret = "test_secret"
    mock_config.x.access_token = "test_token"
    mock_config.x.access_token_secret = "test_token_secret"
    mock_config.bluesky.enabled = True
    mock_config.bluesky.handle = "test.bsky.social"
    mock_config.bluesky.app_password = "test_password"

    @dataclass
    class PostResult:
        success: bool
        url: str = ""
        tweet_id: str = ""
        uri: str = ""

    mock_x_client = MagicMock()
    mock_x_client.post.return_value = PostResult(
        success=True,
        url="https://x.com/test/status/123",
        tweet_id="123"
    )

    mock_bluesky_client = MagicMock()
    mock_bluesky_client.post.return_value = PostResult(
        success=True,
        uri="at://did:plc:test/app.bsky.feed.post/abc",
        url="https://bsky.app/profile/test.bsky.social/post/abc"
    )

    with patch("publish_queue.script_context") as mock_context, \
         patch("publish_queue.XClient", return_value=mock_x_client), \
         patch("publish_queue.BlueskyClient", return_value=mock_bluesky_client), \
         patch("publish_queue.CrossPoster") as mock_cross_poster_class, \
         patch("publish_queue.update_monitoring"), \
         patch("publish_queue.datetime") as mock_datetime:

        mock_datetime.now.return_value = base_time
        mock_context.return_value.__enter__.return_value = (mock_config, test_db)
        mock_context.return_value.__exit__.return_value = False

        mock_cross_poster = MagicMock()
        mock_cross_poster.adapt_for_bluesky.return_value = "Adapted post"
        mock_cross_poster_class.return_value = mock_cross_poster

        from publish_queue import main
        main()

    # Verify both platforms were posted to
    mock_x_client.post.assert_called_once()
    mock_bluesky_client.post.assert_called_once()

    # Verify content was marked published on both platforms
    row = test_db.conn.execute(
        "SELECT published, published_url, bluesky_uri FROM generated_content WHERE id = ?",
        (content_id,)
    ).fetchone()

    assert row["published"] == 1
    assert row["published_url"] == "https://x.com/test/status/123"
    assert row["bluesky_uri"] == "at://did:plc:test/app.bsky.feed.post/abc"


def test_main_all_attempts_bluesky_when_x_fails_and_records_only_x_error(test_db, base_time):
    """If X fails and Bluesky succeeds, keep Bluesky state and name only X in queue error."""
    content_id = test_db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, ?, ?, ?)""",
        ("Test post", "x_post", 7.0, 0)
    ).lastrowid
    queue_id = test_db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (content_id, base_time.isoformat(), "all", "queued")
    ).lastrowid
    test_db.conn.commit()

    fake_x = FakeXClient(FakePostResult(success=False, error="Rate limit exceeded"))
    fake_bluesky = FakeBlueskyClient(FakePostResult(
        success=True,
        uri="at://did:plc:test/app.bsky.feed.post/abc",
        url="https://bsky.app/profile/test.bsky.social/post/abc",
    ))

    with patch("publish_queue.script_context") as mock_context, \
         patch("publish_queue.XClient", return_value=fake_x), \
         patch("publish_queue.BlueskyClient", return_value=fake_bluesky), \
         patch("publish_queue.CrossPoster", FakeCrossPoster), \
         patch("publish_queue.update_monitoring"), \
         patch("publish_queue.datetime") as mock_datetime:

        mock_datetime.now.return_value = base_time
        mock_context.return_value.__enter__.return_value = (make_config(), test_db)
        mock_context.return_value.__exit__.return_value = False

        from publish_queue import main
        main()

    queue_row = test_db.conn.execute(
        "SELECT status, error FROM publish_queue WHERE id = ?",
        (queue_id,)
    ).fetchone()
    content_row = test_db.conn.execute(
        "SELECT published, published_url, bluesky_uri FROM generated_content WHERE id = ?",
        (content_id,)
    ).fetchone()

    assert fake_x.posts == ["Test post"]
    assert fake_bluesky.posts == ["bsky:Test post"]
    assert queue_row["status"] == "failed"
    assert queue_row["error"] == "X: Rate limit exceeded"
    assert content_row["published"] == 0
    assert content_row["published_url"] is None
    assert content_row["bluesky_uri"] == "at://did:plc:test/app.bsky.feed.post/abc"


def test_main_all_skips_previously_successful_x_on_bluesky_retry(test_db, base_time):
    """A retry should not repost X when only Bluesky is still missing."""
    content_id = test_db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published, published_url, tweet_id)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("Retry post", "x_post", 7.0, 1, "https://x.com/test/status/123", "123")
    ).lastrowid
    queue_id = test_db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status, error)
           VALUES (?, ?, ?, ?, ?)""",
        (content_id, base_time.isoformat(), "all", "queued", "Bluesky: timeout")
    ).lastrowid
    test_db.conn.commit()

    fake_x = FakeXClient(FakePostResult(
        success=True,
        url="https://x.com/test/status/duplicate",
        tweet_id="duplicate",
    ))
    fake_bluesky = FakeBlueskyClient(FakePostResult(
        success=True,
        uri="at://did:plc:test/app.bsky.feed.post/retry",
        url="https://bsky.app/profile/test.bsky.social/post/retry",
    ))

    with patch("publish_queue.script_context") as mock_context, \
         patch("publish_queue.XClient", return_value=fake_x), \
         patch("publish_queue.BlueskyClient", return_value=fake_bluesky), \
         patch("publish_queue.CrossPoster", FakeCrossPoster), \
         patch("publish_queue.update_monitoring"), \
         patch("publish_queue.datetime") as mock_datetime:

        mock_datetime.now.return_value = base_time
        mock_context.return_value.__enter__.return_value = (make_config(), test_db)
        mock_context.return_value.__exit__.return_value = False

        from publish_queue import main
        main()

    queue_row = test_db.conn.execute(
        "SELECT status, error FROM publish_queue WHERE id = ?",
        (queue_id,)
    ).fetchone()
    content_row = test_db.conn.execute(
        "SELECT published_url, tweet_id, bluesky_uri FROM generated_content WHERE id = ?",
        (content_id,)
    ).fetchone()

    assert fake_x.posts == []
    assert fake_bluesky.posts == ["bsky:Retry post"]
    assert queue_row["status"] == "published"
    assert queue_row["error"] is None
    assert content_row["published_url"] == "https://x.com/test/status/123"
    assert content_row["tweet_id"] == "123"
    assert content_row["bluesky_uri"] == "at://did:plc:test/app.bsky.feed.post/retry"


def test_main_all_marks_published_without_posting_when_all_platforms_done(test_db, base_time):
    """A queued item with all requested platform state already present should only close the queue."""
    content_id = test_db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published, published_url, tweet_id, bluesky_uri)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "Already posted",
            "x_post",
            7.0,
            1,
            "https://x.com/test/status/123",
            "123",
            "at://did:plc:test/app.bsky.feed.post/already",
        )
    ).lastrowid
    queue_id = test_db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (content_id, base_time.isoformat(), "all", "queued")
    ).lastrowid
    test_db.conn.commit()

    fake_x = FakeXClient(FakePostResult(success=False, error="should not post"))
    fake_bluesky = FakeBlueskyClient(FakePostResult(success=False, error="should not post"))

    with patch("publish_queue.script_context") as mock_context, \
         patch("publish_queue.XClient", return_value=fake_x), \
         patch("publish_queue.BlueskyClient", return_value=fake_bluesky), \
         patch("publish_queue.CrossPoster", FakeCrossPoster), \
         patch("publish_queue.update_monitoring"), \
         patch("publish_queue.datetime") as mock_datetime:

        mock_datetime.now.return_value = base_time
        mock_context.return_value.__enter__.return_value = (make_config(), test_db)
        mock_context.return_value.__exit__.return_value = False

        from publish_queue import main
        main()

    queue_row = test_db.conn.execute(
        "SELECT status, error FROM publish_queue WHERE id = ?",
        (queue_id,)
    ).fetchone()

    assert fake_x.posts == []
    assert fake_bluesky.posts == []
    assert queue_row["status"] == "published"
    assert queue_row["error"] is None


def test_queue_respects_time_boundary(test_db, base_time):
    """Test precise time boundary handling for scheduled_at."""
    # Create posts at exact boundaries
    times = [
        base_time - timedelta(seconds=1),  # Just before now - should be included
        base_time,                          # Exact now - should be included
        base_time + timedelta(seconds=1),  # Just after now - should NOT be included
    ]

    for i, scheduled_time in enumerate(times):
        content_id = test_db.conn.execute(
            """INSERT INTO generated_content
               (content, content_type, eval_score, published)
               VALUES (?, ?, ?, ?)""",
            (f"Post {i}", "x_post", 7.0, 0)
        ).lastrowid

        test_db.conn.execute(
            """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
               VALUES (?, ?, ?, ?)""",
            (content_id, scheduled_time.isoformat(), "x", "queued")
        )
    test_db.conn.commit()

    # Query at base_time
    due_items = test_db.get_due_queue_items(base_time.isoformat())

    # Should return exactly 2 items (before and at, but not after)
    assert len(due_items) == 2
