"""Tests for posting schedule optimization system."""

import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from storage.db import Database
from evaluation.posting_schedule import PostingScheduleAnalyzer, TimeWindow


@pytest.fixture
def db():
    """In-memory database for testing."""
    db_instance = Database(":memory:")
    db_instance.connect()
    schema_path = Path(__file__).parent.parent / "schema.sql"
    db_instance.init_schema(str(schema_path))
    yield db_instance
    db_instance.close()


def insert_test_engagement_data(db, data_points):
    """Helper to insert test engagement data.

    Args:
        db: Database instance
        data_points: List of (published_at, engagement_score) tuples
    """
    for published_at, engagement_score in data_points:
        # Insert content
        content_id = db.insert_generated_content(
            content_type="x_thread",
            source_commits=["test_sha"],
            source_messages=["test_uuid"],
            content="Test content",
            eval_score=7.0,
            eval_feedback="Good"
        )
        # Mark as published
        db.mark_published(content_id, "https://x.com/test/123", tweet_id="123")
        # Update published_at to the specified time
        db.conn.execute(
            "UPDATE generated_content SET published_at = ? WHERE id = ?",
            (published_at.isoformat(), content_id)
        )
        db.conn.commit()
        # Insert engagement data
        db.insert_engagement(
            content_id=content_id,
            tweet_id="123",
            like_count=int(engagement_score * 10),
            retweet_count=int(engagement_score * 2),
            reply_count=int(engagement_score),
            quote_count=0,
            engagement_score=engagement_score
        )


def test_analyze_optimal_windows_basic(db):
    """Test basic window analysis with synthetic data."""
    # Create synthetic data: Mondays at 10 AM UTC are best
    # Use recent dates (within last 90 days)
    now = datetime.now(timezone.utc)
    # Find most recent Monday
    days_since_monday = (now.weekday() - 0) % 7
    recent_monday = now - timedelta(days=days_since_monday)
    base_time = recent_monday.replace(hour=10, minute=0, second=0, microsecond=0)

    data_points = []
    # Good window: Mondays at 10 AM (day_of_week=0, hour=10)
    for i in range(5):
        dt = base_time - timedelta(weeks=i)
        data_points.append((dt, 15.0 + i))  # High engagement

    # Medium window: Wednesdays at 14:00 (day_of_week=2, hour=14)
    recent_wednesday = now - timedelta(days=(now.weekday() - 2) % 7)
    base_wednesday = recent_wednesday.replace(hour=14, minute=0, second=0, microsecond=0)
    for i in range(4):
        dt = base_wednesday - timedelta(weeks=i)
        data_points.append((dt, 8.0 + i))  # Medium engagement

    # Poor window: Fridays at 22:00 (day_of_week=4, hour=22)
    recent_friday = now - timedelta(days=(now.weekday() - 4) % 7)
    base_friday = recent_friday.replace(hour=22, minute=0, second=0, microsecond=0)
    for i in range(3):
        dt = base_friday - timedelta(weeks=i)
        data_points.append((dt, 2.0 + i))  # Low engagement

    insert_test_engagement_data(db, data_points)

    # Analyze windows
    analyzer = PostingScheduleAnalyzer(db, min_samples=3)
    windows = analyzer.analyze_optimal_windows(days=90)

    # Should have 3 windows (all with >= 3 samples)
    assert len(windows) == 3

    # Top window should be Monday 10 AM
    top_window = windows[0]
    assert top_window.day_of_week == 0  # Monday
    assert top_window.hour_utc == 10
    assert top_window.avg_engagement > 15.0
    assert top_window.sample_size == 5

    # Second should be Wednesday 14:00
    second_window = windows[1]
    assert second_window.day_of_week == 2  # Wednesday
    assert second_window.hour_utc == 14
    assert second_window.avg_engagement > 8.0
    assert second_window.sample_size == 4

    # Third should be Friday 22:00
    third_window = windows[2]
    assert third_window.day_of_week == 4  # Friday
    assert third_window.hour_utc == 22
    assert third_window.sample_size == 3


def test_analyze_optimal_windows_min_samples(db):
    """Test that windows with insufficient samples are filtered out."""
    now = datetime.now(timezone.utc)
    base_time = now - timedelta(days=7)
    base_time = base_time.replace(hour=10, minute=0, second=0, microsecond=0)

    data_points = []
    # Only 2 samples for this window (below min_samples=3)
    for i in range(2):
        dt = base_time - timedelta(weeks=i)
        data_points.append((dt, 15.0))

    insert_test_engagement_data(db, data_points)

    analyzer = PostingScheduleAnalyzer(db, min_samples=3)
    windows = analyzer.analyze_optimal_windows(days=90)

    # Should have no windows (insufficient samples)
    assert len(windows) == 0


def test_next_optimal_slot(db):
    """Test finding the next optimal posting slot."""
    # Create data for Monday 10 AM and Wednesday 14:00
    now = datetime.now(timezone.utc)
    recent_monday = now - timedelta(days=(now.weekday() - 0) % 7)
    base_monday = recent_monday.replace(hour=10, minute=0, second=0, microsecond=0)

    recent_wednesday = now - timedelta(days=(now.weekday() - 2) % 7)
    base_wednesday = recent_wednesday.replace(hour=14, minute=0, second=0, microsecond=0)

    data_points = []
    for i in range(5):
        data_points.append((base_monday - timedelta(weeks=i), 20.0))
        data_points.append((base_wednesday - timedelta(weeks=i), 15.0))

    insert_test_engagement_data(db, data_points)

    analyzer = PostingScheduleAnalyzer(db, min_samples=3)

    # Test from different times
    # If it's Monday at 8 AM, next slot should be Monday 10 AM (same day, 2+ hours away)
    test_time = datetime(2024, 3, 4, 8, 0, 0, tzinfo=timezone.utc)  # Monday 8 AM

    # Mock the current time by using exclude_hours
    next_slot = analyzer.next_optimal_slot(exclude_hours=2)

    assert next_slot is not None
    # Should be a future time
    assert next_slot > datetime.now(timezone.utc)
    # Should match one of the optimal windows (Monday 10 AM or Wednesday 14:00)
    assert (next_slot.weekday(), next_slot.hour) in [(0, 10), (2, 14)]


def test_should_queue_in_optimal_window(db):
    """Test that should_queue returns False when in a top-3 window."""
    # Create data for Monday 10 AM (best window)
    now = datetime.now(timezone.utc)
    recent_monday = now - timedelta(days=(now.weekday() - 0) % 7)
    base_time = recent_monday.replace(hour=10, minute=0, second=0, microsecond=0)
    data_points = [(base_time - timedelta(weeks=i), 20.0) for i in range(5)]
    insert_test_engagement_data(db, data_points)

    analyzer = PostingScheduleAnalyzer(db, min_samples=3)

    # Check if we should queue when currently in the optimal window
    result = analyzer.should_queue(current_hour_utc=10, current_dow=0)  # Monday 10 AM

    # Should NOT queue (we're in a good window)
    assert result is False


def test_should_queue_outside_optimal_window(db):
    """Test that should_queue returns True when outside top-3 windows."""
    # Create data for Monday 10 AM only
    now = datetime.now(timezone.utc)
    recent_monday = now - timedelta(days=(now.weekday() - 0) % 7)
    base_time = recent_monday.replace(hour=10, minute=0, second=0, microsecond=0)
    data_points = [(base_time - timedelta(weeks=i), 20.0) for i in range(5)]
    insert_test_engagement_data(db, data_points)

    analyzer = PostingScheduleAnalyzer(db, min_samples=3)

    # Check if we should queue when NOT in the optimal window
    result = analyzer.should_queue(current_hour_utc=22, current_dow=4)  # Friday 10 PM

    # Should queue (we're outside the top windows)
    assert result is True


def test_should_queue_no_data(db):
    """Test that should_queue returns False when there's no historical data."""
    analyzer = PostingScheduleAnalyzer(db, min_samples=3)

    # With no data, should post immediately (no optimization possible)
    result = analyzer.should_queue(current_hour_utc=10, current_dow=0)

    assert result is False


def test_queue_methods(db):
    """Test database queue management methods."""
    # Insert a piece of content
    content_id = db.insert_generated_content(
        content_type="x_thread",
        source_commits=["sha1"],
        source_messages=["uuid1"],
        content="Test thread",
        eval_score=8.0,
        eval_feedback="Great"
    )

    # Queue for publishing
    scheduled_at = datetime.now(timezone.utc) + timedelta(hours=2)
    queue_id = db.queue_for_publishing(content_id, scheduled_at.isoformat(), platform='all')

    assert queue_id > 0

    # Check that we can retrieve due items
    # Should not be due yet
    now_iso = datetime.now(timezone.utc).isoformat()
    due_items = db.get_due_queue_items(now_iso)
    assert len(due_items) == 0

    # Should be due in the future
    future_iso = (datetime.now(timezone.utc) + timedelta(hours=3)).isoformat()
    due_items = db.get_due_queue_items(future_iso)
    assert len(due_items) == 1
    assert due_items[0]['id'] == queue_id
    assert due_items[0]['content_id'] == content_id
    assert due_items[0]['platform'] == 'all'
    assert due_items[0]['content'] == "Test thread"

    # Mark as published
    db.mark_queue_published(queue_id)

    # Should no longer be in queue
    due_items = db.get_due_queue_items(future_iso)
    assert len(due_items) == 0


def test_mark_queue_failed(db):
    """Test marking queue item as failed."""
    content_id = db.insert_generated_content(
        content_type="x_thread",
        source_commits=["sha1"],
        source_messages=["uuid1"],
        content="Test thread",
        eval_score=8.0,
        eval_feedback="Great"
    )

    scheduled_at = datetime.now(timezone.utc) + timedelta(hours=2)
    queue_id = db.queue_for_publishing(content_id, scheduled_at.isoformat(), platform='x')

    # Mark as failed
    db.mark_queue_failed(queue_id, "Rate limit exceeded")

    # Verify status
    cursor = db.conn.execute(
        "SELECT status, error FROM publish_queue WHERE id = ?",
        (queue_id,)
    )
    row = cursor.fetchone()
    assert row['status'] == 'failed'
    assert row['error'] == "Rate limit exceeded"


def test_cancel_queued(db):
    """Test canceling queued items for a content ID."""
    content_id = db.insert_generated_content(
        content_type="x_thread",
        source_commits=["sha1"],
        source_messages=["uuid1"],
        content="Test thread",
        eval_score=8.0,
        eval_feedback="Great"
    )

    # Queue multiple items
    scheduled_at1 = datetime.now(timezone.utc) + timedelta(hours=2)
    scheduled_at2 = datetime.now(timezone.utc) + timedelta(hours=4)
    queue_id1 = db.queue_for_publishing(content_id, scheduled_at1.isoformat(), platform='x')
    queue_id2 = db.queue_for_publishing(content_id, scheduled_at2.isoformat(), platform='bluesky')

    # Cancel all queued items for this content
    db.cancel_queued(content_id)

    # Verify both are cancelled
    cursor = db.conn.execute(
        "SELECT status FROM publish_queue WHERE content_id = ?",
        (content_id,)
    )
    rows = cursor.fetchall()
    assert len(rows) == 2
    assert all(row['status'] == 'cancelled' for row in rows)


def test_full_flow_integration(db):
    """Test the full flow: content -> queue -> publish."""
    # Setup: Create engagement data for optimal windows
    now = datetime.now(timezone.utc)
    recent_monday = now - timedelta(days=(now.weekday() - 0) % 7)
    base_time = recent_monday.replace(hour=10, minute=0, second=0, microsecond=0)
    data_points = [(base_time - timedelta(weeks=i), 20.0) for i in range(5)]
    insert_test_engagement_data(db, data_points)

    # Simulate pipeline generating content at a non-optimal time
    content_id = db.insert_generated_content(
        content_type="x_thread",
        source_commits=["sha1"],
        source_messages=["uuid1"],
        content="New thread",
        eval_score=8.5,
        eval_feedback="Excellent"
    )

    # Check if we should queue (simulating Friday at 10 PM)
    analyzer = PostingScheduleAnalyzer(db, min_samples=3)
    should_queue = analyzer.should_queue(current_hour_utc=22, current_dow=4)

    assert should_queue is True

    # Get next optimal slot
    next_slot = analyzer.next_optimal_slot(exclude_hours=2)
    assert next_slot is not None

    # Queue the content
    queue_id = db.queue_for_publishing(content_id, next_slot.isoformat(), platform='all')
    assert queue_id > 0

    # Later: publish_queue.py runs and finds due items
    future_time = next_slot + timedelta(minutes=10)
    due_items = db.get_due_queue_items(future_time.isoformat())

    assert len(due_items) == 1
    assert due_items[0]['content_id'] == content_id

    # Simulate successful publishing
    db.mark_published(content_id, "https://x.com/test/456", tweet_id="456")
    db.mark_queue_published(queue_id)

    # Verify content is marked as published
    cursor = db.conn.execute(
        "SELECT published, published_url FROM generated_content WHERE id = ?",
        (content_id,)
    )
    row = cursor.fetchone()
    assert row['published'] == 1
    assert row['published_url'] == "https://x.com/test/456"

    # Verify queue item is marked as published
    cursor = db.conn.execute(
        "SELECT status FROM publish_queue WHERE id = ?",
        (queue_id,)
    )
    row = cursor.fetchone()
    assert row['status'] == 'published'


def test_confidence_calculation(db):
    """Test that confidence increases with sample size."""
    now = datetime.now(timezone.utc)
    recent_monday = now - timedelta(days=(now.weekday() - 0) % 7)
    base_time = recent_monday.replace(hour=10, minute=0, second=0, microsecond=0)

    # Test with exactly min_samples
    data_points = [(base_time - timedelta(weeks=i), 15.0) for i in range(3)]
    insert_test_engagement_data(db, data_points)

    analyzer = PostingScheduleAnalyzer(db, min_samples=3)
    windows = analyzer.analyze_optimal_windows(days=90)

    assert len(windows) == 1
    assert windows[0].sample_size == 3
    # Confidence should be 3/(3+5) = 0.375
    assert 0.35 < windows[0].confidence < 0.4

    # Add more samples
    data_points = [(base_time - timedelta(weeks=i), 15.0) for i in range(3, 10)]
    insert_test_engagement_data(db, data_points)

    windows = analyzer.analyze_optimal_windows(days=90)
    assert len(windows) == 1
    assert windows[0].sample_size == 10
    # Confidence should be 10/(10+5) = 0.667
    assert 0.65 < windows[0].confidence < 0.7


def test_lookback_period(db):
    """Test that analyze_optimal_windows respects the lookback period."""
    now = datetime.now(timezone.utc)

    # Recent data (within 90 days)
    recent_time = now - timedelta(days=30)
    recent_data = [(recent_time + timedelta(weeks=i), 15.0) for i in range(5)]

    # Old data (beyond 90 days)
    old_time = now - timedelta(days=120)
    old_data = [(old_time + timedelta(weeks=i), 20.0) for i in range(5)]

    insert_test_engagement_data(db, recent_data + old_data)

    # Analyze with 90-day lookback
    analyzer = PostingScheduleAnalyzer(db, min_samples=3)
    windows = analyzer.analyze_optimal_windows(days=90)

    # Should only see the recent window
    assert len(windows) == 1
    # Engagement should be from recent data (~15.0), not old data (~20.0)
    assert 14.0 < windows[0].avg_engagement < 16.0
