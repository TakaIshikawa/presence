"""Tests for format performance analysis and weighted selection."""

import pytest
from datetime import datetime, timedelta, timezone
from storage.db import Database
from evaluation.format_performance import FormatPerformanceAnalyzer, FormatStat, FormatReport
from knowledge.embeddings import serialize_embedding
import numpy as np


@pytest.fixture
def db():
    """In-memory test database."""
    db = Database(":memory:")
    db.connect()
    db.init_schema("schema.sql")
    yield db
    db.close()


def test_format_tracking_round_trip(db):
    """Test that content_format is stored and retrieved correctly."""
    content_id = db.insert_generated_content(
        content_type="x_thread",
        source_commits=["abc123"],
        source_messages=["uuid1"],
        content="Test thread content",
        eval_score=8.5,
        eval_feedback="Good thread",
        content_format="bold_claim"
    )

    # Verify format was stored
    cursor = db.conn.execute(
        "SELECT content_format FROM generated_content WHERE id = ?",
        (content_id,)
    )
    row = cursor.fetchone()
    assert row is not None
    assert row[0] == "bold_claim"


def test_get_format_engagement_stats_empty(db):
    """Test stats query with no data."""
    stats = db.get_format_engagement_stats(days=90)
    assert stats == []


def test_get_format_engagement_stats(db):
    """Test format engagement stats aggregation."""
    now = datetime.now(timezone.utc)

    # Insert content with different formats
    formats_data = [
        ("micro_story", 8.0, 10, "resonated"),
        ("micro_story", 7.5, 8, "resonated"),
        ("bold_claim", 6.0, 5, "low_resonance"),
        ("bold_claim", 7.0, 6, None),  # Not yet classified
        ("question", 9.0, 15, "resonated"),
    ]

    for format_name, eval_score, engagement, auto_quality in formats_data:
        content_id = db.insert_generated_content(
            content_type="x_thread",
            source_commits=["abc"],
            source_messages=["uuid"],
            content=f"Content for {format_name}",
            eval_score=eval_score,
            eval_feedback="Test",
            content_format=format_name
        )
        # Mark as published
        db.mark_published(content_id, "https://x.com/test/123", tweet_id="123")

        # Set published_at to be within the lookback window
        db.conn.execute(
            "UPDATE generated_content SET published_at = ? WHERE id = ?",
            (now.isoformat(), content_id)
        )

        # Set auto quality
        if auto_quality:
            db.conn.execute(
                "UPDATE generated_content SET auto_quality = ? WHERE id = ?",
                (auto_quality, content_id)
            )

        # Insert engagement data
        db.conn.execute(
            """INSERT INTO post_engagement
               (content_id, tweet_id, like_count, retweet_count, reply_count,
                quote_count, engagement_score, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (content_id, "123", engagement, 0, 0, 0, float(engagement), now.isoformat())
        )

    db.conn.commit()

    # Query stats
    stats = db.get_format_engagement_stats(days=90)

    # Should have 3 formats
    assert len(stats) == 3

    # Convert to dict for easier testing
    stats_by_format = {s["format"]: s for s in stats}

    # Check micro_story stats (2 posts, avg engagement = 9.0)
    assert stats_by_format["micro_story"]["count"] == 2
    assert stats_by_format["micro_story"]["avg_engagement"] == pytest.approx(9.0)
    assert stats_by_format["micro_story"]["resonated_count"] == 2
    assert stats_by_format["micro_story"]["total_classified"] == 2

    # Check bold_claim stats (2 posts, avg engagement = 5.5)
    assert stats_by_format["bold_claim"]["count"] == 2
    assert stats_by_format["bold_claim"]["avg_engagement"] == pytest.approx(5.5)
    assert stats_by_format["bold_claim"]["resonated_count"] == 0
    assert stats_by_format["bold_claim"]["total_classified"] == 1  # Only 1 classified

    # Check question stats (1 post, avg engagement = 15.0)
    assert stats_by_format["question"]["count"] == 1
    assert stats_by_format["question"]["avg_engagement"] == pytest.approx(15.0)
    assert stats_by_format["question"]["resonated_count"] == 1
    assert stats_by_format["question"]["total_classified"] == 1


def test_analyze_format_performance(db):
    """Test format performance analysis."""
    now = datetime.now(timezone.utc)

    # Insert test data
    for i in range(5):
        content_id = db.insert_generated_content(
            content_type="x_thread",
            source_commits=["abc"],
            source_messages=["uuid"],
            content=f"Test {i}",
            eval_score=8.0,
            eval_feedback="Test",
            content_format="micro_story"
        )
        db.mark_published(content_id, f"https://x.com/test/{i}", tweet_id=str(i))
        db.conn.execute(
            "UPDATE generated_content SET published_at = ?, auto_quality = ? WHERE id = ?",
            (now.isoformat(), "resonated", content_id)
        )
        db.conn.execute(
            """INSERT INTO post_engagement
               (content_id, tweet_id, engagement_score, fetched_at)
               VALUES (?, ?, ?, ?)""",
            (content_id, str(i), 10.0, now.isoformat())
        )

    db.conn.commit()

    analyzer = FormatPerformanceAnalyzer(db)
    report = analyzer.analyze_format_performance(days=90)

    assert isinstance(report, FormatReport)
    assert len(report.format_stats) == 1

    stat = report.format_stats[0]
    assert stat.format_name == "micro_story"
    assert stat.sample_count == 5
    assert stat.avg_engagement == pytest.approx(10.0)
    assert stat.resonated_rate == pytest.approx(1.0)  # All resonated


def test_compute_selection_weights_insufficient_samples(db):
    """Test that formats with < MIN_SAMPLES get neutral weight."""
    now = datetime.now(timezone.utc)

    # Insert only 2 posts (below MIN_SAMPLES = 3)
    for i in range(2):
        content_id = db.insert_generated_content(
            content_type="x_thread",
            source_commits=["abc"],
            source_messages=["uuid"],
            content=f"Test {i}",
            eval_score=8.0,
            eval_feedback="Test",
            content_format="micro_story"
        )
        db.mark_published(content_id, f"https://x.com/test/{i}", tweet_id=str(i))
        db.conn.execute(
            "UPDATE generated_content SET published_at = ? WHERE id = ?",
            (now.isoformat(), content_id)
        )
        db.conn.execute(
            """INSERT INTO post_engagement
               (content_id, tweet_id, engagement_score, fetched_at)
               VALUES (?, ?, ?, ?)""",
            (content_id, str(i), 10.0, now.isoformat())
        )

    db.conn.commit()

    analyzer = FormatPerformanceAnalyzer(db)
    weights = analyzer.compute_selection_weights(days=90)

    # Should get neutral weight due to insufficient samples
    assert weights["micro_story"] == 1.0


def test_compute_selection_weights_varied_performance(db):
    """Test weight calculation with varied format performance."""
    now = datetime.now(timezone.utc)

    # Create formats with different engagement levels
    formats_engagement = [
        ("micro_story", 15.0, 5),  # Best performance
        ("bold_claim", 10.0, 5),   # Medium performance
        ("question", 5.0, 5),      # Worst performance
    ]

    for format_name, engagement, count in formats_engagement:
        for i in range(count):
            content_id = db.insert_generated_content(
                content_type="x_thread",
                source_commits=["abc"],
                source_messages=["uuid"],
                content=f"Test {format_name} {i}",
                eval_score=8.0,
                eval_feedback="Test",
                content_format=format_name
            )
            db.mark_published(content_id, f"https://x.com/test/{i}", tweet_id=f"{format_name}_{i}")
            db.conn.execute(
                "UPDATE generated_content SET published_at = ? WHERE id = ?",
                (now.isoformat(), content_id)
            )
            db.conn.execute(
                """INSERT INTO post_engagement
                   (content_id, tweet_id, engagement_score, fetched_at)
                   VALUES (?, ?, ?, ?)""",
                (content_id, f"{format_name}_{i}", engagement, now.isoformat())
            )

    db.conn.commit()

    analyzer = FormatPerformanceAnalyzer(db)
    weights = analyzer.compute_selection_weights(days=90)

    # Best format should have highest weight (up to 3.0)
    # Worst format should have lowest weight (1.0)
    assert weights["micro_story"] == pytest.approx(3.0)
    assert weights["question"] == pytest.approx(1.0)
    assert weights["bold_claim"] == pytest.approx(2.0)

    # All weights should be between WEIGHT_FLOOR and 3.0
    for weight in weights.values():
        assert FormatPerformanceAnalyzer.WEIGHT_FLOOR <= weight <= 3.0


def test_compute_selection_weights_floor(db):
    """Test that weights respect the exploration floor."""
    now = datetime.now(timezone.utc)

    # Even with terrible performance, weight should not drop below WEIGHT_FLOOR
    for i in range(5):
        content_id = db.insert_generated_content(
            content_type="x_thread",
            source_commits=["abc"],
            source_messages=["uuid"],
            content=f"Test {i}",
            eval_score=8.0,
            eval_feedback="Test",
            content_format="micro_story"
        )
        db.mark_published(content_id, f"https://x.com/test/{i}", tweet_id=str(i))
        db.conn.execute(
            "UPDATE generated_content SET published_at = ? WHERE id = ?",
            (now.isoformat(), content_id)
        )
        db.conn.execute(
            """INSERT INTO post_engagement
               (content_id, tweet_id, engagement_score, fetched_at)
               VALUES (?, ?, ?, ?)""",
            (content_id, str(i), 0.0, now.isoformat())  # Zero engagement
        )

    db.conn.commit()

    analyzer = FormatPerformanceAnalyzer(db)
    weights = analyzer.compute_selection_weights(days=90)

    # Even with 0 engagement, should respect floor
    assert weights["micro_story"] >= FormatPerformanceAnalyzer.WEIGHT_FLOOR


def test_compute_selection_weights_uniform_performance(db):
    """Test weights when all formats have same performance."""
    now = datetime.now(timezone.utc)

    # All formats with same engagement
    for format_name in ["micro_story", "bold_claim", "question"]:
        for i in range(5):
            content_id = db.insert_generated_content(
                content_type="x_thread",
                source_commits=["abc"],
                source_messages=["uuid"],
                content=f"Test {format_name} {i}",
                eval_score=8.0,
                eval_feedback="Test",
                content_format=format_name
            )
            db.mark_published(content_id, f"https://x.com/test/{i}", tweet_id=f"{format_name}_{i}")
            db.conn.execute(
                "UPDATE generated_content SET published_at = ? WHERE id = ?",
                (now.isoformat(), content_id)
            )
            db.conn.execute(
                """INSERT INTO post_engagement
                   (content_id, tweet_id, engagement_score, fetched_at)
                   VALUES (?, ?, ?, ?)""",
                (content_id, f"{format_name}_{i}", 10.0, now.isoformat())
            )

    db.conn.commit()

    analyzer = FormatPerformanceAnalyzer(db)
    weights = analyzer.compute_selection_weights(days=90)

    # All should have same neutral weight when performance is identical
    for weight in weights.values():
        assert weight == pytest.approx(1.0)


def test_lookback_window(db):
    """Test that lookback window filters old content correctly."""
    now = datetime.now(timezone.utc)
    old_date = now - timedelta(days=100)

    # Insert old content (outside 90-day window)
    old_id = db.insert_generated_content(
        content_type="x_thread",
        source_commits=["abc"],
        source_messages=["uuid"],
        content="Old content",
        eval_score=8.0,
        eval_feedback="Test",
        content_format="micro_story"
    )
    db.mark_published(old_id, "https://x.com/test/old", tweet_id="old")
    db.conn.execute(
        "UPDATE generated_content SET published_at = ? WHERE id = ?",
        (old_date.isoformat(), old_id)
    )

    # Insert recent content
    recent_id = db.insert_generated_content(
        content_type="x_thread",
        source_commits=["def"],
        source_messages=["uuid2"],
        content="Recent content",
        eval_score=8.0,
        eval_feedback="Test",
        content_format="micro_story"
    )
    db.mark_published(recent_id, "https://x.com/test/recent", tweet_id="recent")
    db.conn.execute(
        "UPDATE generated_content SET published_at = ? WHERE id = ?",
        (now.isoformat(), recent_id)
    )

    db.conn.commit()

    # Query with 90-day lookback
    stats = db.get_format_engagement_stats(days=90)

    # Should only include recent content
    assert len(stats) == 1
    assert stats[0]["count"] == 1


def test_null_format_excluded(db):
    """Test that content without format is excluded from stats."""
    now = datetime.now(timezone.utc)

    # Insert content without format
    no_format_id = db.insert_generated_content(
        content_type="x_thread",
        source_commits=["abc"],
        source_messages=["uuid"],
        content="No format",
        eval_score=8.0,
        eval_feedback="Test",
        content_format=None  # No format
    )
    db.mark_published(no_format_id, "https://x.com/test/1", tweet_id="1")
    db.conn.execute(
        "UPDATE generated_content SET published_at = ? WHERE id = ?",
        (now.isoformat(), no_format_id)
    )

    # Insert content with format
    with_format_id = db.insert_generated_content(
        content_type="x_thread",
        source_commits=["def"],
        source_messages=["uuid2"],
        content="With format",
        eval_score=8.0,
        eval_feedback="Test",
        content_format="micro_story"
    )
    db.mark_published(with_format_id, "https://x.com/test/2", tweet_id="2")
    db.conn.execute(
        "UPDATE generated_content SET published_at = ? WHERE id = ?",
        (now.isoformat(), with_format_id)
    )

    db.conn.commit()

    stats = db.get_format_engagement_stats(days=90)

    # Should only include content with format
    assert len(stats) == 1
    assert stats[0]["format"] == "micro_story"
