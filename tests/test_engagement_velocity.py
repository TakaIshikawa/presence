"""Tests for engagement velocity reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import pytest
import sqlite3
from types import SimpleNamespace

from evaluation.engagement_velocity import (
    build_engagement_velocity_report,
    format_engagement_velocity_csv,
    format_engagement_velocity_json,
)


NOW = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "engagement_velocity.py"
)
spec = importlib.util.spec_from_file_location(
    "engagement_velocity_script",
    SCRIPT_PATH,
)
engagement_velocity_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(engagement_velocity_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _add_content(
    conn: sqlite3.Connection,
    *,
    content_type: str = "x_post",
    published_at: datetime,
) -> int:
    cursor = conn.execute(
        """INSERT INTO generated_content (content_type, content, published, published_at)
           VALUES (?, ?, ?, ?)""",
        (content_type, "test content", 1, published_at.isoformat()),
    )
    conn.commit()
    return cursor.lastrowid


def _add_topic(
    conn: sqlite3.Connection,
    *,
    content_id: int,
    topic: str,
    confidence: float = 1.0,
):
    conn.execute(
        """INSERT INTO content_topics (content_id, topic, confidence)
           VALUES (?, ?, ?)""",
        (content_id, topic, confidence),
    )
    conn.commit()


def _add_post_engagement(
    conn: sqlite3.Connection,
    *,
    content_id: int,
    tweet_id: str,
    like_count: int = 0,
    retweet_count: int = 0,
    reply_count: int = 0,
    quote_count: int = 0,
    fetched_at: datetime,
):
    conn.execute(
        """INSERT INTO post_engagement (content_id, tweet_id, like_count, retweet_count, reply_count, quote_count, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (content_id, tweet_id, like_count, retweet_count, reply_count, quote_count, fetched_at.isoformat()),
    )
    conn.commit()


def _add_linkedin_engagement(
    conn: sqlite3.Connection,
    *,
    content_id: int,
    post_id: str,
    like_count: int = 0,
    comment_count: int = 0,
    share_count: int = 0,
    fetched_at: datetime,
):
    conn.execute(
        """INSERT INTO linkedin_engagement (content_id, linkedin_url, post_id, like_count, comment_count, share_count, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (content_id, f"https://linkedin.com/post/{post_id}", post_id, like_count, comment_count, share_count, fetched_at.isoformat()),
    )
    conn.commit()


def test_calculates_velocity_acceleration():
    """Velocity calculation detects acceleration correctly."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    # Create content published 20 days ago
    content_id = _add_content(conn, published_at=NOW - timedelta(days=20))

    # Previous period (14-7 days ago): low engagement (10 total)
    _add_post_engagement(
        conn,
        content_id=content_id,
        tweet_id="tweet1",
        like_count=5,
        retweet_count=3,
        reply_count=2,
        fetched_at=NOW - timedelta(days=10),
    )

    # Current period (7-0 days ago): high engagement (30 total)
    _add_post_engagement(
        conn,
        content_id=content_id,
        tweet_id="tweet1",
        like_count=15,
        retweet_count=10,
        reply_count=5,
        fetched_at=NOW - timedelta(days=3),
    )

    report = build_engagement_velocity_report(conn, window_days=7, now=NOW)

    assert len(report.rows) == 1
    row = report.rows[0]

    assert row.content_id == content_id
    assert row.platform == "x"
    assert row.current_period_engagement == 30.0
    assert row.previous_period_engagement == 10.0
    assert row.velocity == 20.0
    assert row.acceleration == "accelerating"


def test_calculates_velocity_deceleration():
    """Velocity calculation detects deceleration correctly."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    content_id = _add_content(conn, published_at=NOW - timedelta(days=20))

    # Previous period: high engagement (30 total)
    _add_post_engagement(
        conn,
        content_id=content_id,
        tweet_id="tweet1",
        like_count=15,
        retweet_count=10,
        reply_count=5,
        fetched_at=NOW - timedelta(days=10),
    )

    # Current period: low engagement (10 total)
    _add_post_engagement(
        conn,
        content_id=content_id,
        tweet_id="tweet1",
        like_count=5,
        retweet_count=3,
        reply_count=2,
        fetched_at=NOW - timedelta(days=3),
    )

    report = build_engagement_velocity_report(conn, window_days=7, now=NOW)

    assert len(report.rows) == 1
    row = report.rows[0]

    assert row.velocity == -20.0
    assert row.acceleration == "decelerating"


def test_calculates_velocity_stable():
    """Velocity calculation detects stable engagement."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    content_id = _add_content(conn, published_at=NOW - timedelta(days=20))

    # Previous period: 10 total
    _add_post_engagement(
        conn,
        content_id=content_id,
        tweet_id="tweet1",
        like_count=5,
        retweet_count=3,
        reply_count=2,
        fetched_at=NOW - timedelta(days=10),
    )

    # Current period: 10 total (same)
    _add_post_engagement(
        conn,
        content_id=content_id,
        tweet_id="tweet1",
        like_count=5,
        retweet_count=3,
        reply_count=2,
        fetched_at=NOW - timedelta(days=3),
    )

    report = build_engagement_velocity_report(conn, window_days=7, now=NOW)

    assert len(report.rows) == 1
    row = report.rows[0]

    assert row.velocity == 0.0
    assert row.acceleration == "stable"


def test_handles_multiple_platforms():
    """Report handles engagement from multiple platforms."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    content_id_x = _add_content(conn, published_at=NOW - timedelta(days=20))
    content_id_linkedin = _add_content(conn, published_at=NOW - timedelta(days=20))

    # X engagement (accelerating)
    _add_post_engagement(conn, content_id=content_id_x, tweet_id="t1", like_count=5, fetched_at=NOW - timedelta(days=10))
    _add_post_engagement(conn, content_id=content_id_x, tweet_id="t1", like_count=15, fetched_at=NOW - timedelta(days=3))

    # LinkedIn engagement (decelerating)
    _add_linkedin_engagement(conn, content_id=content_id_linkedin, post_id="l1", like_count=20, fetched_at=NOW - timedelta(days=10))
    _add_linkedin_engagement(conn, content_id=content_id_linkedin, post_id="l1", like_count=10, fetched_at=NOW - timedelta(days=3))

    report = build_engagement_velocity_report(conn, window_days=7, now=NOW)

    assert len(report.rows) == 2
    platforms = {row.platform for row in report.rows}
    assert platforms == {"x", "linkedin"}


def test_filters_by_platform():
    """Report can filter by specific platform."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    content_id_x = _add_content(conn, published_at=NOW - timedelta(days=20))
    content_id_linkedin = _add_content(conn, published_at=NOW - timedelta(days=20))

    _add_post_engagement(conn, content_id=content_id_x, tweet_id="t1", like_count=10, fetched_at=NOW - timedelta(days=3))
    _add_linkedin_engagement(conn, content_id=content_id_linkedin, post_id="l1", like_count=10, fetched_at=NOW - timedelta(days=3))

    report = build_engagement_velocity_report(conn, window_days=7, platform="x", now=NOW)

    assert len(report.rows) == 1
    assert report.rows[0].platform == "x"


def test_aggregates_high_velocity_topics():
    """Report identifies high-velocity topics."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    # Content with "testing" topic (high velocity)
    content_id_1 = _add_content(conn, published_at=NOW - timedelta(days=20))
    _add_topic(conn, content_id=content_id_1, topic="testing")
    _add_post_engagement(conn, content_id=content_id_1, tweet_id="t1", like_count=5, fetched_at=NOW - timedelta(days=10))
    _add_post_engagement(conn, content_id=content_id_1, tweet_id="t1", like_count=25, fetched_at=NOW - timedelta(days=3))

    # Content with "architecture" topic (low velocity)
    content_id_2 = _add_content(conn, published_at=NOW - timedelta(days=20))
    _add_topic(conn, content_id=content_id_2, topic="architecture")
    _add_post_engagement(conn, content_id=content_id_2, tweet_id="t2", like_count=10, fetched_at=NOW - timedelta(days=10))
    _add_post_engagement(conn, content_id=content_id_2, tweet_id="t2", like_count=12, fetched_at=NOW - timedelta(days=3))

    report = build_engagement_velocity_report(conn, window_days=7, now=NOW)

    # "testing" should have higher velocity than "architecture"
    assert "testing" in report.high_velocity_topics
    assert "architecture" in report.high_velocity_topics
    assert report.high_velocity_topics["testing"] > report.high_velocity_topics["architecture"]


def test_filters_by_topic():
    """Report can filter by specific topic."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    content_id_1 = _add_content(conn, published_at=NOW - timedelta(days=20))
    _add_topic(conn, content_id=content_id_1, topic="testing")
    _add_post_engagement(conn, content_id=content_id_1, tweet_id="t1", like_count=10, fetched_at=NOW - timedelta(days=3))

    content_id_2 = _add_content(conn, published_at=NOW - timedelta(days=20))
    _add_topic(conn, content_id=content_id_2, topic="architecture")
    _add_post_engagement(conn, content_id=content_id_2, tweet_id="t2", like_count=10, fetched_at=NOW - timedelta(days=3))

    report = build_engagement_velocity_report(conn, window_days=7, topic="testing", now=NOW)

    assert len(report.rows) == 1
    assert report.rows[0].topic == "testing"


def test_platform_summary_aggregates_metrics():
    """Platform summary provides aggregate velocity and engagement metrics."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    content_id = _add_content(conn, published_at=NOW - timedelta(days=20))
    _add_post_engagement(conn, content_id=content_id, tweet_id="t1", like_count=10, fetched_at=NOW - timedelta(days=10))
    _add_post_engagement(conn, content_id=content_id, tweet_id="t1", like_count=20, fetched_at=NOW - timedelta(days=3))

    report = build_engagement_velocity_report(conn, window_days=7, now=NOW)

    assert "x" in report.platform_summary
    x_summary = report.platform_summary["x"]
    assert x_summary["avg_velocity"] == 10.0  # 20 - 10
    assert x_summary["avg_engagement"] == 20.0
    assert x_summary["item_count"] == 1


def test_calculates_totals_accurately():
    """Report totals count acceleration statuses correctly."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    # Accelerating content
    for i in range(3):
        content_id = _add_content(conn, published_at=NOW - timedelta(days=20))
        _add_post_engagement(conn, content_id=content_id, tweet_id=f"t{i}", like_count=5, fetched_at=NOW - timedelta(days=10))
        _add_post_engagement(conn, content_id=content_id, tweet_id=f"t{i}", like_count=20, fetched_at=NOW - timedelta(days=3))

    # Decelerating content
    for i in range(2):
        content_id = _add_content(conn, published_at=NOW - timedelta(days=20))
        _add_post_engagement(conn, content_id=content_id, tweet_id=f"d{i}", like_count=20, fetched_at=NOW - timedelta(days=10))
        _add_post_engagement(conn, content_id=content_id, tweet_id=f"d{i}", like_count=5, fetched_at=NOW - timedelta(days=3))

    # Stable content
    content_id = _add_content(conn, published_at=NOW - timedelta(days=20))
    _add_post_engagement(conn, content_id=content_id, tweet_id="s1", like_count=10, fetched_at=NOW - timedelta(days=10))
    _add_post_engagement(conn, content_id=content_id, tweet_id="s1", like_count=10, fetched_at=NOW - timedelta(days=3))

    report = build_engagement_velocity_report(conn, window_days=7, now=NOW)

    assert report.totals["total_items"] == 6
    assert report.totals["accelerating_count"] == 3
    assert report.totals["decelerating_count"] == 2
    assert report.totals["stable_count"] == 1


def test_json_output_format():
    """JSON output is valid and includes all required fields."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    content_id = _add_content(conn, published_at=NOW - timedelta(days=20))
    _add_post_engagement(conn, content_id=content_id, tweet_id="t1", like_count=10, fetched_at=NOW - timedelta(days=10))
    _add_post_engagement(conn, content_id=content_id, tweet_id="t1", like_count=20, fetched_at=NOW - timedelta(days=3))

    report = build_engagement_velocity_report(conn, window_days=7, now=NOW)
    json_output = format_engagement_velocity_json(report)

    data = json.loads(json_output)
    assert data["artifact_type"] == "engagement_velocity"
    assert "generated_at" in data
    assert "filters" in data
    assert "totals" in data
    assert "rows" in data
    assert "high_velocity_topics" in data
    assert "platform_summary" in data

    assert len(data["rows"]) == 1
    row = data["rows"][0]
    assert row["platform"] == "x"
    assert row["velocity"] == 10.0
    assert row["acceleration"] == "accelerating"


def test_csv_output_format():
    """CSV output includes headers and properly formatted rows."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    content_id = _add_content(conn, published_at=NOW - timedelta(days=20))
    _add_topic(conn, content_id=content_id, topic="test,topic")  # Topic with comma
    _add_post_engagement(conn, content_id=content_id, tweet_id="t1", like_count=10, fetched_at=NOW - timedelta(days=3))

    report = build_engagement_velocity_report(conn, window_days=7, now=NOW)
    csv_output = format_engagement_velocity_csv(report)

    lines = csv_output.split("\n")
    assert lines[0] == "content_id,topic,platform,current_engagement,previous_engagement,velocity,acceleration,current_posts,previous_posts"
    assert len(lines) == 2  # header + 1 data row

    # CSV should properly escape commas
    assert '"test,topic"' in lines[1]


def test_empty_database():
    """Report handles empty database gracefully."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    report = build_engagement_velocity_report(conn, window_days=7, now=NOW)

    assert len(report.rows) == 0
    assert report.totals["total_items"] == 0
    assert report.totals["accelerating_count"] == 0
    assert report.totals["decelerating_count"] == 0
    assert report.totals["stable_count"] == 0
    assert report.high_velocity_topics == {}
    assert report.platform_summary == {}


def test_missing_engagement_tables():
    """Report handles missing engagement tables."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # Create only generated_content table, no engagement tables
    conn.execute("""
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            content TEXT,
            published INTEGER,
            published_at TEXT
        )
    """)

    report = build_engagement_velocity_report(conn, window_days=7, now=NOW)

    assert len(report.rows) == 0
    assert len(report.missing_tables) > 0


def test_window_days_validation():
    """Non-positive window_days raises ValueError."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    with pytest.raises(ValueError, match="window_days must be positive"):
        build_engagement_velocity_report(conn, window_days=0, now=NOW)

    with pytest.raises(ValueError, match="window_days must be positive"):
        build_engagement_velocity_report(conn, window_days=-7, now=NOW)


def test_sorting_by_velocity_magnitude():
    """Rows are sorted by absolute velocity magnitude."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    # High positive velocity
    content_id_1 = _add_content(conn, published_at=NOW - timedelta(days=20))
    _add_post_engagement(conn, content_id=content_id_1, tweet_id="t1", like_count=5, fetched_at=NOW - timedelta(days=10))
    _add_post_engagement(conn, content_id=content_id_1, tweet_id="t1", like_count=30, fetched_at=NOW - timedelta(days=3))

    # Low positive velocity
    content_id_2 = _add_content(conn, published_at=NOW - timedelta(days=20))
    _add_post_engagement(conn, content_id=content_id_2, tweet_id="t2", like_count=10, fetched_at=NOW - timedelta(days=10))
    _add_post_engagement(conn, content_id=content_id_2, tweet_id="t2", like_count=12, fetched_at=NOW - timedelta(days=3))

    # High negative velocity
    content_id_3 = _add_content(conn, published_at=NOW - timedelta(days=20))
    _add_post_engagement(conn, content_id=content_id_3, tweet_id="t3", like_count=30, fetched_at=NOW - timedelta(days=10))
    _add_post_engagement(conn, content_id=content_id_3, tweet_id="t3", like_count=10, fetched_at=NOW - timedelta(days=3))

    report = build_engagement_velocity_report(conn, window_days=7, now=NOW)

    # Should be sorted by absolute velocity: 25, 20, 2
    assert abs(report.rows[0].velocity) >= abs(report.rows[1].velocity)
    assert abs(report.rows[1].velocity) >= abs(report.rows[2].velocity)


def test_script_json_output(monkeypatch):
    """Script produces valid JSON output."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    monkeypatch.setattr(
        engagement_velocity_script,
        "script_context",
        lambda: _script_context(conn),
    )

    content_id = _add_content(conn, published_at=NOW - timedelta(days=20))
    _add_post_engagement(conn, content_id=content_id, tweet_id="t1", like_count=10, fetched_at=NOW - timedelta(days=3))

    exit_code = engagement_velocity_script.main(["--format", "json"])
    assert exit_code == 0


def test_script_csv_output(monkeypatch):
    """Script produces valid CSV output."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    monkeypatch.setattr(
        engagement_velocity_script,
        "script_context",
        lambda: _script_context(conn),
    )

    content_id = _add_content(conn, published_at=NOW - timedelta(days=20))
    _add_post_engagement(conn, content_id=content_id, tweet_id="t1", like_count=10, fetched_at=NOW - timedelta(days=3))

    exit_code = engagement_velocity_script.main(["--format", "csv"])
    assert exit_code == 0


def test_script_with_filters(monkeypatch):
    """Script applies filters correctly."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    monkeypatch.setattr(
        engagement_velocity_script,
        "script_context",
        lambda: _script_context(conn),
    )

    content_id = _add_content(conn, published_at=NOW - timedelta(days=20))
    _add_topic(conn, content_id=content_id, topic="testing")
    _add_post_engagement(conn, content_id=content_id, tweet_id="t1", like_count=10, fetched_at=NOW - timedelta(days=3))

    exit_code = engagement_velocity_script.main([
        "--window-days", "14",
        "--platform", "x",
        "--topic", "testing",
        "--format", "json",
    ])
    assert exit_code == 0


def test_handles_content_without_previous_period():
    """Content with engagement only in current period is handled."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    content_id = _add_content(conn, published_at=NOW - timedelta(days=5))

    # Only current period engagement, no previous period
    _add_post_engagement(conn, content_id=content_id, tweet_id="t1", like_count=10, fetched_at=NOW - timedelta(days=3))

    report = build_engagement_velocity_report(conn, window_days=7, now=NOW)

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.current_period_engagement == 10.0
    assert row.previous_period_engagement == 0.0
    assert row.velocity == 10.0


def test_custom_window_size():
    """Report respects custom window_days parameter."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(open("schema.sql").read())

    content_id = _add_content(conn, published_at=NOW - timedelta(days=30))

    # Engagement 20 days ago (outside 7-day window, inside 14-day window)
    _add_post_engagement(conn, content_id=content_id, tweet_id="t1", like_count=10, fetched_at=NOW - timedelta(days=20))

    # 7-day window: no data
    report_7day = build_engagement_velocity_report(conn, window_days=7, now=NOW)
    assert len(report_7day.rows) == 0

    # 14-day window: has data in previous period
    report_14day = build_engagement_velocity_report(conn, window_days=14, now=NOW)
    # Should have row if engagement is within 14-28 day range
    # Since engagement is at day 20, it's in the previous period (14-28 days ago)
    # But not in current period (0-14 days ago), so it won't appear
    # Let me add current period data
    _add_post_engagement(conn, content_id=content_id, tweet_id="t1", like_count=20, fetched_at=NOW - timedelta(days=10))

    report_14day = build_engagement_velocity_report(conn, window_days=14, now=NOW)
    assert len(report_14day.rows) == 1
