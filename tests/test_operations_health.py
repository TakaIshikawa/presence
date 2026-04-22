"""Tests for operational health summaries."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from evaluation.operations_health import (
    OperationsHealthThresholds,
    format_operations_health,
    summarize_operations_health,
)
from update_operations_state import build_webhook_payload


NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


def _ts(hours_ago: float = 0) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


def _seed_healthy(conn):
    conn.execute(
        "INSERT INTO poll_state (id, last_poll_time, updated_at) VALUES (1, ?, ?)",
        (_ts(0.1), _ts(0.1)),
    )
    conn.execute(
        "INSERT INTO reply_state (id, last_mention_id, updated_at) VALUES (1, ?, ?)",
        ("mention-1", _ts(0.2)),
    )
    conn.execute(
        "INSERT INTO platform_reply_state (platform, cursor, updated_at) VALUES (?, ?, ?)",
        ("x", "cursor-x", _ts(0.2)),
    )
    conn.execute(
        "INSERT INTO platform_reply_state (platform, cursor, updated_at) VALUES (?, ?, ?)",
        ("bluesky", "cursor-bsky", _ts(0.2)),
    )
    conn.execute(
        """INSERT INTO generated_content
           (content_type, content, published, tweet_id, bluesky_uri, published_at)
           VALUES ('x_post', 'hello', 1, 'tweet-1', 'at://post/1', ?)""",
        (_ts(2),),
    )
    conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, like_count, retweet_count, reply_count,
            quote_count, engagement_score, fetched_at)
           VALUES (1, 'tweet-1', 1, 0, 0, 0, 1.0, ?)""",
        (_ts(1),),
    )
    conn.execute(
        """INSERT INTO bluesky_engagement
           (content_id, bluesky_uri, like_count, repost_count, reply_count,
            quote_count, engagement_score, fetched_at)
           VALUES (1, 'at://post/1', 1, 0, 0, 0, 1.0, ?)""",
        (_ts(1),),
    )
    for i in range(3):
        conn.execute(
            """INSERT INTO pipeline_runs
               (batch_id, content_type, outcome, created_at)
               VALUES (?, 'x_post', 'published', ?)""",
            (f"batch-{i}", _ts(1)),
        )
    conn.commit()


def _summary(conn):
    return summarize_operations_health(
        conn,
        OperationsHealthThresholds(
            max_poll_age_minutes=30,
            max_reply_state_age_hours=6,
            max_platform_reply_state_age_hours=6,
            max_failed_queue_items=0,
            pipeline_window_hours=24,
            min_pipeline_runs_for_rejection_rate=3,
            max_pipeline_rejection_rate=0.5,
            max_engagement_fetch_age_hours=36,
        ),
        now=NOW,
    )


def test_operations_health_healthy(db):
    _seed_healthy(db.conn)

    summary = _summary(db.conn)

    assert summary["status"] == "ok"
    assert summary["warnings"] == []
    assert summary["checks"]["publish_queue"]["failed_count"] == 0
    assert summary["checks"]["pipeline_runs"]["rejection_rate"] == 0
    assert "OPERATIONS HEALTH" in format_operations_health(summary)


def test_operations_health_stale_poll(db):
    _seed_healthy(db.conn)
    db.conn.execute(
        "UPDATE poll_state SET last_poll_time = ?, updated_at = ? WHERE id = 1",
        (_ts(2), _ts(2)),
    )
    db.conn.commit()

    summary = _summary(db.conn)

    assert summary["status"] == "warning"
    assert summary["checks"]["poll_state"]["status"] == "warning"
    assert any("poll_state is stale" in warning for warning in summary["warnings"])


def test_operations_health_failed_queue(db):
    _seed_healthy(db.conn)
    db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, error)
           VALUES (1, ?, 'x', 'failed', 'rate limit')""",
        (_ts(1),),
    )
    db.conn.commit()

    summary = _summary(db.conn)

    assert summary["status"] == "warning"
    assert summary["checks"]["publish_queue"]["failed_count"] == 1
    assert any("publish_queue has 1 failed items" in warning for warning in summary["warnings"])


def test_operations_health_pipeline_rejection_spike(db):
    _seed_healthy(db.conn)
    db.conn.execute("DELETE FROM pipeline_runs")
    for i, outcome in enumerate(
        ["published", "below_threshold", "all_filtered", "below_threshold"]
    ):
        db.conn.execute(
            """INSERT INTO pipeline_runs
               (batch_id, content_type, outcome, created_at)
               VALUES (?, 'x_post', ?, ?)""",
            (f"spike-{i}", outcome, _ts(1)),
        )
    db.conn.commit()

    summary = _summary(db.conn)

    assert summary["status"] == "warning"
    assert summary["checks"]["pipeline_runs"]["rejected_runs"] == 3
    assert summary["checks"]["pipeline_runs"]["rejection_rate"] == 0.75
    assert any("pipeline rejection rate is high" in warning for warning in summary["warnings"])


def test_operations_health_warning_webhook_payload(db):
    _seed_healthy(db.conn)
    db.conn.execute(
        "UPDATE poll_state SET last_poll_time = ?, updated_at = ? WHERE id = 1",
        (_ts(2), _ts(2)),
    )
    db.conn.commit()

    summary = _summary(db.conn)
    payload = build_webhook_payload(
        summary,
        source="operations_health",
        min_level="warning",
    )

    assert payload["source"] == "operations_health"
    assert payload["status"] == "warning"
    assert len(payload["alerts"]) == 1
    assert payload["alerts"][0]["id"] == "poll_state"
    assert payload["alerts"][0]["level"] == "warning"
    assert "poll_state is stale" in payload["alerts"][0]["summary"]
