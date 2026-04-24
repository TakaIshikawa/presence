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


def _summary(
    conn,
    max_model_cost_24h: float = 5.0,
    max_single_run_model_cost: float = 1.0,
):
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
            max_newsletter_weekly_unsubscribes=5,
            max_newsletter_churn_rate=0.05,
            max_api_rate_limit_snapshot_age_hours=24,
            api_rate_limit_min_remaining={
                "x": 10,
                "bluesky": 10,
                "anthropic": 5,
                "github": 10,
            },
            max_model_cost_24h=max_model_cost_24h,
            max_single_run_model_cost=max_single_run_model_cost,
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
    assert summary["checks"]["model_usage"]["status"] == "ok"
    assert summary["checks"]["model_usage"]["total_cost_24h"] == 0
    assert summary["checks"]["model_usage"]["max_run_cost"] == 0
    assert summary["checks"]["model_usage"]["top_operations"] == []
    assert summary["checks"]["newsletter_audience"]["status"] == "ok"
    assert summary["checks"]["newsletter_audience"]["latest_fetched_at"] is None
    assert summary["checks"]["api_rate_limits"]["status"] == "ok"
    assert "OPERATIONS HEALTH" in format_operations_health(summary)


def test_operations_health_model_usage_ok(db):
    _seed_healthy(db.conn)
    run_id = db.conn.execute(
        "SELECT id FROM pipeline_runs WHERE batch_id = 'batch-0'"
    ).fetchone()[0]
    db.conn.execute(
        """INSERT INTO model_usage
           (model_name, operation_name, input_tokens, output_tokens, total_tokens,
            estimated_cost, pipeline_run_id, created_at)
           VALUES ('claude-sonnet', 'generate', 1000, 500, 1500, 0.35, ?, ?)""",
        (run_id, _ts(0.5)),
    )
    db.conn.execute(
        """INSERT INTO model_usage
           (model_name, operation_name, input_tokens, output_tokens, total_tokens,
            estimated_cost, pipeline_run_id, created_at)
           VALUES ('claude-opus', 'evaluate', 1000, 500, 1500, 0.25, ?, ?)""",
        (run_id, _ts(0.4)),
    )
    db.conn.commit()

    summary = _summary(db.conn)

    model_usage = summary["checks"]["model_usage"]
    assert summary["status"] == "ok"
    assert model_usage["status"] == "ok"
    assert model_usage["total_cost_24h"] == 0.6
    assert model_usage["max_run_cost"] == 0.6
    assert model_usage["max_run"]["batch_id"] == "batch-0"
    assert model_usage["top_operations"] == [
        {"operation_name": "generate", "estimated_cost": 0.35, "usage_events": 1},
        {"operation_name": "evaluate", "estimated_cost": 0.25, "usage_events": 1},
    ]
    formatted = format_operations_health(summary)
    assert "Model usage: ok" in formatted
    assert "24h cost: $0.6000" in formatted
    assert "Max run cost: $0.6000 (batch-0)" in formatted


def test_operations_health_model_usage_warning(db):
    _seed_healthy(db.conn)
    run_id = db.conn.execute(
        "SELECT id FROM pipeline_runs WHERE batch_id = 'batch-0'"
    ).fetchone()[0]
    db.conn.execute(
        """INSERT INTO model_usage
           (model_name, operation_name, input_tokens, output_tokens, total_tokens,
            estimated_cost, pipeline_run_id, created_at)
           VALUES ('claude-opus', 'generate', 1000, 500, 1500, 1.40, ?, ?)""",
        (run_id, _ts(0.5)),
    )
    db.conn.execute(
        """INSERT INTO model_usage
           (model_name, operation_name, input_tokens, output_tokens, total_tokens,
            estimated_cost, created_at)
           VALUES ('claude-sonnet', 'refine', 1000, 500, 1500, 0.80, ?)""",
        (_ts(0.4),),
    )
    db.conn.commit()

    summary = _summary(
        db.conn,
        max_model_cost_24h=2.0,
        max_single_run_model_cost=1.0,
    )

    model_usage = summary["checks"]["model_usage"]
    assert summary["status"] == "warning"
    assert model_usage["status"] == "warning"
    assert model_usage["total_cost_24h"] == 2.2
    assert model_usage["max_run_cost"] == 1.4
    assert any("model usage cost is high" in warning for warning in summary["warnings"])
    assert any(
        "single pipeline run model cost is high" in warning
        for warning in summary["warnings"]
    )
    formatted = format_operations_health(summary)
    assert "Model usage: warning" in formatted
    assert "24h cost: $2.2000" in formatted


def test_operations_health_model_usage_ignores_old_usage(db):
    _seed_healthy(db.conn)
    run_id = db.conn.execute(
        "SELECT id FROM pipeline_runs WHERE batch_id = 'batch-0'"
    ).fetchone()[0]
    db.conn.execute(
        """INSERT INTO model_usage
           (model_name, operation_name, input_tokens, output_tokens, total_tokens,
            estimated_cost, pipeline_run_id, created_at)
           VALUES ('claude-opus', 'generate', 1000, 500, 1500, 10.0, ?, ?)""",
        (run_id, _ts(25)),
    )
    db.conn.commit()

    summary = _summary(db.conn)

    model_usage = summary["checks"]["model_usage"]
    assert model_usage["status"] == "ok"
    assert model_usage["total_cost_24h"] == 0
    assert model_usage["max_run_cost"] == 0
    assert model_usage["top_operations"] == []


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


def test_operations_health_newsletter_audience_risk(db):
    _seed_healthy(db.conn)
    db.conn.execute(
        """INSERT INTO newsletter_subscriber_metrics
           (subscriber_count, active_subscriber_count, unsubscribes, churn_rate,
            new_subscribers, net_subscriber_change, fetched_at)
           VALUES (100, 80, 6, 0.06, 1, -5, ?)""",
        (_ts(0.5),),
    )
    db.conn.commit()

    summary = _summary(db.conn)

    newsletter = summary["checks"]["newsletter_audience"]
    assert summary["status"] == "warning"
    assert newsletter["status"] == "warning"
    assert newsletter["latest_fetched_at"] == _ts(0.5)
    assert newsletter["weekly_unsubscribes"] == 6
    assert newsletter["churn_rate"] == 0.06
    assert any(
        "newsletter weekly unsubscribes are high" in warning
        for warning in summary["warnings"]
    )
    assert any(
        "newsletter churn rate is high" in warning
        for warning in summary["warnings"]
    )
    formatted = format_operations_health(summary)
    assert "Newsletter audience: warning" in formatted
    assert "Weekly unsubscribes: 6" in formatted
    assert "Churn rate: 6.00%" in formatted


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
    assert payload["generated_at"] == summary["generated_at"]
    assert payload["warning_count"] == 1
    assert len(payload["warnings"]) == 1
    assert len(payload["alerts"]) == 1
    assert payload["alerts"][0]["id"] == "poll_state"
    assert payload["alerts"][0]["level"] == "warning"
    assert "poll_state is stale" in payload["alerts"][0]["summary"]


def test_operations_health_warns_on_low_api_rate_limit_snapshot(db):
    _seed_healthy(db.conn)
    db.insert_api_rate_limit_snapshot(
        "x",
        endpoint="GET /2/tweets",
        remaining=8,
        limit=100,
        fetched_at=_ts(1),
    )

    summary = _summary(db.conn)

    api_limits = summary["checks"]["api_rate_limits"]
    assert summary["status"] == "warning"
    assert api_limits["status"] == "warning"
    assert api_limits["snapshots"]["x:GET /2/tweets"]["remaining"] == 8
    assert any("x API rate limit for GET /2/tweets is low" in warning for warning in summary["warnings"])
    assert "API rate limits: warning" in format_operations_health(summary)


def test_operations_health_warns_on_stale_api_rate_limit_snapshot(db):
    _seed_healthy(db.conn)
    db.insert_api_rate_limit_snapshot(
        "github",
        endpoint="/user/repos",
        remaining=4000,
        limit=5000,
        fetched_at=_ts(30),
    )

    summary = _summary(db.conn)

    assert summary["checks"]["api_rate_limits"]["status"] == "warning"
    assert any(
        "github API rate limit snapshot for /user/repos is stale" in warning
        for warning in summary["warnings"]
    )
