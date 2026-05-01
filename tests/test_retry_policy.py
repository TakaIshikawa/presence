"""Tests for publication retry policy planning."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.retry_policy import (  # noqa: E402
    BackoffRule,
    build_retry_policy_plan,
    calculate_next_retry_at,
    format_retry_policy_plan_text,
)
from plan_publish_retries import main  # noqa: E402


BASE_TIME = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)


def _insert_content(db, text: str) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["abc123"],
        source_messages=["uuid-1"],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.commit()
    return content_id


def _failed_publication(
    db,
    content_id: int,
    *,
    platform: str = "x",
    queue_platform: str | None = None,
    category: str = "network",
    error: str = "gateway timeout",
    attempt_count: int = 1,
    attempted_at: datetime | None = None,
    next_retry_at: datetime | None = None,
) -> dict[str, int]:
    attempted = attempted_at or (BASE_TIME - timedelta(minutes=10))
    queue_id = db.queue_for_publishing(
        content_id,
        (BASE_TIME - timedelta(hours=1)).isoformat(),
        platform=queue_platform or platform,
    )
    db.mark_queue_failed(queue_id, error, error_category=category)
    pub_id = db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, error, error_category, attempt_count,
            next_retry_at, last_error_at, updated_at)
           VALUES (?, ?, 'failed', ?, ?, ?, ?, ?, ?)
           ON CONFLICT(content_id, platform) DO UPDATE SET
           status = 'failed',
           error = excluded.error,
           error_category = excluded.error_category,
           attempt_count = excluded.attempt_count,
           next_retry_at = excluded.next_retry_at,
           last_error_at = excluded.last_error_at,
           updated_at = excluded.updated_at""",
        (
            content_id,
            platform,
            error,
            category,
            attempt_count,
            next_retry_at.isoformat() if next_retry_at else None,
            attempted.isoformat(),
            attempted.isoformat(),
        ),
    ).lastrowid
    if pub_id == 0:
        row = db.conn.execute(
            """SELECT id FROM content_publications
               WHERE content_id = ? AND platform = ?""",
            (content_id, platform),
        ).fetchone()
        pub_id = row["id"]
    db.record_publication_attempt(
        queue_id,
        content_id,
        platform,
        False,
        attempted_at=attempted.isoformat(),
        error=error,
        error_category=category,
    )
    db.conn.commit()
    return {"queue_id": queue_id, "publication_id": pub_id}


def _queue_only_failure(
    db,
    content_id: int,
    *,
    platform: str = "x",
    category: str = "network",
    error: str = "timeout",
    attempted_at: datetime | None = None,
) -> int:
    attempted = attempted_at or (BASE_TIME - timedelta(minutes=10))
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, error, error_category)
           VALUES (?, ?, ?, 'failed', ?, ?)""",
        (
            content_id,
            (BASE_TIME - timedelta(hours=2)).isoformat(),
            platform,
            error,
            category,
        ),
    ).lastrowid
    db.record_publication_attempt(
        queue_id,
        content_id,
        platform,
        False,
        attempted_at=attempted.isoformat(),
        error=error,
        error_category=category,
    )
    db.conn.commit()
    return int(queue_id)


def test_backoff_calculation_uses_platform_and_error_rules():
    rules = {
        "default": {
            "network": BackoffRule(base_minutes=10, max_minutes=30),
            "rate_limit": BackoffRule(base_minutes=60, max_minutes=240),
        },
        "bluesky": {
            "rate_limit": BackoffRule(base_minutes=20, max_minutes=40),
        },
    }

    retry_at = calculate_next_retry_at(
        platform="bluesky",
        error_category="rate_limit",
        attempt_count=3,
        failure_at=BASE_TIME - timedelta(minutes=10),
        now=BASE_TIME,
        backoff_rules=rules,
    )

    assert retry_at == BASE_TIME + timedelta(minutes=30)


def test_dry_run_groups_failures_and_does_not_update_rows(db):
    x_content = _insert_content(db, "X network retry")
    bsky_content = _insert_content(db, "Bluesky rate limit")
    ids = _failed_publication(
        db,
        x_content,
        platform="x",
        category="network",
        attempt_count=2,
        attempted_at=BASE_TIME - timedelta(minutes=20),
    )
    _failed_publication(
        db,
        bsky_content,
        platform="bluesky",
        category="rate_limit",
        attempt_count=1,
        attempted_at=BASE_TIME - timedelta(minutes=5),
    )

    plan = build_retry_policy_plan(db, now=BASE_TIME, days=2, apply=False)

    assert plan["applied"] is False
    assert plan["totals"]["failures"] == 2
    assert {
        (group["platform"], group["error_category"], group["attempt_count"])
        for group in plan["groups"]
    } == {("x", "network", 2), ("bluesky", "rate_limit", 1)}
    row = db.conn.execute(
        "SELECT next_retry_at FROM content_publications WHERE id = ?",
        (ids["publication_id"],),
    ).fetchone()
    assert row["next_retry_at"] is None
    assert "Publication retry policy plan" in format_retry_policy_plan_text(plan)


def test_platform_filter_applies_before_grouping(db):
    _failed_publication(
        db,
        _insert_content(db, "X retry"),
        platform="x",
        category="network",
    )
    _failed_publication(
        db,
        _insert_content(db, "Bluesky retry"),
        platform="bluesky",
        category="network",
    )

    plan = build_retry_policy_plan(db, platform="bluesky", now=BASE_TIME)

    assert plan["totals"]["failures"] == 1
    assert plan["groups"][0]["platform"] == "bluesky"
    assert {item["platform"] for item in plan["items"]} == {"bluesky"}


def test_failed_publication_without_attempt_audit_is_planned(db):
    content_id = _insert_content(db, "Legacy failed publication")
    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, error, error_category, attempt_count,
            last_error_at, updated_at)
           VALUES (?, 'x', 'failed', 'temporary network timeout', 'network', 2, ?, ?)""",
        (
            content_id,
            (BASE_TIME - timedelta(minutes=15)).isoformat(),
            (BASE_TIME - timedelta(minutes=15)).isoformat(),
        ),
    )
    db.conn.commit()

    plan = build_retry_policy_plan(db, now=BASE_TIME)

    assert plan["totals"]["failures"] == 1
    assert plan["items"][0]["source"] == "content_publications"
    assert plan["items"][0]["attempt_id"] is None
    assert plan["items"][0]["attempt_count"] == 2


def test_terminal_failure_over_max_attempts_is_reported_without_retry(db):
    _failed_publication(
        db,
        _insert_content(db, "Too many attempts"),
        platform="x",
        category="network",
        attempt_count=3,
    )

    plan = build_retry_policy_plan(db, max_attempts=3, now=BASE_TIME)

    assert plan["totals"]["terminal"] == 1
    item = plan["items"][0]
    assert item["action"] == "terminal"
    assert item["terminal_reason"] == "max_attempts"
    assert item["proposed_next_retry_at"] is None


def test_terminal_non_retryable_error_is_reported_without_retry(db):
    _failed_publication(
        db,
        _insert_content(db, "Auth failure"),
        platform="bluesky",
        category="auth",
        error="401 invalid token",
        attempt_count=1,
    )

    plan = build_retry_policy_plan(db, now=BASE_TIME)

    assert plan["items"][0]["action"] == "terminal"
    assert plan["items"][0]["terminal_reason"] == "non_retryable_error"
    assert plan["items"][0]["proposed_next_retry_at"] is None


def test_apply_updates_publication_next_retry_at(db):
    content_id = _insert_content(db, "Apply retry")
    ids = _failed_publication(
        db,
        content_id,
        platform="x",
        category="network",
        attempt_count=1,
        attempted_at=BASE_TIME - timedelta(minutes=1),
    )

    plan = build_retry_policy_plan(db, now=BASE_TIME, apply=True)

    row = db.conn.execute(
        "SELECT status, next_retry_at FROM content_publications WHERE id = ?",
        (ids["publication_id"],),
    ).fetchone()
    assert row["status"] == "failed"
    assert row["next_retry_at"] == plan["items"][0]["proposed_next_retry_at"]
    assert row["next_retry_at"] == (BASE_TIME + timedelta(minutes=4)).isoformat()
    assert plan["items"][0]["applied"] is True


def test_apply_updates_queue_only_failure_scheduled_at(db):
    queue_id = _queue_only_failure(
        db,
        _insert_content(db, "Queue only retry"),
        platform="x",
        category="network",
        attempted_at=BASE_TIME - timedelta(minutes=1),
    )

    plan = build_retry_policy_plan(db, now=BASE_TIME, apply=True)

    row = db.conn.execute(
        "SELECT status, scheduled_at FROM publish_queue WHERE id = ?",
        (queue_id,),
    ).fetchone()
    assert row["status"] == "failed"
    assert row["scheduled_at"] == plan["items"][0]["proposed_next_retry_at"]


def test_apply_marks_terminal_publication_cancelled(db):
    ids = _failed_publication(
        db,
        _insert_content(db, "Terminal retry"),
        platform="x",
        category="network",
        attempt_count=4,
    )

    plan = build_retry_policy_plan(db, max_attempts=3, now=BASE_TIME, apply=True)

    row = db.conn.execute(
        "SELECT status, next_retry_at FROM content_publications WHERE id = ?",
        (ids["publication_id"],),
    ).fetchone()
    assert plan["items"][0]["action"] == "terminal"
    assert row["status"] == "cancelled"
    assert row["next_retry_at"] is None


def test_cli_emits_json_and_honors_apply_flag(db, capsys):
    _failed_publication(
        db,
        _insert_content(db, "CLI retry"),
        platform="x",
        category="network",
        attempt_count=1,
    )

    fixed_plan = build_retry_policy_plan(db, platform="x", now=BASE_TIME, apply=True)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("plan_publish_retries.script_context", fake_script_context), patch(
        "plan_publish_retries.build_retry_policy_plan",
        return_value=fixed_plan,
    ) as build_plan:
        rc = main(
            [
                "--platform",
                "x",
                "--days",
                "2",
                "--max-attempts",
                "4",
                "--apply",
                "--format",
                "json",
            ]
        )

    output = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert output["applied"] is True
    assert output["platform"] == "x"
    build_plan.assert_called_once()
    assert build_plan.call_args.kwargs["apply"] is True
    assert build_plan.call_args.kwargs["days"] == 2
    assert build_plan.call_args.kwargs["max_attempts"] == 4
