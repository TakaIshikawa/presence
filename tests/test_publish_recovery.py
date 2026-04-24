"""Tests for publish recovery recommendations."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publish_recovery import get_publish_recovery_recommendations
from publish_recovery import format_json_recommendations, main


BASE_TIME = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


def _insert_content(db, content: str, *, published: int = 0) -> int:
    content_id = db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, 'x_post', 7.0, ?)""",
        (content, published),
    ).lastrowid
    db.conn.commit()
    return content_id


def _queue_item(
    db,
    content_id: int,
    *,
    platform: str = "x",
    status: str = "queued",
    scheduled_at: datetime | None = None,
    error: str | None = None,
    error_category: str | None = None,
) -> int:
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, error, error_category)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            (scheduled_at or BASE_TIME).isoformat(),
            platform,
            status,
            error,
            error_category,
        ),
    ).lastrowid
    db.conn.commit()
    return queue_id


def _publication_failure(
    db,
    content_id: int,
    *,
    platform: str = "x",
    category: str | None = None,
    error: str = "temporary network timeout",
    attempt_count: int = 1,
    next_retry_at: datetime | None = None,
) -> int:
    pub_id = db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, error, error_category, attempt_count,
            next_retry_at, last_error_at, updated_at)
           VALUES (?, ?, 'failed', ?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            platform,
            error,
            category,
            attempt_count,
            next_retry_at.isoformat() if next_retry_at else None,
            BASE_TIME.isoformat(),
            BASE_TIME.isoformat(),
        ),
    ).lastrowid
    db.conn.commit()
    return pub_id


def _actions(groups: list[dict]) -> dict[tuple[str, str], str]:
    return {
        (group["platform"], group["error_category"]): group["action"]
        for group in groups
    }


def test_recommendations_are_deterministic_for_known_categories(db):
    categories = {
        "auth": "401 invalid token",
        "rate_limit": "429 too many requests",
        "duplicate": "status is a duplicate",
        "media": "unsupported file size",
        "network": "gateway timeout",
        "unknown": "mystery failure",
    }
    for index, (category, error) in enumerate(categories.items(), start=1):
        content_id = _insert_content(db, f"{category} failure")
        _queue_item(db, content_id, platform="x", status="failed")
        _publication_failure(
            db,
            content_id,
            platform="x",
            category=category,
            error=error,
            attempt_count=1,
            next_retry_at=BASE_TIME - timedelta(minutes=index),
        )

    groups = get_publish_recovery_recommendations(
        db.conn,
        now=BASE_TIME,
        limit=20,
    )

    assert _actions(groups) == {
        ("x", "auth"): "refresh_auth",
        ("x", "rate_limit"): "retry_now",
        ("x", "duplicate"): "edit_duplicate",
        ("x", "media"): "inspect_media",
        ("x", "network"): "retry_now",
        ("x", "unknown"): "retry_now",
    }
    assert json.loads(format_json_recommendations(groups))


def test_recommendations_wait_for_future_retry_time(db):
    content_id = _insert_content(db, "Rate limited post")
    _queue_item(db, content_id, platform="x", status="failed")
    _publication_failure(
        db,
        content_id,
        platform="x",
        category="rate_limit",
        error="429 rate limit",
        attempt_count=1,
        next_retry_at=BASE_TIME + timedelta(minutes=15),
    )

    groups = get_publish_recovery_recommendations(db.conn, now=BASE_TIME)

    assert groups[0]["action"] == "wait_for_backoff"
    assert groups[0]["next_retry_at"] == (BASE_TIME + timedelta(minutes=15)).isoformat()


def test_recommendations_cancel_after_max_attempts(db):
    content_id = _insert_content(db, "Repeated network failure")
    _queue_item(db, content_id, platform="bluesky", status="failed")
    _publication_failure(
        db,
        content_id,
        platform="bluesky",
        category="network",
        error="network timeout",
        attempt_count=3,
        next_retry_at=BASE_TIME - timedelta(minutes=1),
    )

    groups = get_publish_recovery_recommendations(
        db.conn,
        now=BASE_TIME,
        max_attempts=3,
    )

    assert groups[0]["action"] == "cancel"
    assert groups[0]["attempt_count"] == 3


def test_held_queue_items_are_represented(db):
    content_id = _insert_content(db, "Held campaign post")
    queue_id = _queue_item(db, content_id, platform="x", status="held")
    db.conn.execute(
        "UPDATE publish_queue SET hold_reason = ? WHERE id = ?",
        ("campaign paused", queue_id),
    )
    db.conn.commit()

    groups = get_publish_recovery_recommendations(
        db.conn,
        status="held",
        now=BASE_TIME,
    )

    assert len(groups) == 1
    assert groups[0]["action"] == "cancel"
    assert groups[0]["items"][0]["queue_id"] == queue_id
    assert groups[0]["items"][0]["hold_reason"] == "campaign paused"


def test_queue_and_publication_rows_merge_without_duplication(db):
    content_id = _insert_content(db, "Failure represented twice")
    queue_id = _queue_item(
        db,
        content_id,
        platform="all",
        status="failed",
        error="X: gateway timeout",
        error_category="network",
    )
    pub_id = _publication_failure(
        db,
        content_id,
        platform="x",
        category="network",
        error="gateway timeout",
        attempt_count=2,
        next_retry_at=BASE_TIME - timedelta(minutes=1),
    )

    groups = get_publish_recovery_recommendations(
        db.conn,
        platform="x",
        now=BASE_TIME,
    )

    assert len(groups) == 1
    assert groups[0]["count"] == 1
    assert groups[0]["items"][0]["queue_id"] == queue_id
    assert groups[0]["items"][0]["publication_id"] == pub_id
    assert groups[0]["items"][0]["attempt_count"] == 2


def test_unknown_category_falls_back_to_unknown(db):
    content_id = _insert_content(db, "Unclassified failure")
    _queue_item(
        db,
        content_id,
        platform="x",
        status="failed",
        error="something odd happened",
        error_category="not-real",
    )

    groups = get_publish_recovery_recommendations(db.conn, now=BASE_TIME)

    assert groups[0]["error_category"] == "unknown"
    assert groups[0]["action"] == "retry_now"


def test_already_published_rows_are_excluded(db):
    published_id = _insert_content(db, "Already published", published=1)
    db.conn.execute(
        "UPDATE generated_content SET tweet_id = ?, published_at = ? WHERE id = ?",
        ("tw-ok", BASE_TIME.isoformat(), published_id),
    )
    _queue_item(db, published_id, platform="x", status="failed", error="stale")
    _publication_failure(
        db,
        published_id,
        platform="x",
        category="network",
        error="stale network error",
        attempt_count=1,
    )

    groups = get_publish_recovery_recommendations(db.conn, now=BASE_TIME)

    assert groups == []


def test_cli_filters_platform_and_emits_stable_json(db, capsys):
    x_content = _insert_content(db, "X needs retry")
    bsky_content = _insert_content(db, "Bluesky needs retry")
    _queue_item(db, x_content, platform="x", status="failed")
    _publication_failure(
        db,
        x_content,
        platform="x",
        category="network",
        error="network timeout",
        attempt_count=1,
        next_retry_at=BASE_TIME - timedelta(minutes=1),
    )
    _queue_item(db, bsky_content, platform="bluesky", status="failed")
    _publication_failure(
        db,
        bsky_content,
        platform="bluesky",
        category="auth",
        error="invalid token",
        attempt_count=1,
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("publish_recovery.script_context", fake_script_context):
        rc = main(["--platform", "bluesky", "--status", "failed", "--json"])

    output = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert len(output) == 1
    assert output[0]["platform"] == "bluesky"
    assert output[0]["action"] == "refresh_auth"
    assert list(output[0].keys()) == sorted(output[0].keys())
