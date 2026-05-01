"""Tests for advisory publish hold resolution."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.publish_hold_resolver import (
    build_publish_hold_resolution,
    format_publish_hold_resolution_json,
    format_publish_hold_resolution_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "resolve_publish_holds.py"
spec = importlib.util.spec_from_file_location("resolve_publish_holds", SCRIPT_PATH)
resolve_publish_holds = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(resolve_publish_holds)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    text: str = "Held publish queue item content",
    *,
    published: int = 0,
    published_at: str | None = None,
) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET published = ?, published_at = ? WHERE id = ?",
        (published, published_at, content_id),
    )
    db.conn.commit()
    return int(content_id)


def _held_queue(
    db,
    *,
    content_id: int,
    platform: str = "x",
    scheduled_at: str | None = None,
    hold_reason: str | None = "operator hold",
    error: str | None = None,
    error_category: str | None = None,
    created_at: str | None = None,
) -> int:
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, error, error_category,
            hold_reason, created_at)
           VALUES (?, ?, ?, 'held', ?, ?, ?, ?)""",
        (
            content_id,
            scheduled_at or (NOW - timedelta(hours=2)).isoformat(),
            platform,
            error,
            error_category,
            hold_reason,
            created_at or (NOW - timedelta(days=1, hours=3)).isoformat(),
        ),
    ).lastrowid
    db.conn.commit()
    return int(queue_id)


def _publication_failure(
    db,
    *,
    content_id: int,
    platform: str = "x",
    error: str = "temporary network timeout",
    error_category: str = "network",
    attempt_count: int = 1,
) -> int:
    publication_id = db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, error, error_category, attempt_count,
            next_retry_at, last_error_at, updated_at)
           VALUES (?, ?, 'failed', ?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            platform,
            error,
            error_category,
            attempt_count,
            (NOW + timedelta(minutes=30)).isoformat(),
            (NOW - timedelta(minutes=30)).isoformat(),
            (NOW - timedelta(minutes=30)).isoformat(),
        ),
    ).lastrowid
    db.conn.commit()
    return int(publication_id)


def _attempt(
    db,
    *,
    queue_id: int,
    content_id: int,
    platform: str = "x",
    error: str = "timeout",
    error_category: str = "network",
) -> None:
    db.conn.execute(
        """INSERT INTO publication_attempts
           (queue_id, content_id, platform, attempted_at, success, error, error_category)
           VALUES (?, ?, ?, ?, 0, ?, ?)""",
        (
            queue_id,
            content_id,
            platform,
            (NOW - timedelta(minutes=15)).isoformat(),
            error,
            error_category,
        ),
    )
    db.conn.commit()


def test_resolver_classifies_held_items_with_age_preview_and_actions(db):
    release_content = _content(db, "Release this held item now")
    future_content = _content(db, "Future held item")
    retry_content = _content(db, "Retry after transient publish failure")
    duplicate_content = _content(db, "Duplicate looking held item")
    manual_content = _content(db, "Repeated error held item")

    release_queue = _held_queue(db, content_id=release_content)
    future_queue = _held_queue(
        db,
        content_id=future_content,
        scheduled_at=(NOW + timedelta(days=1)).isoformat(),
    )
    retry_queue = _held_queue(db, content_id=retry_content)
    _publication_failure(db, content_id=retry_content, error_category="rate_limit")
    duplicate_queue = _held_queue(
        db,
        content_id=duplicate_content,
        hold_reason="duplicate content already posted",
    )
    manual_queue = _held_queue(db, content_id=manual_content)
    _publication_failure(
        db,
        content_id=manual_content,
        error="timeout three times",
        attempt_count=3,
    )
    _attempt(db, queue_id=manual_queue, content_id=manual_content)

    report = build_publish_hold_resolution(db, days=7, now=NOW)
    by_queue = {item["queue_id"]: item for item in report["items"]}

    assert by_queue[release_queue]["recommendation"] == "release_now"
    assert by_queue[release_queue]["hold_age_label"] == "1d 3h"
    assert by_queue[release_queue]["content_preview"] == "Release this held item now"
    assert by_queue[future_queue]["recommendation"] == "reschedule"
    assert by_queue[retry_queue]["recommendation"] == "retry_after_error"
    assert by_queue[retry_queue]["error_category"] == "rate_limit"
    assert by_queue[duplicate_queue]["recommendation"] == "cancel_duplicate"
    assert by_queue[manual_queue]["recommendation"] == "needs_manual_review"
    assert "repeated_errors" in by_queue[manual_queue]["reasons"]
    assert report["recommendation_counts"] == {
        "release_now": 1,
        "reschedule": 1,
        "cancel_duplicate": 1,
        "needs_manual_review": 1,
        "retry_after_error": 1,
    }


def test_missing_content_requires_manual_review(db):
    queue_id = _held_queue(db, content_id=9999)

    report = build_publish_hold_resolution(db, days=7, now=NOW)

    assert report["items"][0]["queue_id"] == queue_id
    assert report["items"][0]["recommendation"] == "needs_manual_review"
    assert report["items"][0]["primary_reason"] == "missing_content"
    assert report["items"][0]["content_preview"] == "[missing generated_content]"


def test_duplicate_takes_precedence_over_retryable_failure(db):
    content_id = _content(db, "Duplicate and rate limited")
    queue_id = _held_queue(db, content_id=content_id)
    _publication_failure(
        db,
        content_id=content_id,
        error="429 but status is a duplicate",
        error_category="rate_limit",
    )
    _attempt(
        db,
        queue_id=queue_id,
        content_id=content_id,
        error="status is a duplicate",
        error_category="duplicate",
    )

    report = build_publish_hold_resolution(db, days=7, now=NOW)

    assert report["items"][0]["recommendation"] == "cancel_duplicate"
    assert report["items"][0]["error_category"] == "duplicate"


def test_filters_platform_and_recent_window(db):
    x_content = _content(db, "X item")
    bluesky_content = _content(db, "Bluesky item")
    old_content = _content(db, "Old item")
    x_queue = _held_queue(db, content_id=x_content, platform="x")
    _held_queue(db, content_id=bluesky_content, platform="bluesky")
    _held_queue(
        db,
        content_id=old_content,
        platform="x",
        created_at=(NOW - timedelta(days=60)).isoformat(),
        scheduled_at=(NOW - timedelta(days=59)).isoformat(),
    )

    report = build_publish_hold_resolution(db, days=7, platform="x", now=NOW)

    assert [item["queue_id"] for item in report["items"]] == [x_queue]
    assert report["filters"] == {"days": 7, "platform": "x"}


def test_resolver_is_read_only(db):
    content_id = _content(db)
    _held_queue(db, content_id=content_id)
    before = [
        dict(row)
        for row in db.conn.execute("SELECT * FROM publish_queue ORDER BY id").fetchall()
    ]

    build_publish_hold_resolution(db, days=7, now=NOW)

    after = [
        dict(row)
        for row in db.conn.execute("SELECT * FROM publish_queue ORDER BY id").fetchall()
    ]
    assert after == before


def test_formatters_and_cli_emit_deterministic_outputs(db, capsys):
    content_id = _content(db, "CLI held item")
    _held_queue(db, content_id=content_id)
    report = build_publish_hold_resolution(db, days=7, now=NOW)

    assert format_publish_hold_resolution_json(report) == format_publish_hold_resolution_json(
        report
    )
    payload = json.loads(format_publish_hold_resolution_json(report))
    text = format_publish_hold_resolution_text(report)

    assert payload["read_only"] is True
    assert "Publish hold resolver" in text
    assert "RECOMMENDATION" in text
    assert "release_now" in text

    with patch.object(
        resolve_publish_holds,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        resolve_publish_holds,
        "build_publish_hold_resolution",
        wraps=lambda db, **kwargs: build_publish_hold_resolution(db, now=NOW, **kwargs),
    ):
        assert resolve_publish_holds.main(["--days", "7", "--platform", "x", "--json"]) == 0

    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"] == {"days": 7, "platform": "x"}
    assert cli_payload["items"][0]["content_preview"] == "CLI held item"
