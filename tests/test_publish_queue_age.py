"""Tests for publish queue aging reports."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from output.publish_queue_age import age_bucket, build_publish_queue_age_report
from publish_queue_age import format_age_report, main


BASE_TIME = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


def _content(db, content_type: str = "x_post") -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, ?, ?, ?)""",
        ("Reviewable queued copy", content_type, 7.0, 0),
    ).lastrowid


def _queue(
    db,
    *,
    scheduled_at: datetime,
    platform: str = "x",
    status: str = "queued",
    content_type: str = "x_post",
) -> int:
    content_id = _content(db, content_type=content_type)
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, error, error_category)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            scheduled_at.isoformat(),
            platform,
            status,
            "rate limit" if status == "failed" else None,
            "rate_limit" if status == "failed" else None,
        ),
    ).lastrowid
    if platform in {"x", "all"}:
        db.conn.execute(
            """INSERT INTO content_publications
               (content_id, platform, status, attempt_count, next_retry_at, error_category)
               VALUES (?, 'x', ?, ?, ?, ?)""",
            (
                content_id,
                "failed" if status == "failed" else "queued",
                2 if status == "failed" else 0,
                (BASE_TIME + timedelta(hours=1)).isoformat() if status == "failed" else None,
                "rate_limit" if status == "failed" else None,
            ),
        )
    db.conn.commit()
    return queue_id


def test_age_bucket_boundaries():
    assert age_bucket(-2) == "future"
    assert age_bucket(0) == "0-1h"
    assert age_bucket(5.9) == "1-6h"
    assert age_bucket(24) == "24-72h"
    assert age_bucket(72) == "72h+"


def test_report_groups_counts_and_stale_items_by_platform(db):
    old_x = _queue(db, scheduled_at=BASE_TIME - timedelta(hours=30), platform="x")
    failed_bsky = _queue(
        db,
        scheduled_at=BASE_TIME - timedelta(hours=8),
        platform="bluesky",
        status="failed",
        content_type="x_thread",
    )
    future_x = _queue(db, scheduled_at=BASE_TIME + timedelta(hours=3), platform="x")
    _queue(db, scheduled_at=BASE_TIME - timedelta(hours=100), platform="x", status="published")

    report = build_publish_queue_age_report(
        db,
        stale_after_hours=12,
        now=BASE_TIME,
    )

    assert report["total"] == 3
    assert report["platforms"]["x"]["total"] == 2
    assert report["platforms"]["x"]["statuses"] == {"queued": 2, "failed": 0}
    assert report["platforms"]["x"]["age_buckets"]["24-72h"] == 1
    assert report["platforms"]["x"]["age_buckets"]["future"] == 1
    assert report["platforms"]["bluesky"]["total"] == 1
    assert report["platforms"]["bluesky"]["statuses"] == {"queued": 0, "failed": 1}
    assert report["oldest_item"]["queue_id"] == old_x

    stale_ids = {item["queue_id"] for item in report["stale_items"]}
    assert stale_ids == {old_x}
    stale = report["stale_items"][0]
    assert stale["content_type"] == "x_post"
    assert stale["retry_state"] == {"x": {"attempt_count": 0, "error": None, "error_category": None, "last_error_at": None, "next_retry_at": None, "status": "queued"}}

    failed_item = report["platforms"]["bluesky"]["oldest_item"]
    assert failed_item["queue_id"] == failed_bsky
    assert failed_item["age_hours"] == 8
    assert future_x not in stale_ids


def test_future_items_count_but_do_not_become_stale(db):
    future_id = _queue(db, scheduled_at=BASE_TIME + timedelta(hours=48), platform="all")

    report = build_publish_queue_age_report(
        db,
        stale_after_hours=1,
        now=BASE_TIME,
    )

    assert report["total"] == 1
    assert report["platforms"]["all"]["age_buckets"]["future"] == 1
    assert report["platforms"]["all"]["oldest_item"]["queue_id"] == future_id
    assert report["platforms"]["all"]["oldest_item"]["age_hours"] == -48
    assert report["stale_items"] == []


def test_report_filters_by_platform(db):
    _queue(db, scheduled_at=BASE_TIME - timedelta(hours=3), platform="x")
    bsky_id = _queue(db, scheduled_at=BASE_TIME - timedelta(hours=4), platform="bluesky")

    report = build_publish_queue_age_report(
        db,
        platform="bluesky",
        stale_after_hours=1,
        now=BASE_TIME,
    )

    assert set(report["platforms"]) == {"bluesky"}
    assert [item["queue_id"] for item in report["stale_items"]] == [bsky_id]


def test_cli_supports_json_output(db, capsys):
    queue_id = _queue(db, scheduled_at=BASE_TIME - timedelta(hours=2), platform="x")

    @contextmanager
    def fake_context():
        yield None, db

    with patch("publish_queue_age.script_context", fake_context), patch(
        "publish_queue_age.build_publish_queue_age_report",
        wraps=lambda db, **kwargs: build_publish_queue_age_report(db, now=BASE_TIME, **kwargs),
    ):
        assert main(["--stale-after-hours", "1", "--json"]) == 0

    output = json.loads(capsys.readouterr().out)
    assert output["stale_items"][0]["queue_id"] == queue_id
    assert output["platforms"]["x"]["age_buckets"]["1-6h"] == 1


def test_human_output_lists_stale_items(db):
    queue_id = _queue(db, scheduled_at=BASE_TIME - timedelta(hours=2), platform="x")
    report = build_publish_queue_age_report(
        db,
        stale_after_hours=1,
        now=BASE_TIME,
    )

    output = format_age_report(report)

    assert "Publish queue age report" in output
    assert "Stale items:" in output
    assert str(queue_id) in output
