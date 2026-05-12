"""Tests for publish queue hold reason cluster reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3

from evaluation.publish_queue_hold_reason_clusters import (
    build_publish_queue_hold_reason_clusters_report,
    format_publish_queue_hold_reason_clusters_json,
    format_publish_queue_hold_reason_clusters_text,
    normalize_hold_reason,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def _content(db, content_type: str = "x_post") -> int:
    return int(
        db.conn.execute(
            """INSERT INTO generated_content
               (content, content_type, eval_score, published, created_at)
               VALUES (?, ?, 7.0, 0, ?)""",
            ("Queued copy", content_type, (NOW - timedelta(days=3)).isoformat()),
        ).lastrowid
    )


def _queue(
    db,
    *,
    reason: str | None,
    platform: str = "x",
    content_type: str = "x_post",
    status: str = "held",
    days_ago: int = 1,
) -> int:
    content_id = _content(db, content_type)
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, hold_reason, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            (NOW + timedelta(days=1)).isoformat(),
            platform,
            status,
            reason,
            (NOW - timedelta(days=days_ago)).isoformat(),
        ),
    ).lastrowid
    db.conn.commit()
    return int(queue_id)


def test_clusters_normalized_reasons_by_platform_and_content_type(db):
    first = _queue(
        db,
        reason="Policy hold for URL https://example.com/a and request id=req-123",
        platform="X",
        content_type="x_post",
        days_ago=5,
    )
    second = _queue(
        db,
        reason="policy HOLD for url https://example.com/b and request id=req-999",
        platform="x",
        content_type="x_post",
        days_ago=2,
    )
    _queue(
        db,
        reason="policy hold for URL https://example.com/c and request id=req-777",
        platform="bluesky",
        content_type="x_post",
        days_ago=1,
    )
    _queue(db, reason="Media missing id=abc123", content_type="x_thread")

    report = build_publish_queue_hold_reason_clusters_report(
        db,
        min_cluster_size=2,
        now=NOW,
    )

    assert len(report.clusters) == 1
    cluster = report.clusters[0]
    assert cluster.normalized_reason == "policy hold for url and"
    assert cluster.count == 2
    assert cluster.platforms == ("x",)
    assert cluster.content_types == ("x_post",)
    assert cluster.representative_ids == (first, second)
    assert cluster.oldest_item_id == first
    assert cluster.newest_item_id == second
    assert report.totals["filtered_item_count"] == 2


def test_min_cluster_size_filters_noise_and_formats_json_text(db):
    for index in range(3):
        _queue(db, reason=f"Duplicate content detected content_id={100 + index}")
    _queue(db, reason="One-off manual hold")

    report = build_publish_queue_hold_reason_clusters_report(
        db,
        min_cluster_size=3,
        representative_limit=2,
        now=NOW,
    )
    payload = json.loads(format_publish_queue_hold_reason_clusters_json(report))
    text = format_publish_queue_hold_reason_clusters_text(report)

    assert payload["artifact_type"] == "publish_queue_hold_reason_clusters"
    assert list(payload) == sorted(payload)
    assert payload["clusters"][0]["representative_ids"] == [1, 2]
    assert report.clusters[0].normalized_reason == "duplicate content detected"
    assert "Duplicate content detected" not in report.clusters[0].normalized_reason
    assert "Clusters: 1 covering 3 items" in text


def test_queued_and_held_items_with_empty_reasons_are_ignored(db):
    _queue(db, reason="Token expired job_id=abc123", status="queued")
    _queue(db, reason="token expired job_id=def456", status="held")
    _queue(db, reason="token expired job_id=ghi789", status="failed")
    _queue(db, reason="   ", status="held")

    report = build_publish_queue_hold_reason_clusters_report(
        db,
        min_cluster_size=2,
        now=NOW,
    )

    assert len(report.clusters) == 1
    assert report.clusters[0].count == 2
    assert report.clusters[0].normalized_reason == "token expired"


def test_missing_queue_or_reason_schema_is_reported():
    empty = sqlite3.connect(":memory:")
    empty.row_factory = sqlite3.Row
    try:
        missing_table = build_publish_queue_hold_reason_clusters_report(empty, now=NOW)
    finally:
        empty.close()

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE publish_queue (
            id INTEGER PRIMARY KEY,
            status TEXT,
            platform TEXT,
            created_at TEXT
        )"""
    )
    try:
        missing_reasons = build_publish_queue_hold_reason_clusters_report(conn, now=NOW)
    finally:
        conn.close()

    assert missing_table.missing_tables == ("publish_queue",)
    assert missing_reasons.missing_columns["publish_queue"] == (
        "hold_reason",
        "rejection_reason",
        "error",
        "error_category",
    )


def test_normalize_hold_reason_trims_urls_ids_and_case():
    assert (
        normalize_hold_reason(
            "Blocked by policy at HTTPS://example.com/x?token=secret trace_id=ABCDEF123456"
        )
        == "blocked by policy at"
    )
