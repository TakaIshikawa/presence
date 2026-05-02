"""Tests for publish queue platform skew reports."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.publish_queue_platform_skew import (
    build_publish_queue_platform_skew_report,
    format_publish_queue_platform_skew_json,
    format_publish_queue_platform_skew_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_queue_platform_skew.py"
spec = importlib.util.spec_from_file_location("publish_queue_platform_skew", SCRIPT_PATH)
publish_queue_platform_skew = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publish_queue_platform_skew)

NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, content_type: str = "x_post") -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published, created_at)
           VALUES (?, ?, 7.0, 0, ?)""",
        ("Queued copy", content_type, (NOW - timedelta(days=2)).isoformat()),
    ).lastrowid


def _queue(
    db,
    *,
    platform: str = "x",
    status: str = "queued",
    content_type: str = "x_post",
    scheduled_at: datetime | None = None,
    created_at: datetime | None = None,
) -> int:
    content_id = _content(db, content_type)
    scheduled_at = scheduled_at or (NOW + timedelta(days=1))
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (
            content_id,
            scheduled_at.isoformat(),
            platform,
            status,
            (created_at or (NOW - timedelta(days=1))).isoformat(),
        ),
    ).lastrowid
    db.conn.commit()
    return queue_id


def test_balanced_queues_do_not_emit_skew_warning(db):
    _queue(db, platform="x", content_type="x_post")
    _queue(db, platform="x", status="held", content_type="x_thread")
    _queue(db, platform="bluesky", content_type="x_post")
    _queue(db, platform="bluesky", status="failed", content_type="x_thread")
    _queue(db, platform="x", status="published")

    report = build_publish_queue_platform_skew_report(
        db,
        days=7,
        skew_threshold=1,
        now=NOW,
    )

    totals = {platform.platform: platform.total for platform in report.platforms}
    assert totals == {"bluesky": 2, "x": 2}
    assert report.warnings == ()
    assert report.totals["raw_open_count"] == 4
    assert report.totals["warning_count"] == 0


def test_skewed_queues_warn_only_when_threshold_is_exceeded(db):
    for index in range(5):
        _queue(
            db,
            platform="x",
            content_type="x_post" if index < 4 else "x_thread",
            scheduled_at=NOW + timedelta(days=index + 1),
            created_at=NOW - timedelta(days=index + 1),
        )
    _queue(db, platform="bluesky", content_type="x_post", created_at=NOW - timedelta(days=2))

    quiet = build_publish_queue_platform_skew_report(
        db,
        days=7,
        skew_threshold=4,
        now=NOW,
    )
    noisy = build_publish_queue_platform_skew_report(
        db,
        days=7,
        skew_threshold=3,
        now=NOW,
    )

    assert quiet.warnings == ()
    assert len(noisy.warnings) == 1
    warning = noisy.warnings[0]
    assert warning.high_platform == "x"
    assert warning.low_platform == "bluesky"
    assert warning.difference == 4
    x_summary = next(platform for platform in noisy.platforms if platform.platform == "x")
    assert x_summary.by_content_type == {"x_post": 4, "x_thread": 1}
    assert x_summary.scheduled_count == 5
    assert x_summary.oldest_queued_age_days == 5.0


def test_all_platform_rows_count_for_each_target_without_creating_skew(db):
    _queue(db, platform="all", content_type="x_post")
    _queue(db, platform="all", content_type="x_thread")
    _queue(db, platform="x", content_type="x_post")

    report = build_publish_queue_platform_skew_report(
        db,
        days=7,
        skew_threshold=1,
        now=NOW,
    )

    summaries = {platform.platform: platform for platform in report.platforms}
    assert summaries["x"].total == 3
    assert summaries["x"].all_platform_count == 2
    assert summaries["bluesky"].total == 2
    assert summaries["bluesky"].all_platform_count == 2
    assert report.totals["raw_open_count"] == 3
    assert report.totals["expanded_open_count"] == 5
    assert report.warnings == ()


def test_empty_queue_formats_deterministically(db):
    report = build_publish_queue_platform_skew_report(db, days=7, now=NOW)
    payload = json.loads(format_publish_queue_platform_skew_json(report))
    text = format_publish_queue_platform_skew_text(report)

    assert report.platforms == ()
    assert report.warnings == ()
    assert payload["artifact_type"] == "publish_queue_platform_skew"
    assert list(payload.keys()) == sorted(payload.keys())
    assert "No open publish queue items found." in text


def test_scheduled_buckets_and_unscheduled_counts_with_compatible_nullable_schema():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE publish_queue (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            scheduled_at TEXT,
            platform TEXT,
            status TEXT,
            created_at TEXT
        )"""
    )
    conn.execute("INSERT INTO generated_content (id, content_type) VALUES (1, 'x_post')")
    conn.execute("INSERT INTO generated_content (id, content_type) VALUES (2, 'x_post')")
    conn.execute("INSERT INTO generated_content (id, content_type) VALUES (3, 'x_thread')")
    conn.execute(
        """INSERT INTO publish_queue
           (id, content_id, scheduled_at, platform, status, created_at)
           VALUES (1, 1, NULL, 'x', 'queued', ?)""",
        ((NOW - timedelta(days=3)).isoformat(),),
    )
    conn.execute(
        """INSERT INTO publish_queue
           (id, content_id, scheduled_at, platform, status, created_at)
           VALUES (2, 2, ?, 'x', 'queued', ?)""",
        ((NOW - timedelta(hours=1)).isoformat(), (NOW - timedelta(days=2)).isoformat()),
    )
    conn.execute(
        """INSERT INTO publish_queue
           (id, content_id, scheduled_at, platform, status, created_at)
           VALUES (3, 3, ?, 'x', 'held', ?)""",
        ((NOW + timedelta(days=30)).isoformat(), (NOW - timedelta(days=1)).isoformat()),
    )
    conn.commit()
    try:
        report = build_publish_queue_platform_skew_report(conn, days=7, now=NOW)
    finally:
        conn.close()

    platform = report.platforms[0]
    assert platform.unscheduled_count == 1
    assert platform.scheduled_count == 2
    assert platform.scheduled_buckets["unscheduled"] == 1
    assert platform.scheduled_buckets["overdue"] == 1
    assert platform.scheduled_buckets["later"] == 1


def test_cli_supports_db_and_json_output(db, file_db, capsys):
    queue_id = _queue(db, platform="x", created_at=NOW - timedelta(days=4))
    _queue(file_db, platform="bluesky", created_at=NOW - timedelta(days=2))

    with patch.object(
        publish_queue_platform_skew,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        publish_queue_platform_skew,
        "build_publish_queue_platform_skew_report",
        wraps=lambda db, **kwargs: build_publish_queue_platform_skew_report(
            db,
            now=NOW,
            **kwargs,
        ),
    ):
        assert publish_queue_platform_skew.main(["--days", "7", "--json"]) == 0
    configured_payload = json.loads(capsys.readouterr().out)

    assert configured_payload["platforms"][0]["oldest_queue_id"] == queue_id

    assert (
        publish_queue_platform_skew.main(
            [
                "--db",
                str(file_db.db_path),
                "--days",
                "7",
                "--skew-threshold",
                "1",
                "--json",
            ]
        )
        == 0
    )
    file_payload = json.loads(capsys.readouterr().out)
    assert file_payload["platforms"][0]["platform"] == "bluesky"
