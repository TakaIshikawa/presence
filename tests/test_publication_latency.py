"""Tests for publication latency SLO reporting."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from evaluation.publication_latency import (  # noqa: E402
    build_publication_latency_report,
    format_publication_latency_text,
)
from publication_latency import main  # noqa: E402


BASE_TIME = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


def _insert_content(db, text: str) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["abc123"],
        source_messages=["uuid-1"],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        ((BASE_TIME - timedelta(hours=3)).isoformat(), content_id),
    )
    db.conn.commit()
    return content_id


def _queue_at(
    db,
    content_id: int,
    *,
    created_minutes_ago: int,
    scheduled_minutes_ago: int,
    platform: str,
) -> int:
    queue_id = db.queue_for_publishing(
        content_id,
        (BASE_TIME - timedelta(minutes=scheduled_minutes_ago)).isoformat(),
        platform=platform,
    )
    db.conn.execute(
        "UPDATE publish_queue SET created_at = ? WHERE id = ?",
        ((BASE_TIME - timedelta(minutes=created_minutes_ago)).isoformat(), queue_id),
    )
    db.conn.commit()
    return queue_id


def test_report_measures_successful_attempt_latency_by_platform(db):
    fast_content = _insert_content(db, "fast x")
    slow_content = _insert_content(db, "slow x")
    fast_queue = _queue_at(
        db,
        fast_content,
        created_minutes_ago=50,
        scheduled_minutes_ago=10,
        platform="x",
    )
    slow_queue = _queue_at(
        db,
        slow_content,
        created_minutes_ago=130,
        scheduled_minutes_ago=70,
        platform="x",
    )
    db.record_publication_attempt(
        fast_queue,
        fast_content,
        "x",
        True,
        attempted_at=(BASE_TIME - timedelta(minutes=5)).isoformat(),
    )
    db.record_publication_attempt(
        slow_queue,
        slow_content,
        "x",
        True,
        attempted_at=(BASE_TIME - timedelta(minutes=10)).isoformat(),
    )

    report = build_publication_latency_report(
        db,
        days=1,
        queued_threshold_minutes=60,
        scheduled_threshold_minutes=30,
        now=BASE_TIME,
    )

    x_stats = report["platforms"]["x"]
    assert x_stats["total"] == 2
    assert x_stats["success_count"] == 2
    assert x_stats["missing_success_count"] == 0
    assert x_stats["queued_p50_minutes"] == 45
    assert x_stats["queued_p90_minutes"] == 120
    assert x_stats["queued_max_minutes"] == 120
    assert x_stats["scheduled_p50_minutes"] == 5
    assert x_stats["scheduled_p90_minutes"] == 60
    assert report["slow_items"][0]["queue_id"] == slow_queue
    assert report["slow_items"][0]["exceeded_thresholds"] == ["queued", "scheduled"]


def test_report_uses_content_publication_success_and_counts_missing(db):
    published_content = _insert_content(db, "ledger success")
    missing_content = _insert_content(db, "missing success")
    published_queue = _queue_at(
        db,
        published_content,
        created_minutes_ago=90,
        scheduled_minutes_ago=30,
        platform="bluesky",
    )
    _queue_at(
        db,
        missing_content,
        created_minutes_ago=40,
        scheduled_minutes_ago=20,
        platform="bluesky",
    )
    db.upsert_publication_success(
        published_content,
        "bluesky",
        published_at=(BASE_TIME - timedelta(minutes=5)).isoformat(),
    )

    report = build_publication_latency_report(
        db,
        days=1,
        platform="bluesky",
        queued_threshold_minutes=60,
        scheduled_threshold_minutes=10,
        now=BASE_TIME,
    )

    stats = report["platforms"]["bluesky"]
    assert stats["total"] == 2
    assert stats["success_count"] == 1
    assert stats["missing_success_count"] == 1
    assert report["missing_success_counts"] == {"bluesky": 1}
    assert report["slow_items"][0]["queue_id"] == published_queue
    assert report["slow_items"][0]["success_source"] == "content_publications"


def test_platform_filter_expands_all_queue_targets(db):
    content_id = _insert_content(db, "all platforms")
    queue_id = _queue_at(
        db,
        content_id,
        created_minutes_ago=30,
        scheduled_minutes_ago=20,
        platform="all",
    )
    db.record_publication_attempt(
        queue_id,
        content_id,
        "x",
        True,
        attempted_at=(BASE_TIME - timedelta(minutes=10)).isoformat(),
    )

    report = build_publication_latency_report(
        db,
        days=1,
        platform="x",
        now=BASE_TIME,
    )

    assert set(report["platforms"]) == {"x"}
    assert report["platforms"]["x"]["total"] == 1
    assert report["platforms"]["x"]["success_count"] == 1
    assert report["missing_success_counts"] == {"x": 0}


def test_text_output_is_stable_for_empty_database():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_publication_latency_report(conn, now=BASE_TIME)
    text = format_publication_latency_text(report)

    assert report["platforms"]["x"]["total"] == 0
    assert report["platforms"]["bluesky"]["total"] == 0
    assert "Publication latency SLO report" in text
    assert "No publish queue items found." in text


def test_cli_supports_json_format_and_threshold_flags(db, capsys):
    content_id = _insert_content(db, "cli slow")
    queue_id = _queue_at(
        db,
        content_id,
        created_minutes_ago=120,
        scheduled_minutes_ago=80,
        platform="x",
    )
    db.record_publication_attempt(
        queue_id,
        content_id,
        "x",
        True,
        attempted_at=(BASE_TIME - timedelta(minutes=5)).isoformat(),
    )
    fixed_report = build_publication_latency_report(
        db,
        days=3,
        platform="x",
        queued_threshold_minutes=30,
        scheduled_threshold_minutes=30,
        now=BASE_TIME,
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("publication_latency.script_context", fake_script_context), patch(
        "publication_latency.build_publication_latency_report",
        return_value=fixed_report,
    ):
        result = main(
            [
                "--days",
                "3",
                "--platform",
                "x",
                "--queued-threshold-minutes",
                "30",
                "--scheduled-threshold-minutes",
                "30",
                "--format",
                "json",
            ]
        )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["platform"] == "x"
    assert payload["thresholds"]["queued_to_published_minutes"] == 30
    assert payload["slow_items"][0]["exceeded_thresholds"] == ["queued", "scheduled"]
