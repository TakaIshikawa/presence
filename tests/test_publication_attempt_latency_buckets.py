"""Tests for publication attempt latency bucket reports."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.publication_attempt_latency_buckets import (
    build_publication_attempt_latency_bucket_report,
    format_publication_attempt_latency_buckets_json,
    format_publication_attempt_latency_buckets_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "publication_attempt_latency_buckets.py"
)
spec = importlib.util.spec_from_file_location(
    "publication_attempt_latency_buckets_script", SCRIPT_PATH
)
publication_attempt_latency_buckets_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publication_attempt_latency_buckets_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_content(db, text: str) -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=["abc123"],
        source_messages=["uuid-1"],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )


def _attempt_at(db, content_id: int, platform: str, minutes_ago: int, success: bool) -> int:
    return db.record_publication_attempt(
        None,
        content_id,
        platform,
        success,
        attempted_at=(NOW - timedelta(minutes=minutes_ago)).isoformat(),
        error=None if success else "temporary outage",
    )


def test_report_buckets_success_latency_and_retry_delay_per_platform(db):
    x_content = _insert_content(db, "retry then success")
    direct_content = _insert_content(db, "direct success")
    failed_content = _insert_content(db, "still failing")

    _attempt_at(db, x_content, "x", 130, False)
    _attempt_at(db, x_content, "x", 70, False)
    _attempt_at(db, x_content, "x", 10, True)
    db.upsert_publication_success(
        x_content,
        "x",
        published_at=(NOW - timedelta(minutes=10)).isoformat(),
    )
    _attempt_at(db, direct_content, "bluesky", 5, True)
    _attempt_at(db, failed_content, "x", 30, False)
    _attempt_at(db, failed_content, "x", 20, False)

    report = build_publication_attempt_latency_bucket_report(db, days=1, now=NOW)

    x_stats = report["platforms"]["x"]
    bluesky_stats = report["platforms"]["bluesky"]
    assert x_stats["attempted_content_count"] == 2
    assert x_stats["attempt_count"] == 5
    assert x_stats["successful_content_count"] == 1
    assert x_stats["failed_only_content_count"] == 1
    assert x_stats["failed_attempt_count"] == 4
    assert x_stats["time_to_success_buckets"]["1h-6h"] == 1
    assert x_stats["retry_delay_buckets"]["15m-1h"] == 1
    assert x_stats["failed_age_buckets"]["15m-1h"] == 1
    assert bluesky_stats["successful_content_count"] == 1
    assert bluesky_stats["retry_delay_buckets"]["no_retry"] == 1
    x_item = next(
        item for item in report["successful_items"] if item["content_id"] == x_content
    )
    assert x_item["time_to_success_minutes"] == 120
    assert x_item["retry_delay_minutes"] == 60
    assert report["failed_only_items"][0]["content_id"] == failed_content


def test_report_uses_content_publication_success_when_attempt_log_has_only_failures(db):
    content_id = _insert_content(db, "ledger success after failed attempts")
    _attempt_at(db, content_id, "x", 90, False)
    _attempt_at(db, content_id, "x", 40, False)
    db.upsert_publication_success(
        content_id,
        "x",
        published_at=(NOW - timedelta(minutes=5)).isoformat(),
    )

    report = build_publication_attempt_latency_bucket_report(
        db,
        days=1,
        platform="x",
        now=NOW,
    )

    item = report["successful_items"][0]
    assert item["success_source"] == "content_publications"
    assert item["failed_attempt_count"] == 2
    assert item["time_to_success_bucket"] == "1h-6h"
    assert item["retry_delay_bucket"] == "15m-1h"
    assert report["failed_only_items"] == []


def test_platform_filter_and_day_window_limit_attempt_groups(db):
    recent_x = _insert_content(db, "recent x")
    old_x = _insert_content(db, "old x")
    recent_bluesky = _insert_content(db, "recent bluesky")
    _attempt_at(db, recent_x, "x", 60 * 48, False)
    _attempt_at(db, recent_x, "x", 20, True)
    _attempt_at(db, old_x, "x", 60 * 48, True)
    _attempt_at(db, recent_bluesky, "bluesky", 10, True)

    report = build_publication_attempt_latency_bucket_report(
        db,
        days=1,
        platform="x",
        now=NOW,
    )

    assert set(report["platforms"]) == {"x"}
    assert report["platforms"]["x"]["attempted_content_count"] == 1
    assert report["platforms"]["x"]["attempt_count"] == 2
    assert [item["content_id"] for item in report["successful_items"]] == [recent_x]
    assert report["successful_items"][0]["time_to_success_bucket"] == ">24h"


def test_json_and_text_output_are_stable_for_empty_database(db):
    report = build_publication_attempt_latency_bucket_report(db, days=1, now=NOW)
    payload = json.loads(format_publication_attempt_latency_buckets_json(report))
    text = format_publication_attempt_latency_buckets_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["platforms"]["x"]["attempted_content_count"] == 0
    assert "Publication Attempt Latency Bucket Report" in text
    assert "No publication attempts found." in text


def test_cli_supports_days_platform_and_json_output(db, monkeypatch, capsys):
    content_id = _insert_content(db, "cli x")
    _attempt_at(db, content_id, "x", 25, True)
    monkeypatch.setattr(
        publication_attempt_latency_buckets_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        publication_attempt_latency_buckets_script,
        "build_publication_attempt_latency_bucket_report",
        lambda db, **kwargs: build_publication_attempt_latency_bucket_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = publication_attempt_latency_buckets_script.main(
        ["--days", "1", "--platform", "x", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["window_days"] == 1
    assert payload["platform"] == "x"
    assert set(payload["platforms"]) == {"x"}
    assert payload["platforms"]["x"]["successful_content_count"] == 1
