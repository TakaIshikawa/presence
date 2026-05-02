"""Tests for publication error burst detection."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.publication_error_bursts import (
    build_publication_error_burst_report,
    format_publication_error_burst_json,
    format_publication_error_burst_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_error_bursts.py"
spec = importlib.util.spec_from_file_location("publication_error_bursts_script", SCRIPT_PATH)
publication_error_bursts_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publication_error_bursts_script)


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


def _attempt_at(
    db,
    content_id: int,
    platform: str,
    minutes_ago: int,
    success: bool,
    *,
    error_category: str | None = None,
) -> int:
    return db.record_publication_attempt(
        None,
        content_id,
        platform,
        success,
        attempted_at=(NOW - timedelta(minutes=minutes_ago)).isoformat(),
        error=None if success else "temporary publish failure",
        error_category=error_category,
    )


def test_groups_failures_by_platform_and_normalized_error_category(db):
    x_first = _insert_content(db, "x network first")
    x_second = _insert_content(db, "x network second")
    unknown = _insert_content(db, "unknown category")
    bsky = _insert_content(db, "bluesky network")

    _attempt_at(db, x_first, "x", 55, False, error_category="network")
    _attempt_at(db, x_second, "x", 45, False, error_category="network")
    _attempt_at(db, unknown, "x", 35, False, error_category="not-real")
    _attempt_at(db, bsky, "bluesky", 25, False, error_category="network")

    report = build_publication_error_burst_report(
        db,
        hours=1,
        min_failures=2,
        min_consecutive=9,
        now=NOW,
    )

    assert [(burst["platform"], burst["error_category"]) for burst in report["bursts"]] == [
        ("x", "network"),
    ]
    burst = report["bursts"][0]
    assert burst["count"] == 2
    assert burst["first_seen"] == (NOW - timedelta(minutes=55)).isoformat()
    assert burst["last_seen"] == (NOW - timedelta(minutes=45)).isoformat()
    assert report["totals"]["by_error_category"] == {"network": 3, "unknown": 1}
    assert report["totals"]["by_platform"] == {"bluesky": 1, "x": 3}


def test_consecutive_failure_threshold_can_trigger_without_total_count(db):
    first = _insert_content(db, "first")
    second = _insert_content(db, "second")

    _attempt_at(db, first, "x", 20, False, error_category="auth")
    _attempt_at(db, second, "x", 10, False, error_category="auth")

    report = build_publication_error_burst_report(
        db,
        hours=1,
        min_failures=10,
        min_consecutive=2,
        now=NOW,
    )

    burst = report["bursts"][0]
    assert burst["count"] == 2
    assert burst["max_consecutive_failures"] == 2
    assert burst["thresholds_exceeded"] == ["consecutive_failures"]


def test_success_attempt_resets_consecutive_failure_streak(db):
    first = _insert_content(db, "first")
    success = _insert_content(db, "success")
    second = _insert_content(db, "second")
    third = _insert_content(db, "third")

    _attempt_at(db, first, "x", 50, False, error_category="network")
    _attempt_at(db, success, "x", 40, True)
    _attempt_at(db, second, "x", 30, False, error_category="network")
    _attempt_at(db, third, "x", 20, False, error_category="network")

    report = build_publication_error_burst_report(
        db,
        hours=1,
        min_failures=9,
        min_consecutive=3,
        now=NOW,
    )

    assert report["bursts"] == []
    report = build_publication_error_burst_report(
        db,
        hours=1,
        min_failures=9,
        min_consecutive=2,
        now=NOW,
    )
    assert report["bursts"][0]["max_consecutive_failures"] == 2
    assert report["bursts"][0]["consecutive_first_seen"] == (
        NOW - timedelta(minutes=30)
    ).isoformat()


def test_platform_filter_limits_attempts_and_bursts(db):
    x_content = _insert_content(db, "x")
    bsky_first = _insert_content(db, "bsky first")
    bsky_second = _insert_content(db, "bsky second")

    _attempt_at(db, x_content, "x", 20, False, error_category="media")
    _attempt_at(db, bsky_first, "bluesky", 15, False, error_category="media")
    _attempt_at(db, bsky_second, "bluesky", 5, False, error_category="media")

    report = build_publication_error_burst_report(
        db,
        hours=1,
        min_failures=2,
        min_consecutive=9,
        platform="bluesky",
        now=NOW,
    )

    assert report["filters"]["platform"] == "bluesky"
    assert report["totals"]["attempts"] == 2
    assert [(burst["platform"], burst["count"]) for burst in report["bursts"]] == [
        ("bluesky", 2),
    ]


def test_json_formatter_is_stable_and_text_names_burst_fields(db):
    content_id = _insert_content(db, "json")
    _attempt_at(db, content_id, "x", 15, False, error_category="rate_limit")

    report = build_publication_error_burst_report(
        db,
        hours=1,
        min_failures=1,
        min_consecutive=2,
        now=NOW,
    )
    payload = json.loads(format_publication_error_burst_json(report))
    text = format_publication_error_burst_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["artifact_type"] == "publication_error_bursts"
    assert payload["bursts"][0]["error_category"] == "rate_limit"
    assert "x / rate_limit" in text
    assert "count=1" in text
    assert f"first_seen={(NOW - timedelta(minutes=15)).isoformat()}" in text
    assert f"last_seen={(NOW - timedelta(minutes=15)).isoformat()}" in text


def test_cli_supports_filters_and_json_output(db, monkeypatch, capsys):
    content_id = _insert_content(db, "cli")
    _attempt_at(db, content_id, "x", 25, False, error_category="auth")
    monkeypatch.setattr(
        publication_error_bursts_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        publication_error_bursts_script,
        "build_publication_error_burst_report",
        lambda db, **kwargs: build_publication_error_burst_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = publication_error_bursts_script.main(
        [
            "--hours",
            "1",
            "--min-failures",
            "1",
            "--min-consecutive",
            "9",
            "--platform",
            "x",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"]["hours"] == 1
    assert payload["filters"]["platform"] == "x"
    assert payload["bursts"][0]["error_category"] == "auth"


def test_invalid_arguments_and_missing_schema(db):
    with pytest.raises(ValueError, match="hours must be positive"):
        build_publication_error_burst_report(db, hours=0, now=NOW)
    with pytest.raises(ValueError, match="min_failures must be positive"):
        build_publication_error_burst_report(db, min_failures=0, now=NOW)
    with pytest.raises(ValueError, match="min_consecutive must be positive"):
        build_publication_error_burst_report(db, min_consecutive=0, now=NOW)
    with pytest.raises(ValueError, match="invalid platform"):
        build_publication_error_burst_report(db, platform="mastodon", now=NOW)

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_publication_error_burst_report(conn, now=NOW)
    assert report["missing_tables"] == ["publication_attempts"]
    assert report["bursts"] == []
