"""Tests for publication attempt recovery reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from evaluation.publication_attempt_recovery import (
    build_publication_attempt_recovery_report,
    format_publication_attempt_recovery_json,
    format_publication_attempt_recovery_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_recovery.py"
)
spec = importlib.util.spec_from_file_location("publication_attempt_recovery_script", SCRIPT_PATH)
publication_attempt_recovery_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publication_attempt_recovery_script)


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
        error=None if success else "temporary outage",
        error_category=error_category,
    )


def test_report_distinguishes_recovered_unrecovered_and_mixed_categories(db):
    recovered = _insert_content(db, "network recovers")
    unrecovered = _insert_content(db, "network still failing")
    auth = _insert_content(db, "auth still failing")

    _attempt_at(db, recovered, "x", 90, False, error_category="network")
    _attempt_at(db, recovered, "x", 50, False, error_category="network")
    _attempt_at(db, recovered, "x", 10, True)
    _attempt_at(db, unrecovered, "x", 40, False, error_category="network")
    _attempt_at(db, auth, "x", 30, False, error_category="auth")

    report = build_publication_attempt_recovery_report(
        db,
        days=1,
        platform="x",
        now=NOW,
    )

    network = _bucket(report, "x", "network")
    assert network["failed_attempts"] == 3
    assert network["later_successes"] == 2
    assert network["unrecovered_count"] == 1
    assert network["recovery_rate"] == 0.6667
    assert network["median_attempts_to_recovery"] == 1.5
    assert network["representative_content_ids"] == [unrecovered, recovered]
    auth_bucket = _bucket(report, "x", "auth")
    assert auth_bucket["later_successes"] == 0
    assert "refresh credentials" in auth_bucket["recommendation"]
    assert report["totals"]["failed_attempts"] == 4
    assert report["totals"]["later_successes"] == 2
    assert report["totals"]["unrecovered_count"] == 2


def test_report_uses_content_publications_success_when_attempt_log_has_no_success(db):
    content_id = _insert_content(db, "ledger success")
    _attempt_at(db, content_id, "bluesky", 80, False, error_category="rate_limit")
    _attempt_at(db, content_id, "bluesky", 40, False, error_category="rate_limit")
    db.upsert_publication_success(
        content_id,
        "bluesky",
        published_at=(NOW - timedelta(minutes=5)).isoformat(),
    )

    report = build_publication_attempt_recovery_report(
        db,
        days=1,
        platform="bluesky",
        now=NOW,
    )

    bucket = _bucket(report, "bluesky", "rate_limit")
    assert bucket["failed_attempts"] == 2
    assert bucket["later_successes"] == 2
    assert bucket["unrecovered_count"] == 0
    assert bucket["median_attempts_to_recovery"] == 1.5


def test_platform_filter_and_day_window_limit_failed_attempt_groups(db):
    recent_x = _insert_content(db, "recent x")
    old_x = _insert_content(db, "old x")
    recent_bluesky = _insert_content(db, "recent bluesky")

    _attempt_at(db, recent_x, "x", 30, False, error_category="media")
    _attempt_at(db, old_x, "x", 60 * 48, False, error_category="media")
    _attempt_at(db, old_x, "x", 10, True)
    _attempt_at(db, recent_bluesky, "bluesky", 20, False, error_category="media")

    report = build_publication_attempt_recovery_report(
        db,
        days=1,
        platform="x",
        now=NOW,
    )

    assert [bucket["platform"] for bucket in report["buckets"]] == ["x"]
    bucket = _bucket(report, "x", "media")
    assert bucket["failed_attempts"] == 1
    assert bucket["representative_content_ids"] == [recent_x]


def test_json_and_text_output_include_recovery_rate_and_low_recovery_recommendation(db):
    content_id = _insert_content(db, "unknown failure")
    _attempt_at(db, content_id, "x", 15, False, error_category="not-real")

    report = build_publication_attempt_recovery_report(db, days=1, now=NOW)
    payload = json.loads(format_publication_attempt_recovery_json(report))
    text = format_publication_attempt_recovery_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["artifact_type"] == "publication_attempt_recovery"
    assert payload["buckets"][0]["error_category"] == "unknown"
    assert "recovery_rate=0.0%" in text
    assert "Low recovery:" in text
    assert str(content_id) in text


def test_invalid_arguments_raise_value_error(db):
    with pytest.raises(ValueError, match="days must be positive"):
        build_publication_attempt_recovery_report(db, days=0, now=NOW)
    with pytest.raises(ValueError, match="invalid platform"):
        build_publication_attempt_recovery_report(db, platform="mastodon", now=NOW)
    with pytest.raises(ValueError, match="representative_limit must be positive"):
        build_publication_attempt_recovery_report(db, representative_limit=0, now=NOW)


def test_cli_supports_filters_limit_and_json_output(db, monkeypatch, capsys):
    content_id = _insert_content(db, "cli x")
    _attempt_at(db, content_id, "x", 25, False, error_category="network")
    _attempt_at(db, content_id, "x", 5, True)
    monkeypatch.setattr(
        publication_attempt_recovery_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        publication_attempt_recovery_script,
        "build_publication_attempt_recovery_report",
        lambda db, **kwargs: build_publication_attempt_recovery_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = publication_attempt_recovery_script.main(
        [
            "--days",
            "1",
            "--platform",
            "x",
            "--format",
            "json",
            "--representative-limit",
            "1",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["window_days"] == 1
    assert payload["platform"] == "x"
    assert payload["representative_limit"] == 1
    assert payload["buckets"][0]["representative_content_ids"] == [content_id]


def _bucket(report: dict, platform: str, category: str) -> dict:
    return next(
        bucket
        for bucket in report["buckets"]
        if bucket["platform"] == platform and bucket["error_category"] == category
    )
