"""Tests for publication reliability streak reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.publication_reliability_streaks import (
    PublicationAttemptRecord,
    build_publication_reliability_streak_report,
    format_publication_reliability_streak_json,
    format_publication_reliability_streak_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "publication_reliability_streaks.py"
)
spec = importlib.util.spec_from_file_location("publication_reliability_streaks_script", SCRIPT_PATH)
publication_reliability_streaks_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publication_reliability_streaks_script)


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


def test_in_memory_attempts_calculate_current_and_longest_streaks():
    attempts = [
        _record(6, "x", False, "network"),
        _record(5, "x", False, "network"),
        _record(4, "x", True),
        _record(3, "x", False, "rate_limit"),
        _record(2, "x", False, "rate_limit"),
        _record(1, "x", False, "not-real"),
    ]

    report = build_publication_reliability_streak_report(
        attempts=attempts,
        days=1,
        failure_threshold=3,
        now=NOW,
    )

    row = report.platforms[0]
    assert row.platform == "x"
    assert row.current_streak_type == "failure"
    assert row.current_streak_count == 3
    assert row.longest_failure_streak == 3
    assert row.total_attempts == 6
    assert row.success_count == 1
    assert row.failure_count == 5
    assert row.success_rate == 0.1667
    assert row.most_recent_error_category == "unknown"
    assert row.needs_attention is True
    assert report.totals["attention_platform_count"] == 1


def test_mixed_platforms_are_sorted_and_success_resets_current_streak(db):
    attempts = [
        _record(5, "x", False, "network"),
        _record(4, "bluesky", False, "auth"),
        _record(3, "x", True),
        _record(2, "bluesky", False, "auth"),
        _record(1, "x", True),
    ]

    report = build_publication_reliability_streak_report(
        attempts=attempts,
        days=1,
        failure_threshold=2,
        now=NOW,
    )

    assert [row.platform for row in report.platforms] == ["bluesky", "x"]
    bluesky, x_row = report.platforms
    assert bluesky.current_streak_type == "failure"
    assert bluesky.current_streak_count == 2
    assert bluesky.longest_failure_streak == 2
    assert bluesky.needs_attention is True
    assert x_row.current_streak_type == "success"
    assert x_row.current_streak_count == 2
    assert x_row.longest_failure_streak == 1
    assert x_row.needs_attention is False


def test_database_loading_applies_attempted_at_lookback(db):
    recent = _insert_content(db, "recent")
    old = _insert_content(db, "old")
    _attempt_at(db, old, "x", 60 * 48, False, error_category="network")
    _attempt_at(db, recent, "x", 30, True)

    report = build_publication_reliability_streak_report(db, days=1, now=NOW)

    row = report.platforms[0]
    assert row.platform == "x"
    assert row.total_attempts == 1
    assert row.success_count == 1
    assert row.current_streak_type == "success"
    assert row.longest_failure_streak == 0
    assert report.filters["cutoff"] == (NOW - timedelta(days=1)).isoformat()


def test_no_attempts_returns_empty_report(db):
    report = build_publication_reliability_streak_report(db, days=1, now=NOW)

    assert report.platforms == ()
    assert report.totals == {
        "attention_platform_count": 0,
        "failure_count": 0,
        "platform_count": 0,
        "success_count": 0,
        "success_rate": 0.0,
        "total_attempts": 0,
    }
    assert "No publication attempts found." in format_publication_reliability_streak_text(report)


def test_missing_publication_attempts_table_is_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_publication_reliability_streak_report(conn, days=1, now=NOW)

    assert report.missing_tables == ("publication_attempts",)
    assert report.platforms == ()
    assert "Missing tables: publication_attempts" in format_publication_reliability_streak_text(
        report
    )


def test_formatters_are_deterministic():
    report = build_publication_reliability_streak_report(
        attempts=[
            _record(2, "x", False, "network"),
            _record(1, "bluesky", True),
        ],
        days=1,
        failure_threshold=2,
        now=NOW,
    )

    payload = json.loads(format_publication_reliability_streak_json(report))
    text = format_publication_reliability_streak_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["artifact_type"] == "publication_reliability_streaks"
    assert [row["platform"] for row in payload["platforms"]] == ["bluesky", "x"]
    assert "bluesky: current=success:1" in text
    assert "x: current=failure:1" in text


def test_invalid_arguments_raise_value_error(db):
    with pytest.raises(ValueError, match="days must be positive"):
        build_publication_reliability_streak_report(db, days=0, now=NOW)
    with pytest.raises(ValueError, match="failure_threshold must be positive"):
        build_publication_reliability_streak_report(db, failure_threshold=0, now=NOW)


def test_cli_supports_db_days_threshold_and_json_output(file_db, capsys):
    content_id = _insert_content(file_db, "cli")
    _attempt_at(file_db, content_id, "x", 25, False, error_category="network")
    _attempt_at(file_db, content_id, "x", 5, True)

    exit_code = publication_reliability_streaks_script.main(
        [
            "--db",
            str(file_db.db_path),
            "--days",
            "30",
            "--failure-threshold",
            "2",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"]["days"] == 30
    assert payload["filters"]["failure_threshold"] == 2
    assert payload["platforms"][0]["platform"] == "x"
    assert payload["platforms"][0]["current_streak_type"] == "success"


def test_cli_parse_errors_return_argparse_status(capsys):
    assert publication_reliability_streaks_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_cli_uses_script_context_when_db_is_not_supplied(db, monkeypatch, capsys):
    content_id = _insert_content(db, "context")
    _attempt_at(db, content_id, "x", 20, True)
    monkeypatch.setattr(
        publication_reliability_streaks_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        publication_reliability_streaks_script,
        "build_publication_reliability_streak_report",
        lambda db, **kwargs: build_publication_reliability_streak_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = publication_reliability_streaks_script.main(["--days", "1"])

    assert exit_code == 0
    assert "x: current=success:1" in capsys.readouterr().out


def _record(minutes_ago: int, platform: str, success: bool, category: str | None = None):
    return PublicationAttemptRecord(
        id=minutes_ago,
        platform=platform,
        attempted_at=(NOW - timedelta(minutes=minutes_ago)).isoformat(),
        success=success,
        error_category=category,
    )
