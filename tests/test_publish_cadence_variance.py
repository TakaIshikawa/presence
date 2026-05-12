"""Tests for publish cadence variance reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from evaluation.publish_cadence_variance import (
    build_publish_cadence_variance_report,
    format_publish_cadence_variance_json,
    format_publish_cadence_variance_text,
)


NOW = datetime(2026, 5, 11, 0, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_cadence_variance.py"
spec = importlib.util.spec_from_file_location("publish_cadence_variance_script", SCRIPT_PATH)
publish_cadence_variance_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publish_cadence_variance_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db) -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Published copy",
        eval_score=7.0,
        eval_feedback="good",
    )


def _success(db, channel: str, published_at: datetime) -> int:
    content_id = _content(db)
    db.record_publication_attempt(
        queue_id=None,
        content_id=content_id,
        platform=channel,
        success=True,
        attempted_at=published_at.isoformat(),
    )
    return content_id


def _week_row(report, channel: str, period_start: str = "2026-05-04"):
    return next(
        row
        for row in report.rows
        if row.channel == channel
        and row.period_type == "week"
        and row.period_start == period_start
    )


def test_balanced_cadence_matches_weekly_target(db):
    for days_ago in range(1, 8):
        _success(db, "x", NOW - timedelta(days=days_ago, hours=-1))

    report = build_publish_cadence_variance_report(
        db,
        lookback_days=7,
        targets={"x": 7},
        now=NOW,
    )

    week = _week_row(report, "x")
    assert week.expected_count == 7
    assert week.actual_count == 7
    assert week.status == "balanced"
    assert report.totals["by_channel"] == {"x": 7}


def test_under_posting_over_posting_and_quiet_windows_are_flagged(db):
    _success(db, "x", datetime(2026, 5, 5, 12, tzinfo=timezone.utc))
    for hour in (9, 10, 11):
        _success(db, "bluesky", datetime(2026, 5, 6, hour, tzinfo=timezone.utc))

    report = build_publish_cadence_variance_report(
        db,
        lookback_days=7,
        targets={"x": 7, "bluesky": 7},
        now=NOW,
    )

    assert _week_row(report, "x").status == "under_posting"
    assert _week_row(report, "bluesky").status == "under_posting"
    bluesky_day = next(
        row
        for row in report.rows
        if row.channel == "bluesky"
        and row.period_type == "day"
        and row.period_start == "2026-05-06"
    )
    assert bluesky_day.status == "over_posting"
    quiet_days = [
        row
        for row in report.rows
        if row.channel == "x" and row.period_type == "day" and row.quiet_window
    ]
    assert quiet_days
    assert report.totals["over_posting_count"] >= 1
    assert report.totals["quiet_window_count"] >= 1


def test_recent_variance_by_day_and_week_uses_channel_filter(db):
    _success(db, "x", datetime(2026, 5, 5, 12, tzinfo=timezone.utc))
    _success(db, "bluesky", datetime(2026, 5, 5, 12, tzinfo=timezone.utc))

    report = build_publish_cadence_variance_report(
        db,
        lookback_days=7,
        targets={"x": 7, "bluesky": 7},
        channels=("bluesky",),
        now=NOW,
    )

    assert report.filters["channels"] == ["bluesky"]
    assert {row.channel for row in report.rows} == {"bluesky"}
    assert any(row.period_type == "day" for row in report.rows)
    assert any(row.period_type == "week" for row in report.rows)
    assert report.totals["actual_count"] == 1


def test_content_publications_are_used_when_attempt_rows_are_absent(db):
    content_id = _content(db)
    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, published_at)
           VALUES (?, 'x', 'published', ?)""",
        (content_id, datetime(2026, 5, 5, 12, tzinfo=timezone.utc).isoformat()),
    )
    db.conn.commit()

    report = build_publish_cadence_variance_report(
        db,
        lookback_days=7,
        targets={"x": 1},
        now=NOW,
    )

    assert report.totals["actual_count"] == 1
    assert report.totals["by_channel"] == {"x": 1}


def test_formatter_output_and_cli_json(db, monkeypatch, capsys):
    _success(db, "x", datetime(2026, 5, 5, 12, tzinfo=timezone.utc))
    monkeypatch.setattr(
        publish_cadence_variance_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        publish_cadence_variance_script,
        "build_publish_cadence_variance_report",
        lambda db, **kwargs: build_publish_cadence_variance_report(db, now=NOW, **kwargs),
    )

    report = build_publish_cadence_variance_report(
        db,
        lookback_days=7,
        targets={"x": 7},
        now=NOW,
    )
    payload = json.loads(format_publish_cadence_variance_json(report))
    text = format_publish_cadence_variance_text(report)
    exit_code = publish_cadence_variance_script.main(
        ["--lookback-days", "7", "--channel", "x", "--target", "x=7", "--json"]
    )
    cli_payload = json.loads(capsys.readouterr().out)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "publish_cadence_variance"
    assert "Publish Cadence Variance" in text
    assert "status=under_posting" in text
    assert exit_code == 0
    assert cli_payload["filters"]["targets_per_week"] == {"x": 7.0}


def test_invalid_arguments_are_reported(db, capsys):
    with pytest.raises(ValueError, match="lookback_days must be positive"):
        build_publish_cadence_variance_report(db, lookback_days=0)
    with pytest.raises(ValueError, match="target counts must be non-negative"):
        build_publish_cadence_variance_report(db, targets={"x": -1})

    assert publish_cadence_variance_script.main(["--target", "bad"]) == 2
    assert "target must be CHANNEL=COUNT_PER_WEEK" in capsys.readouterr().err
