"""Tests for engagement resonance drift reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from evaluation.resonance_drift import (
    build_resonance_drift_report,
    format_resonance_drift_json,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "resonance_drift.py"
spec = importlib.util.spec_from_file_location("resonance_drift_script", SCRIPT_PATH)
resonance_drift_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(resonance_drift_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    text: str,
    *,
    auto_quality: str | None,
    published_at: datetime | None = None,
) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="usable",
    )
    db.conn.execute(
        """UPDATE generated_content
           SET published = 1, published_at = ?, auto_quality = ?
           WHERE id = ?""",
        ((published_at or NOW).isoformat(), auto_quality, content_id),
    )
    db.conn.commit()
    return content_id


def _x_snapshot(db, content_id: int, score: float, fetched_at: datetime) -> None:
    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, like_count, retweet_count, reply_count,
            quote_count, engagement_score, fetched_at)
           VALUES (?, ?, 0, 0, 0, 0, ?, ?)""",
        (content_id, str(content_id), score, fetched_at.isoformat()),
    )
    db.conn.commit()


def test_positive_drift_reports_improved_metrics(db):
    baseline_low = _content(db, "Baseline low", auto_quality="low_resonance")
    baseline_high = _content(db, "Baseline okay", auto_quality="resonated")
    recent_high = _content(db, "Recent strong", auto_quality="resonated")
    recent_higher = _content(db, "Recent stronger", auto_quality="resonated")

    _x_snapshot(db, baseline_low, 2, NOW - timedelta(days=20))
    _x_snapshot(db, baseline_high, 4, NOW - timedelta(days=18))
    _x_snapshot(db, recent_high, 8, NOW - timedelta(days=4))
    _x_snapshot(db, recent_higher, 10, NOW - timedelta(days=2))

    report = build_resonance_drift_report(db, recent_days=7, baseline_days=21, now=NOW)
    payload = json.loads(format_resonance_drift_json(report))

    assert payload["artifact_type"] == "resonance_drift"
    assert report.status == "improved"
    assert report.baseline.average_engagement_score == 3
    assert report.recent.average_engagement_score == 9
    assert report.baseline.resonance_rate == 0.5
    assert report.recent.resonance_rate == 1.0
    by_metric = {item.metric: item for item in report.drift}
    assert by_metric["average_engagement_score"].absolute_drift == 6
    assert by_metric["average_engagement_score"].percent_drift == 200
    assert by_metric["resonance_rate"].status == "improved"


def test_negative_drift_reports_declined_metrics(db):
    baseline_high = _content(db, "Baseline strong", auto_quality="resonated")
    baseline_higher = _content(db, "Baseline stronger", auto_quality="resonated")
    recent_low = _content(db, "Recent weak", auto_quality="low_resonance")
    recent_lower = _content(db, "Recent weaker", auto_quality="low_resonance")

    _x_snapshot(db, baseline_high, 10, NOW - timedelta(days=16))
    _x_snapshot(db, baseline_higher, 8, NOW - timedelta(days=14))
    _x_snapshot(db, recent_low, 4, NOW - timedelta(days=4))
    _x_snapshot(db, recent_lower, 2, NOW - timedelta(days=1))

    report = build_resonance_drift_report(db, recent_days=7, baseline_days=14, now=NOW)

    assert report.status == "declined"
    by_metric = {item.metric: item for item in report.drift}
    assert by_metric["average_engagement_score"].absolute_drift == -6
    assert by_metric["average_engagement_score"].percent_drift == pytest.approx(-66.667)
    assert by_metric["resonance_rate"].absolute_drift == -1
    assert by_metric["resonance_rate"].percent_drift == -100
    assert by_metric["resonance_rate"].status == "declined"


def test_no_baseline_handles_missing_data_without_division_errors(db):
    recent = _content(db, "Only recent", auto_quality="resonated")
    _x_snapshot(db, recent, 5, NOW - timedelta(days=1))

    report = build_resonance_drift_report(db, recent_days=7, baseline_days=14, now=NOW)
    by_metric = {item.metric: item for item in report.drift}

    assert report.status == "no_baseline_data"
    assert report.baseline.row_count == 0
    assert report.baseline.average_engagement_score is None
    assert by_metric["average_engagement_score"].absolute_drift is None
    assert by_metric["average_engagement_score"].percent_drift is None
    assert by_metric["average_engagement_score"].status == "insufficient_data"


def test_date_windows_are_half_open_and_bucketed_deterministically(db):
    baseline_start = _content(db, "At baseline start", auto_quality="resonated")
    recent_start = _content(db, "At recent start", auto_quality="low_resonance")
    at_end = _content(db, "At end excluded", auto_quality="resonated")
    before = _content(db, "Before baseline", auto_quality="resonated")

    _x_snapshot(db, before, 99, NOW - timedelta(days=15, seconds=1))
    _x_snapshot(db, baseline_start, 3, NOW - timedelta(days=15))
    _x_snapshot(db, recent_start, 7, NOW - timedelta(days=5))
    _x_snapshot(db, at_end, 11, NOW)

    report = build_resonance_drift_report(
        db,
        recent_days=5,
        baseline_days=10,
        bucket_days=5,
        now=NOW,
    )

    assert report.baseline.row_count == 1
    assert report.baseline.average_engagement_score == 3
    assert report.recent.row_count == 1
    assert report.recent.average_engagement_score == 7
    assert [bucket.row_count for bucket in report.buckets] == [1, 0, 1]
    assert report.filters["baseline_start"] == (NOW - timedelta(days=15)).isoformat()
    assert report.filters["recent_start"] == (NOW - timedelta(days=5)).isoformat()


def test_latest_snapshot_per_content_platform_prevents_duplicate_fetch_inflation(db):
    content_id = _content(db, "Duplicate snapshots", auto_quality="resonated")
    _x_snapshot(db, content_id, 2, NOW - timedelta(days=2, hours=1))
    _x_snapshot(db, content_id, 8, NOW - timedelta(days=2))

    report = build_resonance_drift_report(db, recent_days=7, baseline_days=14, now=NOW)

    assert report.recent.row_count == 1
    assert report.recent.average_engagement_score == 8


def test_cli_supports_window_options_and_emits_json(db, monkeypatch, capsys):
    content_id = _content(db, "CLI row", auto_quality="resonated")
    _x_snapshot(db, content_id, 6, NOW - timedelta(days=1))
    monkeypatch.setattr(resonance_drift_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        resonance_drift_script,
        "build_resonance_drift_report",
        lambda db, **kwargs: build_resonance_drift_report(db, now=NOW, **kwargs),
    )

    assert resonance_drift_script.main(["--recent-days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    assert (
        resonance_drift_script.main(
            ["--recent-days", "3", "--baseline-days", "9", "--bucket-days", "3"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["recent_days"] == 3
    assert payload["filters"]["baseline_days"] == 9
    assert payload["filters"]["bucket_days"] == 3
    assert payload["recent"]["row_count"] == 1
