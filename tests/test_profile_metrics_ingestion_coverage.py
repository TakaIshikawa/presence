"""Tests for profile metrics ingestion coverage reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.profile_metrics_ingestion_coverage import (
    build_profile_metrics_ingestion_coverage_report,
    format_profile_metrics_ingestion_coverage_json,
    format_profile_metrics_ingestion_coverage_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "profile_metrics_ingestion_coverage.py"
)
spec = importlib.util.spec_from_file_location(
    "profile_metrics_ingestion_coverage_script",
    SCRIPT_PATH,
)
profile_metrics_ingestion_coverage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(profile_metrics_ingestion_coverage_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _metric(
    db,
    platform: str,
    followers: int,
    tweets: int,
    fetched_at: datetime,
) -> None:
    db.conn.execute(
        """INSERT INTO profile_metrics
           (platform, follower_count, following_count, tweet_count, listed_count, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (platform, followers, 10, tweets, None, fetched_at.isoformat()),
    )
    db.conn.commit()


def test_fresh_platform_reports_counts_gaps_and_deltas(db):
    _metric(db, "x", 100, 50, NOW - timedelta(hours=24))
    _metric(db, "x", 105, 53, NOW - timedelta(hours=12))
    _metric(db, "x", 111, 56, NOW)

    report = build_profile_metrics_ingestion_coverage_report(
        db,
        days=2,
        expected_interval_hours=24,
        max_stale_hours=36,
        now=NOW,
    )

    assert report.has_issues is False
    row = report.rows[0]
    assert row.platform == "x"
    assert row.status == "fresh"
    assert row.sample_count == 3
    assert row.latest_sample_age_hours == 0
    assert row.max_gap_hours == 12
    assert row.follower_delta == 11
    assert row.tweet_count_delta == 6
    assert "status=fresh" in format_profile_metrics_ingestion_coverage_text(report)


def test_sparse_status_responds_to_expected_interval_hours(db):
    _metric(db, "x", 100, 50, NOW - timedelta(hours=30))
    _metric(db, "x", 104, 52, NOW - timedelta(hours=1))

    sparse = build_profile_metrics_ingestion_coverage_report(
        db,
        days=3,
        expected_interval_hours=24,
        max_stale_hours=48,
        now=NOW,
    )
    fresh = build_profile_metrics_ingestion_coverage_report(
        db,
        days=3,
        expected_interval_hours=30,
        max_stale_hours=48,
        now=NOW,
    )

    assert sparse.rows[0].status == "sparse"
    assert sparse.rows[0].max_gap_hours == 29
    assert fresh.rows[0].status == "fresh"


def test_stale_status_responds_to_max_stale_hours(db):
    _metric(db, "x", 100, 50, NOW - timedelta(hours=40))
    _metric(db, "x", 105, 55, NOW - timedelta(hours=25))

    stale = build_profile_metrics_ingestion_coverage_report(
        db,
        days=3,
        expected_interval_hours=24,
        max_stale_hours=24,
        now=NOW,
    )
    not_stale = build_profile_metrics_ingestion_coverage_report(
        db,
        days=3,
        expected_interval_hours=24,
        max_stale_hours=26,
        now=NOW,
    )

    assert stale.rows[0].status == "stale"
    assert stale.rows[0].latest_sample_age_hours == 25
    assert not_stale.rows[0].status == "fresh"


def test_platform_filter_limits_rows_and_samples(db):
    _metric(db, "x", 100, 50, NOW - timedelta(hours=12))
    _metric(db, "x", 101, 51, NOW)
    _metric(db, "bluesky", 20, 5, NOW - timedelta(hours=12))
    _metric(db, "bluesky", 21, 6, NOW)

    report = build_profile_metrics_ingestion_coverage_report(
        db,
        days=2,
        platform="bluesky",
        expected_interval_hours=24,
        max_stale_hours=48,
        now=NOW,
    )

    assert [row.platform for row in report.rows] == ["bluesky"]
    assert report.totals["sample_count"] == 2
    assert report.rows[0].follower_delta == 1


def test_missing_profile_metrics_schema_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_profile_metrics_ingestion_coverage_report(conn, now=NOW)
    payload = json.loads(format_profile_metrics_ingestion_coverage_json(report))

    assert report.rows == ()
    assert report.missing_tables == ("profile_metrics",)
    assert report.totals["sample_count"] == 0
    assert payload["missing_tables"] == ["profile_metrics"]


def test_missing_required_columns_returns_empty_report_with_schema_gaps():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE profile_metrics (platform TEXT, fetched_at TEXT)")

    report = build_profile_metrics_ingestion_coverage_report(conn, now=NOW)

    assert report.rows == ()
    assert report.missing_columns == {
        "profile_metrics": ("follower_count", "tweet_count"),
    }


def test_cli_outputs_json(db, monkeypatch, capsys):
    _metric(db, "x", 100, 50, NOW - timedelta(hours=24))
    _metric(db, "x", 103, 51, NOW)
    monkeypatch.setattr(
        profile_metrics_ingestion_coverage_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        profile_metrics_ingestion_coverage_script,
        "build_profile_metrics_ingestion_coverage_report",
        lambda db, **kwargs: build_profile_metrics_ingestion_coverage_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = profile_metrics_ingestion_coverage_script.main(
        [
            "--days",
            "2",
            "--platform",
            "x",
            "--expected-interval-hours",
            "24",
            "--max-stale-hours",
            "48",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["artifact_type"] == "profile_metrics_ingestion_coverage"
    assert payload["filters"]["platform"] == "x"
    assert payload["rows"][0]["status"] == "fresh"
    assert payload["rows"][0]["tweet_count_delta"] == 1
