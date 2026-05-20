"""Tests for profile metrics platform snapshot gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.profile_metrics_platform_snapshot_gaps import (
    build_profile_metrics_platform_snapshot_gaps_report,
    build_profile_metrics_platform_snapshot_gaps_report_from_db,
    format_profile_metrics_platform_snapshot_gaps_json,
    format_profile_metrics_platform_snapshot_gaps_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "profile_metrics_platform_snapshot_gaps.py"
spec = importlib.util.spec_from_file_location("profile_metrics_platform_snapshot_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE profile_metrics (
            id INTEGER PRIMARY KEY,
            platform TEXT,
            follower_count INTEGER,
            following_count INTEGER,
            tweet_count INTEGER,
            listed_count INTEGER,
            fetched_at TEXT
        );
        """
    )
    return conn


def _insert(
    conn: sqlite3.Connection,
    row_id: int,
    platform: str,
    fetched_at: datetime,
    *,
    followers: int = 100,
    following: int = 10,
    tweets: int = 50,
    listed: int = 1,
) -> None:
    conn.execute(
        """INSERT INTO profile_metrics
           (id, platform, follower_count, following_count, tweet_count, listed_count, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (row_id, platform, followers, following, tweets, listed, fetched_at.isoformat()),
    )


def test_builder_flags_stale_gap_non_monotonic_and_negative_metrics():
    report = build_profile_metrics_platform_snapshot_gaps_report(
        [
            {
                "platform": "x",
                "fetched_at": (NOW - timedelta(hours=5)).isoformat(),
                "follower_count": 100,
                "following_count": 10,
                "tweet_count": 50,
                "listed_count": 1,
            },
            {
                "platform": "x",
                "fetched_at": (NOW - timedelta(hours=40)).isoformat(),
                "follower_count": -1,
                "following_count": 10,
                "tweet_count": 55,
                "listed_count": 1,
            },
            {
                "platform": "x",
                "fetched_at": (NOW - timedelta(hours=1)).isoformat(),
                "follower_count": 110,
                "following_count": 11,
                "tweet_count": 60,
                "listed_count": -2,
            },
            {
                "platform": "bluesky",
                "fetched_at": (NOW - timedelta(hours=60)).isoformat(),
                "follower_count": 10,
                "following_count": 5,
                "tweet_count": 2,
                "listed_count": 0,
            },
        ],
        max_age_hours=24,
        max_gap_hours=24,
        now=NOW,
    )

    assert report["artifact_type"] == "profile_metrics_platform_snapshot_gaps"
    counts = report["summary"]["by_issue_type"]
    assert counts["stale_platform_snapshot"] == 1
    assert counts["snapshot_gap"] == 1
    assert counts["non_monotonic_snapshot"] == 1
    assert counts["negative_metric"] == 2


def test_platform_filter_and_limit_are_applied():
    report = build_profile_metrics_platform_snapshot_gaps_report(
        [
            {
                "platform": "x",
                "fetched_at": (NOW - timedelta(hours=60)).isoformat(),
                "follower_count": 100,
                "following_count": 10,
                "tweet_count": 50,
                "listed_count": 1,
            },
            {
                "platform": "bluesky",
                "fetched_at": (NOW - timedelta(hours=60)).isoformat(),
                "follower_count": 20,
                "following_count": 5,
                "tweet_count": 3,
                "listed_count": 0,
            },
        ],
        platform="bluesky",
        max_age_hours=24,
        limit=1,
        now=NOW,
    )

    assert report["summary"]["snapshot_count"] == 1
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["platform"] == "bluesky"


def test_db_loader_reads_snapshots_and_handles_missing_schema():
    conn = _conn()
    _insert(conn, 1, "x", NOW - timedelta(hours=48))
    _insert(conn, 2, "x", NOW - timedelta(hours=1), followers=-5)

    report = build_profile_metrics_platform_snapshot_gaps_report_from_db(
        conn,
        platform="x",
        max_age_hours=24,
        max_gap_hours=24,
        now=NOW,
    )

    assert report["summary"]["platform_count"] == 1
    assert report["summary"]["by_issue_type"]["snapshot_gap"] == 1
    assert report["summary"]["by_issue_type"]["negative_metric"] == 1

    missing_table = build_profile_metrics_platform_snapshot_gaps_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing_table["missing_tables"] == ["profile_metrics"]

    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE profile_metrics (platform TEXT, fetched_at TEXT)")
    missing_columns = build_profile_metrics_platform_snapshot_gaps_report_from_db(bad, now=NOW)
    assert missing_columns["missing_columns"] == {
        "profile_metrics": ["follower_count", "following_count", "listed_count", "tweet_count"]
    }


def test_json_and_text_formatters_are_stable():
    report = build_profile_metrics_platform_snapshot_gaps_report(
        [
            {
                "platform": "x",
                "fetched_at": (NOW - timedelta(hours=60)).isoformat(),
                "follower_count": 100,
                "following_count": 10,
                "tweet_count": 50,
                "listed_count": 1,
            },
        ],
        max_age_hours=24,
        now=NOW,
    )

    payload = json.loads(format_profile_metrics_platform_snapshot_gaps_json(report))
    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "profile_metrics_platform_snapshot_gaps"
    text = format_profile_metrics_platform_snapshot_gaps_text(report)
    assert "Profile Metrics Platform Snapshot Gaps" in text
    assert "stale_platform_snapshot" in text


def test_cli_supports_db_platform_limits_json_text_and_invalid_numbers(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "profile_metrics.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE profile_metrics (
            id INTEGER PRIMARY KEY,
            platform TEXT,
            follower_count INTEGER,
            following_count INTEGER,
            tweet_count INTEGER,
            listed_count INTEGER,
            fetched_at TEXT
        );
        INSERT INTO profile_metrics VALUES (1, 'x', 100, 10, 50, 1, '2026-05-01T11:00:00+00:00');
        INSERT INTO profile_metrics VALUES (2, 'x', -1, 10, 55, 1, '2026-05-01T12:00:00+00:00');
        """
    )
    conn.close()

    assert script.main(
        [
            "--db",
            str(db_path),
            "--platform",
            "x",
            "--max-age-hours",
            "24",
            "--max-gap-hours",
            "24",
            "--limit",
            "5",
            "--format",
            "json",
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "profile_metrics_platform_snapshot_gaps"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "negative_metric" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_tables"] == ["profile_metrics"]
    with pytest.raises(SystemExit):
        script.parse_args(["--max-age-hours", "0"])
