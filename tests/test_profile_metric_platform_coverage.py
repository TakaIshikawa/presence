"""Tests for profile metric platform coverage reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.profile_metric_platform_coverage import (
    build_profile_metric_platform_coverage_report,
    build_profile_metric_platform_coverage_report_from_db,
    format_profile_metric_platform_coverage_json,
    format_profile_metric_platform_coverage_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "profile_metric_platform_coverage.py"
spec = importlib.util.spec_from_file_location("profile_metric_platform_coverage_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_flags_missing_stale_and_malformed_counts():
    report = build_profile_metric_platform_coverage_report(
        [
            {"metric_id": 1, "platform": "x", "follower_count": "100", "fetched_at": "2026-05-19T00:00:00+00:00"},
            {"metric_id": 2, "platform": "linkedin", "follower_count": "200", "fetched_at": "2026-05-17T00:00:00+00:00"},
            {"metric_id": 3, "platform": "x", "follower_count": "bad", "listed_count": "also-bad", "fetched_at": "2026-05-19T00:00:00+00:00"},
        ],
        expected_platforms=("x", "linkedin", "bluesky"),
        stale_days=2,
        now=NOW,
    )
    payload = json.loads(format_profile_metric_platform_coverage_json(report))
    assert payload["artifact_type"] == "profile_metric_platform_coverage"
    assert payload["summary"]["expected_platform_count"] == 3
    assert payload["summary"]["observed_platform_count"] == 2
    assert payload["summary"]["stale_platform_count"] == 1
    assert [item["issue_type"] for item in payload["issue_items"]] == ["missing_platform", "stale_platform_metrics", "malformed_counts"]
    assert "Profile Metric Platform Coverage" in format_profile_metric_platform_coverage_text(report)


def test_from_db_handles_optional_listed_count_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE profile_metrics (
            id INTEGER PRIMARY KEY,
            platform TEXT,
            follower_count TEXT,
            fetched_at TEXT
        );
        INSERT INTO profile_metrics VALUES (1, 'x', '100', '2026-05-19T00:00:00+00:00');
        """
    )
    report = build_profile_metric_platform_coverage_report_from_db(conn, expected_platforms=("x", "linkedin"), now=NOW)
    assert report["summary"]["issue_count"] == 1
    assert report["issue_items"][0]["issue_type"] == "missing_platform"

    db_path = tmp_path / "profile.sqlite"
    conn.backup(sqlite3.connect(db_path))
    assert script.main(["--db", str(db_path), "--expected-platform", "x", "--expected-platform", "linkedin"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["issue_count"] == 1

    missing = build_profile_metric_platform_coverage_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["profile_metrics"]
