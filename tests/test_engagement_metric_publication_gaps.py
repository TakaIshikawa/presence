"""Tests for engagement metric publication gap reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.engagement_metric_publication_gaps import (
    build_engagement_metric_publication_gaps_report,
    build_engagement_metric_publication_gaps_report_from_db,
    format_engagement_metric_publication_gaps_json,
    format_engagement_metric_publication_gaps_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "engagement_metric_publication_gaps.py"
spec = importlib.util.spec_from_file_location("engagement_metric_publication_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _ts(hours_ago: int) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE generated_content (
               id INTEGER PRIMARY KEY,
               status TEXT,
               published INTEGER,
               published_at TEXT
           );
           CREATE TABLE content_publications (
               id INTEGER PRIMARY KEY,
               content_id INTEGER,
               platform TEXT,
               status TEXT,
               platform_post_id TEXT,
               platform_url TEXT,
               published_at TEXT
           );
           CREATE TABLE post_engagement (
               content_id INTEGER,
               tweet_id TEXT,
               url TEXT,
               fetched_at TEXT
           );
           CREATE TABLE bluesky_engagement (
               content_id INTEGER,
               uri TEXT,
               fetched_at TEXT
           );"""
    )
    return conn


def test_builder_summarizes_metric_publication_gaps_by_platform_and_issue():
    report = build_engagement_metric_publication_gaps_report(
        [
            {"metric_id": 1, "platform": "x", "content_id": None},
            {"metric_id": 2, "platform": "x", "content_id": 99},
            {"metric_id": 3, "platform": "x", "content_id": 10, "metric_post_id": "wrong", "metric_url": "https://x/wrong"},
            {"metric_id": 4, "platform": "bluesky", "content_id": 11},
        ],
        [
            {"id": 10, "status": "published"},
            {"id": 11, "status": "draft"},
        ],
        [
            {"publication_id": 1, "content_id": 10, "platform": "x", "status": "published", "platform_post_id": "tw-10", "platform_url": "https://x/10", "published_at": _ts(30)},
            {"publication_id": 2, "content_id": 12, "platform": "mastodon", "status": "published", "published_at": _ts(30)},
        ],
        minimum_age_hours=24,
        now=NOW,
    )

    assert report["artifact_type"] == "engagement_metric_publication_gaps"
    assert report["summary"]["by_issue_type"] == {
        "content_without_published_marker": 1,
        "engagement_content_missing": 1,
        "engagement_missing_content_id": 1,
        "platform_post_id_mismatch": 1,
        "platform_url_mismatch": 1,
        "published_content_missing_engagement": 1,
    }
    assert {"platform": "mastodon", "issue_type": "published_content_missing_engagement", "count": 1} in report["groups"]


def test_db_loader_inspects_existing_metric_tables_and_records_absent_tables():
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (10, 'published', 1, ?)", (_ts(30),))
    conn.execute("INSERT INTO generated_content VALUES (11, 'published', 1, ?)", (_ts(30),))
    conn.execute("INSERT INTO content_publications VALUES (1, 10, 'x', 'published', 'tw-10', 'https://x/10', ?)", (_ts(30),))
    conn.execute("INSERT INTO content_publications VALUES (2, 11, 'bluesky', 'published', 'at://b/11', NULL, ?)", (_ts(30),))
    conn.execute("INSERT INTO post_engagement VALUES (10, 'wrong', 'https://x/10', ?)", (_ts(1),))

    report = build_engagement_metric_publication_gaps_report_from_db(conn, minimum_age_hours=24, now=NOW)

    assert report["missing_tables"] == ["linkedin_engagement", "mastodon_engagement"]
    assert report["summary"]["by_issue_type"] == {
        "platform_post_id_mismatch": 1,
        "published_content_missing_engagement": 1,
    }
    assert [item["platform"] for item in report["gap_items"]] == ["bluesky", "x"]


def test_cli_json_text_and_validation(tmp_path, capsys):
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (10, 'published', 1, ?)", (_ts(30),))
    conn.execute("INSERT INTO content_publications VALUES (1, 10, 'x', 'published', 'tw-10', NULL, ?)", (_ts(30),))
    conn.commit()
    db_path = tmp_path / "engagement.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path), "--format", "json", "--minimum-age-hours", "1000"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "engagement_metric_publication_gaps"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Engagement Metric Publication Gaps" in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--minimum-age-hours", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert script.main(["--db", str(db_path), "--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_formatters_missing_tables_and_invalid_thresholds():
    missing = build_engagement_metric_publication_gaps_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == [
        "bluesky_engagement",
        "content_publications",
        "generated_content",
        "linkedin_engagement",
        "mastodon_engagement",
        "post_engagement",
    ]
    report = build_engagement_metric_publication_gaps_report([], [], [], now=NOW)
    assert json.loads(format_engagement_metric_publication_gaps_json(report))["artifact_type"] == "engagement_metric_publication_gaps"
    assert "No engagement metric publication gaps found" in format_engagement_metric_publication_gaps_text(report)
    with pytest.raises(ValueError, match="minimum_age_hours must be positive"):
        build_engagement_metric_publication_gaps_report([], [], [], minimum_age_hours=0)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_engagement_metric_publication_gaps_report([], [], [], limit=0)
