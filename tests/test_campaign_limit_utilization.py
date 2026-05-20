"""Tests for campaign limit utilization reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sqlite3

from evaluation.campaign_limit_utilization import (
    build_campaign_limit_utilization_report,
    build_campaign_limit_utilization_report_from_db,
    format_campaign_limit_utilization_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "campaign_limit_utilization.py"
spec = importlib.util.spec_from_file_location("campaign_limit_utilization_script", SCRIPT_PATH)
campaign_limit_utilization_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(campaign_limit_utilization_script)


def test_builder_flags_daily_weekly_missing_dates_and_zero_topics():
    rows = [
        {"campaign_id": 1, "campaign_name": "A", "campaign_status": "active", "daily_limit": 1, "weekly_limit": 2, "planned_topic_id": 1, "target_date": "2026-05-20"},
        {"campaign_id": 1, "campaign_name": "A", "campaign_status": "active", "daily_limit": 1, "weekly_limit": 2, "planned_topic_id": 2, "target_date": "2026-05-20"},
        {"campaign_id": 1, "campaign_name": "A", "campaign_status": "active", "daily_limit": 1, "weekly_limit": 2, "planned_topic_id": 3, "target_date": "2026-05-21"},
        {"campaign_id": 1, "campaign_name": "A", "campaign_status": "active", "daily_limit": 1, "weekly_limit": 2, "planned_topic_id": 4, "target_date": "not-a-date"},
        {"campaign_id": 2, "campaign_name": "B", "campaign_status": "planned", "daily_limit": None, "weekly_limit": None},
        {"campaign_id": 3, "campaign_name": "C", "campaign_status": "archived", "daily_limit": 1, "weekly_limit": 1},
    ]

    report = build_campaign_limit_utilization_report(rows, now=NOW, limit=10)

    assert report["artifact_type"] == "campaign_limit_utilization"
    assert report["summary"]["campaigns_scanned"] == 2
    assert [item["issue_type"] for item in report["issue_items"]] == [
        "over_daily_limit",
        "over_weekly_limit",
        "no_target_dates",
        "campaign_without_planned_topics",
    ]
    assert report["campaign_summaries"][0]["planned_counts_by_week"] == {"2026-W21": 3}


def test_db_loader_preserves_campaigns_with_zero_topics():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE content_campaigns (
            id INTEGER PRIMARY KEY, name TEXT, status TEXT, daily_limit INTEGER, weekly_limit INTEGER
        );
        CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY, campaign_id INTEGER, topic TEXT, target_date TEXT
        );
        INSERT INTO content_campaigns VALUES (1, 'A', 'active', 2, 3);
        INSERT INTO content_campaigns VALUES (2, 'B', 'planned', 1, 1);
        INSERT INTO planned_topics VALUES (10, 1, 'one', '2026-05-20');
        """
    )

    report = build_campaign_limit_utilization_report_from_db(conn, now=NOW)

    assert report["missing_tables"] == []
    assert [summary["campaign_id"] for summary in report["campaign_summaries"]] == [1, 2]
    assert report["issue_items"][0]["issue_type"] == "campaign_without_planned_topics"
    assert "Campaign Limit Utilization" in format_campaign_limit_utilization_text(report)


def test_missing_tables_report_metadata():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_campaign_limit_utilization_report_from_db(conn, now=NOW)
    assert report["missing_tables"] == ["content_campaigns", "planned_topics"]
    assert report["summary"]["campaigns_scanned"] == 0


def test_cli_outputs_json_and_text(tmp_path, capsys):
    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE content_campaigns (id INTEGER PRIMARY KEY, status TEXT);
        CREATE TABLE planned_topics (id INTEGER PRIMARY KEY, campaign_id INTEGER, target_date TEXT);
        INSERT INTO content_campaigns VALUES (1, 'planned');
        """
    )
    conn.close()

    assert campaign_limit_utilization_script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert '"artifact_type": "campaign_limit_utilization"' in capsys.readouterr().out
    assert campaign_limit_utilization_script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Campaign Limit Utilization" in capsys.readouterr().out
