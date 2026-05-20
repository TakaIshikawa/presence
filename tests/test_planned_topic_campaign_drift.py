"""Tests for planned topic campaign drift reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.planned_topic_campaign_drift import (
    build_planned_topic_campaign_drift_report,
    build_planned_topic_campaign_drift_report_from_db,
    format_planned_topic_campaign_drift_json,
    format_planned_topic_campaign_drift_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "planned_topic_campaign_drift.py"
spec = importlib.util.spec_from_file_location("planned_topic_campaign_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_flags_campaign_relationship_drift_types():
    topics = [
        {"planned_topic_id": 1, "campaign_id": 99, "target_date": "2026-05-20", "status": "planned"},
        {"planned_topic_id": 2, "campaign_id": 1, "target_date": "2026-04-30", "status": "planned"},
        {"planned_topic_id": 3, "campaign_id": 1, "target_date": "2026-06-10", "status": "planned"},
        {"planned_topic_id": 4, "campaign_id": 2, "target_date": "2026-04-01", "status": "planned"},
        {"planned_topic_id": 5, "campaign_id": 3, "target_date": "2026-05-22", "status": "skipped"},
    ]
    campaigns = [
        {"campaign_id": 1, "campaign_name": "May", "start_date": "2026-05-01", "end_date": "2026-05-31", "status": "active"},
        {"campaign_id": 2, "campaign_name": "Old", "start_date": "2026-03-01", "end_date": "2026-06-01", "status": "active"},
        {"campaign_id": 3, "campaign_name": "Skipped", "start_date": "2026-05-01", "end_date": "2026-06-01", "status": "active"},
    ]
    report = build_planned_topic_campaign_drift_report(topics, campaigns, now=NOW, stale_days=30)
    payload = json.loads(format_planned_topic_campaign_drift_json(report))
    assert payload["artifact_type"] == "planned_topic_campaign_drift"
    assert [item["issue_type"] for item in payload["issue_items"]] == [
        "missing_campaign",
        "target_before_campaign",
        "target_after_campaign",
        "active_campaign_without_live_topics",
        "active_campaign_without_live_topics",
        "stale_planned_topic",
    ]
    assert payload["issue_items"][0]["planned_topic_id"] == 1
    assert "Planned Topic Campaign Drift" in format_planned_topic_campaign_drift_text(report)


def test_from_db_handles_missing_tables_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY,
            campaign_id INTEGER,
            target_date TEXT,
            status TEXT
        );
        CREATE TABLE content_campaigns (
            id INTEGER PRIMARY KEY,
            name TEXT,
            start_date TEXT,
            end_date TEXT,
            status TEXT
        );
        INSERT INTO content_campaigns VALUES (1, 'May', '2026-05-01', '2026-05-31', 'active');
        INSERT INTO planned_topics VALUES (1, 1, '2026-06-10', 'planned');
        """
    )
    report = build_planned_topic_campaign_drift_report_from_db(conn, now=NOW)
    assert report["summary"]["issue_count"] == 1
    assert report["issue_items"][0]["issue_type"] == "target_after_campaign"

    db_path = tmp_path / "campaign.sqlite"
    conn.backup(sqlite3.connect(db_path))
    assert script.main(["--db", str(db_path), "--stale-days", "30"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["issue_count"] == 1

    missing = build_planned_topic_campaign_drift_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["content_campaigns", "planned_topics"]
