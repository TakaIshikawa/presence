"""Tests for campaign topic deadline risk reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.campaign_topic_deadline_risk import (
    build_campaign_topic_deadline_risk_report,
    build_campaign_topic_deadline_risk_report_from_db,
    format_campaign_topic_deadline_risk_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "campaign_topic_deadline_risk.py"
spec = importlib.util.spec_from_file_location("campaign_topic_deadline_risk_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE content_campaigns (
            id INTEGER PRIMARY KEY,
            name TEXT,
            start_date TEXT,
            end_date TEXT
        );
        CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY,
            campaign_id INTEGER,
            target_date TEXT,
            status TEXT,
            content_id INTEGER
        );
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            published INTEGER,
            published_at TEXT
        );
        CREATE TABLE content_publications (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            status TEXT
        );
        """
    )
    return conn


def test_builder_flags_deadline_risks():
    rows = [
        {"campaign_id": 1, "campaign_name": "A", "campaign_start_date": "2026-05-01", "campaign_end_date": "2026-05-31", "topic_id": 1, "target_date": "2026-05-21", "status": "planned", "content_id": None},
        {"campaign_id": 1, "campaign_name": "A", "campaign_start_date": "2026-05-01", "campaign_end_date": "2026-05-31", "topic_id": 2, "target_date": "2026-05-18", "status": "generated", "content_id": 20, "publication_published": 0},
        {"campaign_id": 1, "campaign_name": "A", "campaign_start_date": "2026-05-01", "campaign_end_date": "2026-05-31", "topic_id": 3, "target_date": "2026-06-05", "status": "planned", "content_id": None},
    ]
    report = build_campaign_topic_deadline_risk_report(rows, now=NOW, warning_days=3)

    assert report["artifact_type"] == "campaign_topic_deadline_risk"
    assert report["filters"]["warning_days"] == 3
    assert report["summary"]["by_risk_reason"] == {
        "generated_overdue_unpublished": 1,
        "planned_due_soon_missing_content": 1,
        "topic_outside_campaign_dates": 1,
    }
    assert {
        "campaign_id",
        "topic_id",
        "target_date",
        "status",
        "risk_reason",
        "days_until_target",
        "content_id",
    }.issubset(report["findings"][0])


def test_db_loader_handles_optional_tables_and_publications():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE content_campaigns (id INTEGER PRIMARY KEY, start_date TEXT, end_date TEXT);
        CREATE TABLE planned_topics (id INTEGER PRIMARY KEY, campaign_id INTEGER, target_date TEXT, status TEXT, content_id INTEGER);
        """
    )
    conn.execute("INSERT INTO content_campaigns VALUES (1, '2026-05-01', '2026-05-31')")
    conn.execute("INSERT INTO planned_topics VALUES (10, 1, '2026-05-21', 'planned', NULL)")
    report = build_campaign_topic_deadline_risk_report_from_db(conn, now=NOW, warning_days=3)
    assert report["findings"][0]["risk_reason"] == "planned_due_soon_missing_content"

    full = _conn()
    full.execute("INSERT INTO content_campaigns VALUES (1, 'A', '2026-05-01', '2026-05-31')")
    full.execute("INSERT INTO planned_topics VALUES (10, 1, '2026-05-18', 'generated', 100)")
    full.execute("INSERT INTO generated_content VALUES (100, 0, NULL)")
    stale = build_campaign_topic_deadline_risk_report_from_db(full, now=NOW)
    assert stale["findings"][0]["content_id"] == 100
    assert stale["findings"][0]["risk_reason"] == "generated_overdue_unpublished"

    full.execute("INSERT INTO content_publications VALUES (1, 100, 'published')")
    clean = build_campaign_topic_deadline_risk_report_from_db(full, now=NOW)
    assert clean["summary"]["finding_count"] == 0


def test_schema_gaps_text_cli_and_validation(tmp_path, capsys):
    missing = build_campaign_topic_deadline_risk_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["content_campaigns", "planned_topics"]
    assert "No campaign topic deadline risks" in format_campaign_topic_deadline_risk_text(missing)

    db_path = tmp_path / "campaign.sqlite"
    conn = _conn(db_path)
    conn.execute("INSERT INTO content_campaigns VALUES (1, 'A', '2026-05-01', '2026-05-31')")
    conn.execute("INSERT INTO planned_topics VALUES (10, 1, '2026-05-21', 'planned', NULL)")
    conn.commit()
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json", "--now", "2026-05-20T12:00:00+00:00", "--warning-days", "3", "--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["limit"] == 5
    assert payload["findings"][0]["days_until_target"] == 1

    assert script.main(["--db", str(db_path), "--format", "text", "--now", "2026-05-20T12:00:00+00:00"]) == 0
    assert "Campaign Topic Deadline Risk" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
