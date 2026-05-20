"""Tests for content campaign throughput SLA reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.content_campaign_throughput_sla import (
    build_content_campaign_throughput_sla_report,
    build_content_campaign_throughput_sla_report_from_db,
    format_content_campaign_throughput_sla_json,
    format_content_campaign_throughput_sla_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_campaign_throughput_sla.py"
spec = importlib.util.spec_from_file_location("content_campaign_throughput_sla_script", SCRIPT_PATH)
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
            status TEXT,
            start_date TEXT,
            end_date TEXT,
            daily_limit INTEGER,
            weekly_limit INTEGER
        );
        CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY,
            campaign_id INTEGER,
            topic TEXT,
            target_date TEXT,
            status TEXT,
            content_id INTEGER
        );
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            published INTEGER,
            published_at TEXT,
            created_at TEXT
        );
        CREATE TABLE content_publications (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            status TEXT
        );
        """
    )
    return conn


def test_builder_flags_all_throughput_reasons():
    rows = [
        {"campaign_id": 1, "campaign_name": "A", "campaign_status": "active", "start_date": "2026-05-01", "end_date": "2026-05-30", "daily_limit": 1, "weekly_limit": 2, "topic_id": 1, "target_date": "2026-05-18", "topic_status": "planned", "content_id": None},
        {"campaign_id": 1, "campaign_name": "A", "campaign_status": "active", "start_date": "2026-05-01", "end_date": "2026-05-30", "daily_limit": 1, "weekly_limit": 2, "topic_id": 2, "target_date": "2026-05-20", "topic_status": "generated", "content_id": 20, "content_created_at": "2026-05-15T10:00:00+00:00", "generated_published": 0, "publication_published": 0},
        {"campaign_id": 1, "campaign_name": "A", "campaign_status": "active", "start_date": "2026-05-01", "end_date": "2026-05-30", "daily_limit": 1, "weekly_limit": 2, "topic_id": 3, "target_date": "2026-05-20", "topic_status": "planned", "content_id": None},
        {"campaign_id": 1, "campaign_name": "A", "campaign_status": "active", "start_date": "2026-05-01", "end_date": "2026-05-30", "daily_limit": 1, "weekly_limit": 2, "topic_id": 4, "target_date": "2026-05-21", "topic_status": "skipped", "content_id": None},
        {"campaign_id": 2, "campaign_name": "B", "campaign_status": "active", "start_date": "2026-06-01", "end_date": "2026-06-30", "daily_limit": 5, "weekly_limit": 5, "topic_id": 5, "target_date": "2026-06-02", "topic_status": "planned", "content_id": None},
    ]

    report = build_content_campaign_throughput_sla_report(rows, now=NOW, overdue_days=1, generated_age_days=3)
    payload = json.loads(format_content_campaign_throughput_sla_json(report))

    assert payload["artifact_type"] == "content_campaign_throughput_sla"
    assert payload["summary"]["by_throughput_reason"] == {
        "active_campaign_date_boundary": 1,
        "daily_limit_breach": 1,
        "overdue_planned_topic": 1,
        "stale_generated_unpublished": 1,
        "weekly_limit_breach": 1,
    }
    assert payload["summary"]["by_campaign"] == {"1": 4, "2": 1}
    assert payload["campaign_summaries"][0]["generated_count"] == 1
    assert payload["campaign_summaries"][0]["skipped_count"] == 1


def test_db_loader_and_missing_schema():
    missing = build_content_campaign_throughput_sla_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["content_campaigns", "planned_topics"]
    assert missing["summary"]["finding_count"] == 0

    bad = sqlite3.connect(":memory:")
    bad.executescript(
        """
        CREATE TABLE content_campaigns (name TEXT);
        CREATE TABLE planned_topics (id INTEGER);
        """
    )
    gaps = build_content_campaign_throughput_sla_report_from_db(bad, now=NOW)
    assert gaps["missing_columns"] == {
        "content_campaigns": ["id"],
        "planned_topics": ["campaign_id"],
    }

    conn = _conn()
    conn.execute("INSERT INTO content_campaigns VALUES (1, 'A', 'active', '2026-05-01', '2026-05-31', 1, 1)")
    conn.execute("INSERT INTO planned_topics VALUES (10, 1, 'topic', '2026-05-20', 'generated', 100)")
    conn.execute("INSERT INTO generated_content VALUES (100, 0, NULL, '2026-05-10T10:00:00+00:00')")
    report = build_content_campaign_throughput_sla_report_from_db(conn, now=NOW, generated_age_days=3)
    assert report["findings"][0]["throughput_reason"] == "stale_generated_unpublished"

    conn.execute("INSERT INTO content_publications VALUES (1, 100, 'published')")
    clean = build_content_campaign_throughput_sla_report_from_db(conn, now=NOW, generated_age_days=3)
    assert clean["summary"]["finding_count"] == 0


def test_cli_json_text_now_and_validation(tmp_path, capsys):
    db_path = tmp_path / "campaign.sqlite"
    conn = _conn(db_path)
    conn.execute("INSERT INTO content_campaigns VALUES (1, 'A', 'active', '2026-05-01', '2026-05-31', 1, 1)")
    conn.execute("INSERT INTO planned_topics VALUES (10, 1, 'topic', '2026-05-18', 'planned', NULL)")
    conn.commit()
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json", "--now", "2026-05-20T12:00:00+00:00", "--overdue-days", "1", "--generated-age-days", "3", "--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"] == {"generated_age_days": 3, "limit": 5, "overdue_days": 1}
    assert payload["findings"][0]["throughput_reason"] == "overdue_planned_topic"

    assert script.main(["--db", str(db_path), "--format", "text", "--now", "2026-05-20T12:00:00+00:00"]) == 0
    assert "Content Campaign Throughput SLA" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
    with pytest.raises(SystemExit):
        script.parse_args(["--overdue-days", "-1"])


def test_text_formatter_and_invalid_builder_args():
    report = build_content_campaign_throughput_sla_report([], now=NOW)
    assert "No content campaign throughput SLA gaps found" in format_content_campaign_throughput_sla_text(report)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_content_campaign_throughput_sla_report([], limit=0)
    with pytest.raises(ValueError, match="generated_age_days must be non-negative"):
        build_content_campaign_throughput_sla_report([], generated_age_days=-1)
