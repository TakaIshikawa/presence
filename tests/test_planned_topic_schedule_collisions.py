from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3

from synthesis.planned_topic_schedule_collisions import (
    build_planned_topic_schedule_collisions_report,
    format_planned_topic_schedule_collisions_json,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)


def _campaign(db, status="active"):
    db.conn.execute(
        "INSERT INTO content_campaigns (name, status, start_date, end_date) VALUES (?, ?, ?, ?)",
        ("Launch", status, "2026-05-01", "2026-05-31"),
    )
    db.conn.commit()
    return db.conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _topic(db, campaign_id, topic, target_date, status="planned", content_id=None):
    db.conn.execute(
        """INSERT INTO planned_topics (campaign_id, topic, angle, target_date, status, content_id)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (campaign_id, topic, "angle", target_date, status, content_id),
    )
    db.conn.commit()


def test_report_finds_collisions_and_state_gaps(db):
    campaign_id = _campaign(db)
    _topic(db, campaign_id, "ai", "2026-05-20")
    _topic(db, campaign_id, "ai", "2026-05-20")
    _topic(db, campaign_id, "old", "2026-05-01", status="planned")
    _topic(db, campaign_id, "generated", "2026-05-14", status="generated")
    _topic(db, campaign_id, "skipped", "2026-05-16", status="skipped")

    payload = json.loads(format_planned_topic_schedule_collisions_json(
        build_planned_topic_schedule_collisions_report(db, now=NOW)
    ))

    assert payload["artifact_type"] == "planned_topic_schedule_collisions"
    assert payload["totals"]["planned_topic_count"] == 5
    assert payload["date_groups"][0]["count"] == 1
    finding_types = {finding["finding_type"] for finding in payload["findings"]}
    assert {
        "same_day_collision",
        "overdue_planned_topic",
        "generated_missing_content",
        "skipped_active_campaign_topic",
    }.issubset(finding_types)
    assert payload["campaign_groups"][0]["status_counts"]["planned"] == 3


def test_content_campaigns_table_is_optional():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY, campaign_id INTEGER, topic TEXT, angle TEXT,
            target_date TEXT, status TEXT, content_id INTEGER, created_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO planned_topics VALUES (1, 42, 'ai', 'angle', '2026-05-01', 'planned', NULL, '2026-05-01')"
    )

    report = build_planned_topic_schedule_collisions_report(conn, now=NOW)

    assert report["missing_tables"] == []
    assert report["findings"][0]["finding_type"] == "overdue_planned_topic"


def test_missing_planned_topics_is_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_planned_topic_schedule_collisions_report(conn, now=NOW)

    assert report["missing_tables"] == ["planned_topics"]
