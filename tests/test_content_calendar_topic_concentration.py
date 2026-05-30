from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from evaluation.content_calendar_topic_concentration import (
    build_content_calendar_topic_concentration_report,
    build_content_calendar_topic_concentration_report_from_db,
    format_content_calendar_topic_concentration_json,
    format_content_calendar_topic_concentration_text,
)


NOW = datetime(2026, 5, 30, tzinfo=timezone.utc)


def test_detects_repeated_topics_in_rolling_window():
    rows = [
        {"item_id": "a", "planned_at": "2026-06-01T09:00:00+00:00", "topic": "AI Agents", "category": "ops"},
        {"item_id": "b", "planned_at": "2026-06-03T09:00:00+00:00", "topic": "ai agents", "category": "ops"},
        {"item_id": "c", "planned_at": "2026-06-05T09:00:00+00:00", "topic": "AI Agents", "category": "ops"},
    ]
    report = build_content_calendar_topic_concentration_report(rows, window_days=7, max_topic_items=2, now=NOW)
    issue = report["issues"][0]
    assert issue["issue_type"] == "topic_concentration"
    assert issue["item_ids"] == ["a", "b", "c"]
    assert issue["severity"] == "medium"


def test_balanced_calendar_has_no_findings():
    rows = [
        {"item_id": "a", "planned_at": "2026-06-01T09:00:00+00:00", "topic": "AI", "category": "research"},
        {"item_id": "b", "planned_at": "2026-06-02T09:00:00+00:00", "topic": "Ops", "category": "ops"},
    ]
    report = build_content_calendar_topic_concentration_report(rows, required_categories="research,ops", now=NOW)
    assert report["issues"] == []
    assert "content_calendar_topic_concentration" in format_content_calendar_topic_concentration_json(report)
    assert "No content calendar" in format_content_calendar_topic_concentration_text(report)


def test_missing_required_category_is_reported():
    report = build_content_calendar_topic_concentration_report(
        [{"item_id": "a", "planned_at": "2026-06-01T09:00:00+00:00", "topic": "AI", "category": "research"}],
        required_categories=["research", "case study"],
        now=NOW,
    )
    assert report["issues"][0]["issue_type"] == "missing_category_coverage"
    assert report["issues"][0]["category"] == "case study"


def test_db_reads_content_calendar():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE content_calendar (id TEXT, date TEXT, theme TEXT, category TEXT, status TEXT)")
    for row_id in ("a", "b", "c"):
        conn.execute("INSERT INTO content_calendar VALUES (?, '2026-06-01T00:00:00+00:00', 'Launch', 'ops', 'planned')", (row_id,))
    report = build_content_calendar_topic_concentration_report_from_db(conn, max_topic_items=2, now=NOW)
    assert report["issues"][0]["topic"] == "launch"
