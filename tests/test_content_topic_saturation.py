from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json

from evaluation.content_topic_saturation import (
    build_content_topic_saturation_report,
    format_content_topic_saturation_json,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)


def _content(db, *, content_type="x_post", published_days_ago=1):
    db.conn.execute(
        """INSERT INTO generated_content (content_type, content, published_at, created_at)
           VALUES (?, ?, ?, ?)""",
        (
            content_type,
            "content",
            (NOW - timedelta(days=published_days_ago)).isoformat(),
            (NOW - timedelta(days=published_days_ago)).isoformat(),
        ),
    )
    db.conn.commit()
    return db.conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _topic(db, content_id, topic="testing", subtopic="unit", confidence=0.9):
    db.conn.execute(
        """INSERT INTO content_topics (content_id, topic, subtopic, confidence, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (content_id, topic, subtopic, confidence, NOW.isoformat()),
    )
    db.conn.commit()


def test_report_returns_topic_groups_findings_and_totals(db):
    ids = [_content(db, published_days_ago=days) for days in (1, 2, 3)]
    for content_id in ids:
        _topic(db, content_id, topic="testing", subtopic="unit", confidence=0.8)
    stale_id = _content(db, published_days_ago=60)
    _topic(db, stale_id, topic="legacy", subtopic="old", confidence=0.95)
    _topic(db, ids[0], topic="ai", subtopic="agents", confidence=0.2)
    _topic(db, 9999, topic="orphan", subtopic="", confidence=0.9)

    payload = json.loads(format_content_topic_saturation_json(
        build_content_topic_saturation_report(db, overused_topic_count=3, stale_after_days=30, now=NOW)
    ))

    assert payload["artifact_type"] == "content_topic_saturation"
    assert payload["totals"]["topic_group_count"] == 4
    by_topic = {(group["topic"], group["subtopic"]): group for group in payload["topic_groups"]}
    assert by_topic[("testing", "unit")]["content_count"] == 3
    assert by_topic[("testing", "unit")]["representative_content_ids"] == ids
    finding_types = {finding["finding_type"] for finding in payload["findings"]}
    assert {"overused_topic", "stale_topic", "low_confidence_topic", "orphan_topic_assignment"}.issubset(finding_types)


def test_missing_content_topics_table_is_reported():
    import sqlite3

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_content_topic_saturation_report(conn, now=NOW)

    assert report["missing_tables"] == ["content_topics"]
    assert report["totals"]["topic_assignment_count"] == 0
