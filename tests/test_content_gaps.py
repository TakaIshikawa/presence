"""Tests for content gap detection."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from content_gaps import format_json_report, main
from synthesis.content_gaps import ContentGapDetector, classify_source_topics


def _add_generated(db, topic: str, published_at: str, content: str = "generated post") -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="good",
    )
    db.conn.execute(
        "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
        (published_at, content_id),
    )
    db.conn.commit()
    db.insert_content_topics(content_id, [(topic, "", 1.0)])
    return content_id


def seed_content_gap_data(db) -> int:
    campaign_id = db.create_campaign(
        name="April Engineering",
        goal="Cover current engineering work",
        start_date="2026-04-01",
        end_date="2026-04-30",
        status="active",
    )
    other_campaign_id = db.create_campaign(
        name="Other",
        start_date="2026-04-01",
        end_date="2026-04-30",
        status="active",
    )

    db.insert_planned_topic(
        topic="testing",
        angle="fixture cleanup",
        target_date="2026-04-20",
        campaign_id=campaign_id,
    )
    db.insert_planned_topic(
        topic="architecture",
        angle="module boundary write-up",
        target_date="2026-04-22",
        campaign_id=campaign_id,
    )
    db.insert_planned_topic(
        topic="developer-tools",
        angle="CLI ergonomics",
        target_date="2026-04-22",
        campaign_id=other_campaign_id,
    )

    _add_generated(db, "testing", "2026-04-20T09:00:00+00:00", "Post about test fixtures")
    _add_generated(db, "testing", "2026-04-21T09:00:00+00:00", "Post about pytest coverage")
    _add_generated(db, "testing", "2026-04-22T09:00:00+00:00", "Post about assertion style")
    _add_generated(db, "debugging", "2026-04-22T10:00:00+00:00", "Post about debugging")

    db.insert_commit(
        "presence",
        "sha-perf-1",
        "perf: optimize cache latency in source scoring",
        "2026-04-21T12:00:00+00:00",
        "taka",
    )
    db.insert_commit(
        "presence",
        "sha-perf-2",
        "fix: reduce slow throughput path in generation",
        "2026-04-22T12:00:00+00:00",
        "taka",
    )
    db.insert_claude_message(
        "sess-perf",
        "msg-perf-1",
        "/repo",
        "2026-04-22T13:00:00+00:00",
        "Investigate performance and cache behavior for generated content",
    )
    db.insert_claude_message(
        "sess-testing",
        "msg-test-1",
        "/repo",
        "2026-04-22T14:00:00+00:00",
        "Add pytest coverage for the content gap detector",
    )

    return campaign_id


def test_classify_source_topics_is_deterministic():
    assert classify_source_topics("perf: optimize cache latency") == ["performance"]
    assert "testing" in classify_source_topics("Add pytest fixtures and coverage")


def test_detects_planned_overused_and_source_rich_gaps(db):
    campaign_id = seed_content_gap_data(db)

    report = ContentGapDetector(db).detect(
        days=7,
        campaign_id=campaign_id,
        target_date=datetime(2026, 4, 23, tzinfo=timezone.utc),
    )

    assert [gap.topic for gap in report.planned_gaps] == ["architecture"]
    assert report.planned_gaps[0].campaign_id == campaign_id
    assert report.planned_gaps[0].nearest_generated_at is None

    assert [(topic.topic, topic.count) for topic in report.overused_topics] == [("testing", 3)]

    source_gap_topics = {gap.topic: gap for gap in report.source_rich_gaps}
    assert "performance" in source_gap_topics
    assert source_gap_topics["performance"].source_count == 3
    assert source_gap_topics["performance"].commit_count == 2
    assert source_gap_topics["performance"].message_count == 1
    assert "testing" not in source_gap_topics


def test_campaign_filter_only_applies_to_planned_gaps(db):
    seed_content_gap_data(db)
    other_campaign = db.get_campaign_by_name("Other")

    report = ContentGapDetector(db).detect(
        days=7,
        campaign_id=other_campaign["id"],
        target_date=datetime(2026, 4, 23, tzinfo=timezone.utc),
    )

    assert [gap.topic for gap in report.planned_gaps] == ["developer-tools"]
    assert [topic.topic for topic in report.overused_topics] == ["testing"]


def test_json_format_is_machine_readable(db):
    campaign_id = seed_content_gap_data(db)
    report = ContentGapDetector(db).detect(
        days=7,
        campaign_id=campaign_id,
        target_date=datetime(2026, 4, 23, tzinfo=timezone.utc),
    )

    payload = json.loads(format_json_report(report))

    assert payload["campaign_id"] == campaign_id
    assert payload["planned_gaps"][0]["topic"] == "architecture"
    assert payload["source_rich_gaps"][0]["topic"] == "performance"


def test_main_json_output(db, capsys):
    campaign_id = seed_content_gap_data(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("content_gaps.script_context", fake_script_context):
        main(["--days", "7", "--campaign-id", str(campaign_id), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["campaign_id"] == campaign_id
    assert "planned_gaps" in payload
    assert "overused_topics" in payload
    assert "source_rich_gaps" in payload
