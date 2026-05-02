"""Tests for campaign handoff packet export."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.campaign_handoff_packet import (
    STATUS_NOT_FOUND,
    STATUS_OK,
    build_campaign_handoff_packet,
    format_campaign_handoff_packet_json,
    format_campaign_handoff_packet_text,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "export_campaign_handoff_packet.py"
)
spec = importlib.util.spec_from_file_location("export_campaign_handoff_packet", SCRIPT_PATH)
export_campaign_handoff_packet = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(export_campaign_handoff_packet)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _campaign(db, name: str = "Launch Arc") -> int:
    return db.create_campaign(
        name=name,
        goal="Review the launch content system",
        start_date="2026-05-01",
        end_date="2026-05-31",
        status="active",
    )


def _content(db, content: str = "Launch review content with evidence.") -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=["abc123"],
        source_messages=[],
        content=content,
        eval_score=8.5,
        eval_feedback="ready",
        claim_check_summary={
            "supported_count": 2,
            "unsupported_count": 0,
            "annotation_text": "supported",
        },
    )


def test_packet_includes_campaign_topics_content_publication_evidence_and_engagement(db):
    campaign_id = _campaign(db)
    content_id = _content(db)
    ready_topic_id = db.insert_planned_topic(
        topic="launch",
        angle="handoff packet",
        target_date="2026-05-03",
        source_material=json.dumps({"commits": ["abc123"]}),
        campaign_id=campaign_id,
        status="generated",
    )
    db.conn.execute(
        "UPDATE planned_topics SET content_id = ? WHERE id = ?",
        (content_id, ready_topic_id),
    )
    db.insert_planned_topic(
        topic="follow-up",
        angle="manual review",
        target_date="2026-05-04",
        campaign_id=campaign_id,
        status="planned",
    )
    db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (
            content_id,
            "2026-05-03T10:00:00+00:00",
            "x",
            "queued",
            "2026-05-01T09:00:00+00:00",
        ),
    )
    db.upsert_publication_success(
        content_id,
        "bluesky",
        platform_post_id="at://post/1",
        platform_url="https://bsky.app/profile/test/post/1",
        published_at="2026-05-03T11:00:00+00:00",
    )
    db.insert_engagement(content_id, "tweet-1", 5, 1, 2, 0, 12.5)
    db.insert_linkedin_engagement(
        content_id,
        linkedin_url="https://linkedin.example/post",
        engagement_score=8.0,
        fetched_at="2026-05-03T13:00:00+00:00",
    )
    db.conn.commit()

    packet = build_campaign_handoff_packet(db, campaign_id=campaign_id, now=NOW)

    assert packet.status == STATUS_OK
    assert packet.campaign["name"] == "Launch Arc"
    assert packet.planned_topic_status_counts == {
        "generated": 1,
        "planned": 1,
        "total": 2,
    }
    assert [topic["planned_topic_id"] for topic in packet.planned_topics] == [
        ready_topic_id,
        ready_topic_id + 1,
    ]
    assert packet.planned_topics[0]["evidence_status"] == "ready"
    assert packet.planned_topics[1]["evidence_status"] == "missing"
    assert packet.publish_queue_state["summary"]["queued"] == 1
    assert packet.publish_queue_state["summary"]["published"] == 1
    assert packet.evidence_readiness == {
        "ready": 1,
        "thin": 0,
        "missing": 1,
        "unavailable": 0,
        "topic_count": 2,
    }
    assert packet.engagement_summaries["summary"]["snapshot_count"] == 2
    assert packet.engagement_summaries["summary"]["platform_counts"] == {
        "linkedin": 1,
        "x": 1,
    }
    assert packet.generated_content[0]["publication_states"][0]["platform"] == "bluesky"
    assert packet.generated_content[0]["queue_items"][0]["status"] == "queued"


def test_campaign_filter_resolves_slugified_name_and_missing_campaign_is_status(db):
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute(
            """CREATE TABLE content_campaigns (
               id INTEGER PRIMARY KEY,
               name TEXT NOT NULL,
               status TEXT,
               created_at TEXT
            )"""
        )
        conn.execute(
            """CREATE TABLE planned_topics (
               id INTEGER PRIMARY KEY,
               campaign_id INTEGER,
               topic TEXT,
               status TEXT,
               content_id INTEGER,
               created_at TEXT
            )"""
        )
        conn.execute(
            """INSERT INTO content_campaigns (id, name, status, created_at)
               VALUES (7, 'Launch Arc', 'active', '2026-05-01')"""
        )
        conn.execute(
            """INSERT INTO planned_topics
               (id, campaign_id, topic, status, content_id, created_at)
               VALUES (9, 7, 'launch', 'planned', NULL, '2026-05-01')"""
        )
        conn.commit()

        packet = build_campaign_handoff_packet(conn, campaign="launch-arc", now=NOW)
        missing = build_campaign_handoff_packet(conn, campaign="missing", now=NOW)
    finally:
        conn.close()

    assert packet.status == STATUS_OK
    assert packet.campaign["id"] == 7
    assert packet.availability["publish_queue"] is False
    assert "publish_queue" in packet.missing_tables
    assert packet.planned_topics[0]["planned_topic_id"] == 9
    assert missing.status == STATUS_NOT_FOUND
    assert missing.message == "Campaign 'missing' not found"


def test_packet_is_schema_tolerant_when_optional_tables_or_columns_are_missing():
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE content_campaigns (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute(
            """CREATE TABLE planned_topics (
               id INTEGER PRIMARY KEY,
               campaign_id INTEGER,
               topic TEXT,
               status TEXT,
               content_id INTEGER
            )"""
        )
        conn.execute("CREATE TABLE publish_queue (id INTEGER PRIMARY KEY, status TEXT)")
        conn.execute("INSERT INTO content_campaigns (id, name) VALUES (1, 'Sparse')")
        conn.execute(
            """INSERT INTO planned_topics
               (id, campaign_id, topic, status, content_id)
               VALUES (2, 1, 'sparse', 'planned', NULL)"""
        )
        conn.commit()

        packet = build_campaign_handoff_packet(conn, campaign_id=1, now=NOW)
    finally:
        conn.close()

    assert packet.status == STATUS_OK
    assert packet.generated_content == ()
    assert packet.planned_topics[0]["evidence_status"] == "missing"
    assert "generated_content" in packet.missing_tables
    assert packet.missing_columns["publish_queue"] == ("content_id", "platform")


def test_json_text_and_cli_output_are_deterministic(db, capsys, tmp_path):
    campaign_id = _campaign(db, "CLI Packet")
    content_id = _content(db, "CLI packet content")
    topic_id = db.insert_planned_topic(
        topic="cli",
        target_date="2026-05-03",
        campaign_id=campaign_id,
        status="generated",
    )
    db.conn.execute(
        "UPDATE planned_topics SET content_id = ? WHERE id = ?",
        (content_id, topic_id),
    )
    db.conn.commit()

    packet = build_campaign_handoff_packet(db, campaign="cli-packet", now=NOW)

    assert format_campaign_handoff_packet_json(packet) == format_campaign_handoff_packet_json(packet)
    payload = json.loads(format_campaign_handoff_packet_json(packet))
    assert sorted(payload) == [
        "artifact_type",
        "availability",
        "campaign",
        "engagement_summaries",
        "evidence_readiness",
        "filters",
        "generated_at",
        "generated_content",
        "matches",
        "message",
        "missing_columns",
        "missing_tables",
        "planned_topic_status_counts",
        "planned_topics",
        "publish_queue_state",
        "status",
    ]
    text = format_campaign_handoff_packet_text(packet)
    assert "Campaign Handoff Packet" in text
    assert "Campaign: #" in text
    assert "Evidence: ready=1" in text

    output_path = tmp_path / "packet.json"
    with patch.object(
        export_campaign_handoff_packet,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        export_campaign_handoff_packet,
        "build_campaign_handoff_packet",
        wraps=lambda db, **kwargs: build_campaign_handoff_packet(db, now=NOW, **kwargs),
    ):
        assert (
            export_campaign_handoff_packet.main(
                [
                    "--campaign",
                    "cli-packet",
                    "--format",
                    "json",
                    "--output",
                    str(output_path),
                ]
            )
            == 0
        )

    assert capsys.readouterr().out == ""
    cli_payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert cli_payload["campaign"]["name"] == "CLI Packet"
    assert cli_payload["planned_topics"][0]["planned_topic_id"] == topic_id


def test_cli_returns_clear_not_found_status(db, capsys):
    with patch.object(
        export_campaign_handoff_packet,
        "script_context",
        wraps=lambda: _script_context(db),
    ):
        assert (
            export_campaign_handoff_packet.main(
                ["--campaign", "missing-campaign", "--format", "json"]
            )
            == 1
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == STATUS_NOT_FOUND
    assert payload["message"] == "Campaign 'missing-campaign' not found"
