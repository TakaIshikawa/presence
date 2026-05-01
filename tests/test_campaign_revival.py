"""Tests for dormant campaign revival planning."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.campaign_revival import (
    build_campaign_revival_report,
    format_campaign_revival_json,
    format_campaign_revival_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "campaign_revival.py"
spec = importlib.util.spec_from_file_location("campaign_revival_script", SCRIPT_PATH)
campaign_revival_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(campaign_revival_script)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _campaign(db, name: str, *, status: str = "active") -> int:
    return db.insert_content_campaign(
        name=name,
        goal=f"{name} goal",
        start_date="2026-04-01",
        status=status,
    )


def _content(db, created_at: str, *, topic: str | None = None) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=f"{topic or 'general'} content",
        eval_score=7.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created_at, content_id),
    )
    if topic:
        db.insert_content_topics(content_id, [(topic, "", 1.0)])
    db.conn.commit()
    return content_id


def _link_content(db, planned_topic_id: int, content_id: int) -> None:
    db.conn.execute(
        "UPDATE planned_topics SET status = 'generated', content_id = ? WHERE id = ?",
        (content_id, planned_topic_id),
    )
    db.conn.commit()


def test_classifies_healthy_ready_dormant_and_stalled_campaigns(db):
    healthy_id = _campaign(db, "Healthy")
    healthy_topic = db.insert_planned_topic(
        "testing",
        "recent work",
        campaign_id=healthy_id,
        status="generated",
    )
    _link_content(
        db,
        healthy_topic,
        _content(db, "2026-04-30T12:00:00+00:00", topic="testing"),
    )

    revive_id = _campaign(db, "Revive")
    revive_done = db.insert_planned_topic(
        "architecture",
        "old lesson",
        campaign_id=revive_id,
        status="generated",
    )
    _link_content(
        db,
        revive_done,
        _content(db, "2026-04-01T12:00:00+00:00", topic="architecture"),
    )
    next_topic = db.insert_planned_topic(
        "architecture",
        "next angle",
        target_date="2026-05-02",
        campaign_id=revive_id,
    )

    dormant_id = _campaign(db, "Dormant")
    dormant_done = db.insert_planned_topic(
        "release",
        "wrapup",
        campaign_id=dormant_id,
        status="generated",
    )
    _link_content(
        db,
        dormant_done,
        _content(db, "2026-03-15T12:00:00+00:00", topic="release"),
    )
    db.insert_content_idea(
        note="Dormant follow-up idea",
        topic="release",
        status="open",
    )

    stalled_id = _campaign(db, "Stalled", status="paused")
    stalled_done = db.insert_planned_topic(
        "ops",
        "done",
        campaign_id=stalled_id,
        status="generated",
    )
    _link_content(
        db,
        stalled_done,
        _content(db, "2026-03-01T12:00:00+00:00", topic="ops"),
    )

    report = build_campaign_revival_report(db, days_idle=14, now=NOW)
    by_id = {item.campaign_id: item for item in report.recommendations}

    assert by_id[healthy_id].status == "healthy"
    assert by_id[healthy_id].next_action == "continue the current campaign cadence"
    assert by_id[revive_id].status == "ready_to_revive"
    assert by_id[revive_id].next_planned_topic_id == next_topic
    assert "generate planned_topic" in by_id[revive_id].next_action
    assert by_id[dormant_id].status == "dormant"
    assert "open ideas exist" in by_id[dormant_id].reason
    assert by_id[stalled_id].status == "stalled"
    assert "seed a new content idea" in by_id[stalled_id].next_action
    assert report.totals == {
        "campaigns": 4,
        "healthy": 1,
        "dormant": 1,
        "stalled": 1,
        "ready_to_revive": 1,
        "missing_tables": 0,
    }


def test_uses_content_topics_as_activity_signal_for_unlinked_campaign_content(db):
    campaign_id = _campaign(db, "Topic-linked")
    db.insert_planned_topic(
        "observability",
        "queued angle",
        campaign_id=campaign_id,
    )
    _content(db, "2026-04-29T12:00:00+00:00", topic="observability")

    report = build_campaign_revival_report(db, days_idle=14, now=NOW)
    item = report.recommendations[0]

    assert item.status == "healthy"
    assert item.last_generated_at == "2026-04-29T12:00:00+00:00"


def test_json_output_is_deterministic_and_contains_required_recommendation_fields(db):
    campaign_id = _campaign(db, "Revival")
    topic_id = db.insert_planned_topic(
        "testing",
        "next proof",
        campaign_id=campaign_id,
    )

    report = build_campaign_revival_report(db, days_idle=10, now=NOW)
    first = format_campaign_revival_json(report)
    second = format_campaign_revival_json(report)

    assert first == second
    payload = json.loads(first)
    assert payload["filters"]["days_idle"] == 10
    assert payload["recommendations"] == [
        {
            "campaign_id": campaign_id,
            "campaign_name": "Revival",
            "campaign_status": "active",
            "generated_topic_count": 0,
            "idle_days": None,
            "last_generated_at": None,
            "next_action": f"generate planned_topic #{topic_id}: testing (next proof)",
            "next_planned_topic": "testing",
            "next_planned_topic_id": topic_id,
            "open_idea_count": 0,
            "planned_topic_count": 1,
            "reason": "campaign has not generated content yet but still has planned topics",
            "remaining_planned_topic_count": 1,
            "status": "ready_to_revive",
        }
    ]


def test_text_output_summarizes_actions(db):
    campaign_id = _campaign(db, "Recap")
    done_topic = db.insert_planned_topic(
        "launch",
        "done",
        campaign_id=campaign_id,
        status="generated",
    )
    _link_content(
        db,
        done_topic,
        _content(db, "2026-03-01T12:00:00+00:00", topic="launch"),
    )

    text = format_campaign_revival_text(
        build_campaign_revival_report(db, days_idle=14, now=NOW)
    )

    assert "Campaign Revival Planner" in text
    assert "Summary: campaigns=1 healthy=0 dormant=1 stalled=0 ready_to_revive=0" in text
    assert f"campaign_id={campaign_id} Recap [dormant]" in text
    assert "schedule a campaign recap" in text


def test_cli_outputs_json_with_filters(db, capsys):
    campaign_id = _campaign(db, "CLI")
    db.insert_planned_topic("testing", campaign_id=campaign_id)

    with patch.object(
        campaign_revival_script,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        campaign_revival_script,
        "build_campaign_revival_report",
        wraps=lambda db, **kwargs: build_campaign_revival_report(
            db,
            now=NOW,
            **kwargs,
        ),
    ):
        assert campaign_revival_script.main(
            ["--days-idle", "7", "--campaign-id", str(campaign_id), "--json"]
        ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["days_idle"] == 7
    assert payload["filters"]["campaign_id"] == campaign_id
    assert payload["recommendations"][0]["campaign_id"] == campaign_id


def test_missing_required_tables_return_empty_report():
    conn = sqlite3.connect(":memory:")
    try:
        report = build_campaign_revival_report(conn, days_idle=14, now=NOW)
    finally:
        conn.close()

    assert report.recommendations == ()
    assert "content_campaigns" in report.missing_tables
    assert "No active or paused campaigns found" in format_campaign_revival_text(report)


def test_rejects_invalid_filters(db):
    try:
        build_campaign_revival_report(db, days_idle=0, now=NOW)
    except ValueError as exc:
        assert "days-idle must be positive" in str(exc)
    else:
        raise AssertionError("expected ValueError")
