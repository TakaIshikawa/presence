"""Tests for newsletter topic planning."""

from __future__ import annotations

import importlib.util
import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.newsletter_topic_planner import (
    build_newsletter_topic_plan,
    format_newsletter_topic_plan_json,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "newsletter_topic_plan.py"
)
spec = importlib.util.spec_from_file_location("newsletter_topic_plan", SCRIPT_PATH)
newsletter_topic_plan = importlib.util.module_from_spec(spec)
sys.modules["newsletter_topic_plan"] = newsletter_topic_plan
spec.loader.exec_module(newsletter_topic_plan)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _content(
    db,
    topic: str,
    *,
    created_at: str = "2026-04-30T12:00:00+00:00",
    published: int = 0,
) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=f"{topic} content",
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.insert_content_topics(content_id, [(topic, None, 1.0)])
    db.conn.execute(
        "UPDATE generated_content SET created_at = ?, published = ? WHERE id = ?",
        (created_at, published, content_id),
    )
    db.conn.commit()
    return content_id


def _send(db, content_ids: list[int], *, sent_at: str = "2026-04-29T12:00:00+00:00") -> int:
    send_id = db.insert_newsletter_send(
        issue_id=f"issue-{sent_at}",
        subject="Recent issue",
        content_ids=content_ids,
    )
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ? WHERE id = ?",
        (sent_at, send_id),
    )
    db.conn.commit()
    return send_id


def test_counts_recent_newsletter_usage_from_source_content_ids(db):
    used = _content(db, "testing")
    available = _content(db, "testing", created_at="2026-04-30T15:00:00+00:00")
    _send(db, [used])

    recommendations = build_newsletter_topic_plan(db, days=14, limit=5, now=NOW)

    testing = recommendations[0]
    assert testing.topic == "testing"
    assert testing.recent_newsletter_uses == 1
    assert testing.supporting_content_ids == (available,)
    assert used not in testing.supporting_content_ids
    assert testing.last_newsletter_sent_at == "2026-04-29T12:00:00+00:00"
    assert testing.recommendation_type == "newly-available"


def test_underused_unsent_generated_content_becomes_recommendation(db):
    content_id = _content(db, "architecture", created_at="2026-04-28T12:00:00+00:00")
    used = _content(db, "testing")
    _send(db, [used])

    recommendations = build_newsletter_topic_plan(db, days=14, limit=5, now=NOW)
    by_topic = {item.topic: item for item in recommendations}

    assert by_topic["architecture"].recommendation_type == "underused"
    assert by_topic["architecture"].supporting_content_ids == (content_id,)
    assert "has not appeared" in by_topic["architecture"].reason
    assert by_topic["architecture"].freshness_days == 3


def test_open_planned_topics_can_be_included_and_campaign_backed(db):
    campaign_id = db.create_campaign(name="Launch", status="active")
    planned_id = db.insert_planned_topic(
        topic="ai-agents",
        angle="operator lessons",
        target_date="2026-05-03",
        campaign_id=campaign_id,
    )

    without_planned = build_newsletter_topic_plan(
        db,
        days=14,
        limit=5,
        include_planned=False,
        now=NOW,
    )
    with_planned = build_newsletter_topic_plan(
        db,
        days=14,
        limit=5,
        include_planned=True,
        now=NOW,
    )

    assert without_planned == []
    assert len(with_planned) == 1
    recommendation = with_planned[0]
    assert recommendation.topic == "ai-agents"
    assert recommendation.recommendation_type == "campaign-backed"
    assert recommendation.supporting_planned_topic_ids == (planned_id,)
    assert recommendation.campaign_ids == (campaign_id,)
    assert recommendation.campaign_names == ("Launch",)
    assert recommendation.newest_planned_at == "2026-05-03"


def test_limit_and_json_output_are_stable(db):
    _content(db, "architecture")
    _content(db, "testing")

    recommendations = build_newsletter_topic_plan(db, days=14, limit=1, now=NOW)
    payload = json.loads(format_newsletter_topic_plan_json(recommendations))

    assert len(payload) == 1
    assert list(payload[0]) == sorted(payload[0])
    assert payload[0]["supporting_content_ids"]
    assert payload[0]["reason"]


def test_cli_supports_table_and_json_output(db, capsys):
    _content(db, "architecture")

    @contextmanager
    def fake_script_context():
        yield SimpleNamespace(), db

    with patch.object(
        newsletter_topic_plan,
        "script_context",
        fake_script_context,
    ):
        newsletter_topic_plan.main(["--days", "14", "--limit", "5"])

    table = capsys.readouterr().out
    assert "Newsletter Topic Plan (last 14 days)" in table
    assert "architecture" in table
    assert "support: content=" in table

    with patch.object(
        newsletter_topic_plan,
        "script_context",
        fake_script_context,
    ):
        newsletter_topic_plan.main(["--days", "14", "--limit", "5", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["topic"] == "architecture"
