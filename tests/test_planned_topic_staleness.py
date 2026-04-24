"""Tests for planned topic staleness scanning."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from planned_topic_staleness import main
from synthesis.planned_topic_staleness import (
    analyze_planned_topic_staleness,
    mark_stale_topics_skipped,
    scan_planned_topic_staleness,
)


TODAY = date(2026, 4, 25)


def _planned_rows():
    return [
        {
            "id": 1,
            "campaign_id": 10,
            "topic": "testing",
            "target_date": "2026-04-20",
            "campaign_end_date": "2026-05-01",
            "status": "planned",
            "content_id": None,
        },
        {
            "id": 2,
            "campaign_id": 10,
            "topic": "architecture",
            "target_date": "2026-04-24",
            "campaign_end_date": "2026-05-01",
            "status": "planned",
            "content_id": None,
        },
        {
            "id": 3,
            "campaign_id": 20,
            "topic": "debugging",
            "target_date": "2026-05-02",
            "campaign_end_date": "2026-04-21",
            "status": "planned",
            "content_id": None,
        },
        {
            "id": 4,
            "campaign_id": None,
            "topic": "workflow",
            "target_date": None,
            "campaign_end_date": None,
            "status": "planned",
            "content_id": None,
        },
        {
            "id": 5,
            "campaign_id": 10,
            "topic": "performance",
            "target_date": "2026-05-05",
            "campaign_end_date": "2026-05-10",
            "status": "planned",
            "content_id": None,
        },
        {
            "id": 6,
            "campaign_id": 10,
            "topic": "devops",
            "target_date": "2026-04-15",
            "campaign_end_date": "2026-05-01",
            "status": "generated",
            "content_id": 99,
        },
    ]


def test_analyzer_classifies_only_matching_stale_topics():
    stale = analyze_planned_topic_staleness(_planned_rows(), today=TODAY, days_overdue=3)

    assert [(item.topic_id, item.classification, item.recommendation) for item in stale] == [
        (1, "overdue", "reschedule"),
        (3, "campaign_ended", "skip"),
        (4, "missing_target_date", "review"),
    ]
    assert stale[0].days_overdue == 5
    assert stale[1].days_overdue == 0
    assert stale[2].days_overdue is None


def test_analyzer_filters_by_campaign_id():
    stale = analyze_planned_topic_staleness(
        _planned_rows(),
        today=TODAY,
        days_overdue=0,
        campaign_id=10,
    )

    assert [(item.topic_id, item.classification) for item in stale] == [
        (1, "overdue"),
        (2, "overdue"),
    ]


def _seed_staleness_data(db) -> dict[str, int]:
    active_campaign = db.create_campaign(
        name="Active Arc",
        start_date="2026-04-01",
        end_date="2999-12-31",
        status="active",
    )
    ended_campaign = db.create_campaign(
        name="Ended Arc",
        start_date="2000-01-01",
        end_date="2000-01-31",
        status="completed",
    )
    stale = db.insert_planned_topic(
        topic="testing",
        angle="late fixture cleanup",
        target_date="2000-01-01",
        campaign_id=active_campaign,
    )
    ended = db.insert_planned_topic(
        topic="architecture",
        angle="missed wrap-up",
        target_date="2999-01-01",
        campaign_id=ended_campaign,
    )
    missing = db.insert_planned_topic(topic="workflow", angle="needs scheduling")
    future = db.insert_planned_topic(
        topic="debugging",
        angle="future incident review",
        target_date="2999-01-01",
        campaign_id=active_campaign,
    )
    generated_content = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="already generated",
        eval_score=8.0,
        eval_feedback="good",
    )
    generated = db.insert_planned_topic(
        topic="performance",
        angle="already generated",
        target_date="2000-01-01",
        campaign_id=active_campaign,
    )
    db.mark_planned_topic_generated(generated, generated_content)
    return {
        "active_campaign": active_campaign,
        "stale": stale,
        "ended": ended,
        "missing": missing,
        "future": future,
        "generated": generated,
    }


def test_main_json_dry_run_has_no_database_side_effects(db, capsys):
    ids = _seed_staleness_data(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with (
        patch("planned_topic_staleness.script_context", fake_script_context),
        patch("planned_topic_staleness.scan_planned_topic_staleness") as scan,
    ):
        scan.side_effect = lambda database, **kwargs: scan_planned_topic_staleness(
            database,
            today=TODAY,
            **kwargs,
        )
        main(["--days-overdue", "3", "--json"])

    payload = json.loads(capsys.readouterr().out)
    stale_ids = {item["topic_id"] for item in payload["stale_topics"]}
    assert stale_ids == {ids["stale"], ids["ended"], ids["missing"]}
    assert "updates" not in payload
    assert payload["stale_topics"][0]["classification"] == "overdue"
    assert {
        row["id"]: row["status"]
        for row in db.conn.execute("SELECT id, status FROM planned_topics").fetchall()
    } == {
        ids["stale"]: "planned",
        ids["ended"]: "planned",
        ids["missing"]: "planned",
        ids["future"]: "planned",
        ids["generated"]: "generated",
    }


def test_mark_skipped_updates_only_stale_planned_topics(db):
    ids = _seed_staleness_data(db)
    stale = scan_planned_topic_staleness(db, today=TODAY, days_overdue=3)

    updates = mark_stale_topics_skipped(db, stale)

    assert {update["topic_id"] for update in updates} == {
        ids["stale"],
        ids["ended"],
        ids["missing"],
    }
    assert all(update["updated"] for update in updates)
    assert all(update["reason"] for update in updates)
    statuses = {
        row["id"]: row["status"]
        for row in db.conn.execute("SELECT id, status FROM planned_topics").fetchall()
    }
    assert statuses[ids["stale"]] == "skipped"
    assert statuses[ids["ended"]] == "skipped"
    assert statuses[ids["missing"]] == "skipped"
    assert statuses[ids["future"]] == "planned"
    assert statuses[ids["generated"]] == "generated"
