"""Tests for campaign topic scheduling."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from schedule_campaign_topics import main
from synthesis.campaign_scheduler import CampaignTopicScheduler, schedule_campaign_topics


def test_scheduler_expands_allowed_weekdays_with_topic_rotation():
    scheduler = CampaignTopicScheduler(allowed_weekdays=(0, 2, 4), max_topics_per_week=2)
    campaign = {
        "id": 7,
        "name": "Launch Arc",
        "status": "active",
        "start_date": "2026-05-01",
        "end_date": "2026-05-15",
    }

    report = scheduler.expand_campaigns(
        [campaign],
        start_date=date(2026, 5, 1),
        end_date=date(2026, 5, 15),
        topic_rotation=("testing", "architecture"),
        angles=("first", "second"),
        dry_run=True,
    )

    assert [
        (item.status, item.topic, item.angle, item.target_date)
        for item in report.items
    ] == [
        ("proposed", "testing", "first", "2026-05-01"),
        ("proposed", "architecture", "second", "2026-05-04"),
        ("proposed", "testing", "first", "2026-05-06"),
        ("proposed", "architecture", "second", "2026-05-11"),
        ("proposed", "testing", "first", "2026-05-13"),
    ]


def test_scheduler_skips_existing_campaign_topic_date():
    scheduler = CampaignTopicScheduler(allowed_weekdays=(0,), max_topics_per_week=1)
    campaign = {
        "id": 3,
        "name": "Testing Arc",
        "status": "active",
        "start_date": "2026-05-04",
        "end_date": "2026-05-18",
    }

    report = scheduler.expand_campaigns(
        [campaign],
        start_date="2026-05-04",
        end_date="2026-05-18",
        topic_rotation=("testing",),
        existing_topics=[
            {"campaign_id": 3, "topic": "testing", "target_date": "2026-05-04"}
        ],
    )

    assert [(item.status, item.target_date, item.reason) for item in report.items] == [
        ("skipped", "2026-05-04", "existing planned topic"),
        ("created", "2026-05-11", "created"),
        ("created", "2026-05-18", "created"),
    ]


def test_schedule_campaign_topics_persists_and_is_idempotent(db):
    campaign_id = db.create_campaign(
        name="Testing Foundations",
        start_date="2026-05-04",
        end_date="2026-05-22",
        weekly_limit=2,
        status="active",
    )

    first = schedule_campaign_topics(
        db,
        campaign_id=campaign_id,
        days=15,
        topic_rotation=("testing", "architecture"),
        angles=("property tests", "boundaries"),
        now=date(2026, 5, 4),
    )
    second = schedule_campaign_topics(
        db,
        campaign_id=campaign_id,
        days=15,
        topic_rotation=("testing", "architecture"),
        angles=("property tests", "boundaries"),
        now=date(2026, 5, 4),
    )

    assert len(first.created) == 5
    assert len(second.created) == 0
    assert len(second.skipped) == 5
    planned = db.get_planned_topics(status="planned")
    assert len(planned) == 5
    assert planned[0]["campaign_id"] == campaign_id
    assert planned[0]["topic"] == "testing"
    assert planned[0]["angle"] == "property tests"


def test_schedule_campaign_topics_dry_run_does_not_write(db):
    campaign_id = db.create_campaign(
        name="Dry Run Arc",
        start_date="2026-05-04",
        end_date="2026-05-08",
        status="active",
    )

    report = schedule_campaign_topics(
        db,
        campaign_id=campaign_id,
        days=5,
        max_topics_per_week=5,
        topic_rotation=("testing",),
        dry_run=True,
        now=date(2026, 5, 4),
    )

    assert len(report.proposed) == 5
    assert db.get_planned_topics(status="planned") == []


def test_schedule_campaign_topics_cli_json_serializes_report(db, capsys):
    campaign_id = db.create_campaign(
        name="CLI Arc",
        start_date="2026-04-25",
        end_date="2026-04-30",
        weekly_limit=1,
        status="active",
    )
    db.insert_planned_topic(
        topic="testing",
        angle="seed angle",
        target_date="2026-04-25",
        campaign_id=campaign_id,
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("schedule_campaign_topics.script_context", fake_script_context):
        main(["--campaign-id", str(campaign_id), "--days", "3", "--dry-run", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"] == {"created": 0, "proposed": 0, "skipped": 1}
    assert payload["skipped"][0]["topic"] == "testing"
    assert payload["skipped"][0]["target_date"] == "2026-04-25"
