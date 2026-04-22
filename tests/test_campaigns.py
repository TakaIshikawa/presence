"""Tests for campaign YAML import/export."""

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from campaigns import dump_campaign_yaml, export_campaigns, import_campaigns


def test_import_campaigns_is_idempotent(db):
    data = {
        "campaigns": [
            {
                "name": "Testing Foundations",
                "goal": "Build a testing arc",
                "start_date": "2026-05-01",
                "end_date": "2026-05-15",
                "status": "planned",
                "planned_topics": [
                    {
                        "topic": "testing",
                        "angle": "property-based tests",
                        "target_date": "2026-05-03",
                    },
                    {
                        "topic": "architecture",
                        "angle": "service boundaries",
                        "target_date": "2026-05-05",
                    },
                ],
            }
        ],
        "planned_topics": [
            {
                "topic": "debugging",
                "angle": "triage workflow",
                "target_date": "2026-05-10",
            }
        ],
    }

    first_changes = import_campaigns(db, data)
    second_changes = import_campaigns(db, data)

    assert first_changes == [
        "create campaign: Testing Foundations",
        "create planned topic: testing @ 2026-05-03 (campaign=Testing Foundations)",
        "create planned topic: architecture @ 2026-05-05 (campaign=Testing Foundations)",
        "create planned topic: debugging @ 2026-05-10 (no campaign)",
    ]
    assert second_changes == []

    campaigns = db.get_campaigns()
    planned = db.get_planned_topics(status="planned")
    assert len(campaigns) == 1
    assert len(planned) == 3
    assert {topic["topic"] for topic in planned} == {"testing", "architecture", "debugging"}


def test_import_campaigns_dry_run_prints_changes_without_writing(db, capsys):
    data = {
        "campaigns": [
            {
                "name": "Launch Arc",
                "goal": "Explain launch lessons",
                "status": "active",
                "planned_topics": [
                    {
                        "topic": "product-thinking",
                        "angle": "launch checklist",
                        "target_date": "2026-06-01",
                    }
                ],
            }
        ]
    }

    changes = import_campaigns(db, data, dry_run=True)

    output = capsys.readouterr().out
    assert "create campaign: Launch Arc" in output
    assert "create planned topic: product-thinking @ 2026-06-01" in output
    assert changes
    assert db.get_campaigns() == []
    assert db.get_planned_topics(status="planned") == []


def test_export_campaigns_round_trip_shape(db):
    active_id = db.create_campaign(
        name="Launch Arc",
        goal="Explain launch lessons",
        start_date="2026-06-01",
        end_date="2026-06-30",
        status="active",
    )
    db.create_campaign(name="Completed Arc", status="completed")
    db.insert_planned_topic(
        topic="product-thinking",
        angle="launch checklist",
        target_date="2026-06-03",
        source_material="abc123",
        campaign_id=active_id,
    )
    db.insert_planned_topic(topic="testing", target_date="2026-06-04", campaign_id=active_id, status="generated")
    db.insert_planned_topic(topic="debugging", angle="incident review", target_date="2026-06-05")

    exported = export_campaigns(db)
    rendered = dump_campaign_yaml(exported)
    loaded = yaml.safe_load(rendered)

    assert loaded == {
        "campaigns": [
            {
                "name": "Launch Arc",
                "goal": "Explain launch lessons",
                "start_date": "2026-06-01",
                "end_date": "2026-06-30",
                "status": "active",
                "planned_topics": [
                    {
                        "topic": "product-thinking",
                        "angle": "launch checklist",
                        "target_date": "2026-06-03",
                        "source_material": "abc123",
                        "status": "planned",
                    }
                ],
            }
        ],
        "planned_topics": [
            {
                "topic": "debugging",
                "angle": "incident review",
                "target_date": "2026-06-05",
                "source_material": None,
                "status": "planned",
            }
        ],
    }
