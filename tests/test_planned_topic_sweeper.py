"""Tests for stale planned topic sweeping."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from sweep_planned_topics import main
from synthesis.planned_topic_sweeper import sweep_planned_topics


NOW = datetime(2026, 4, 25, 12, tzinfo=timezone.utc)


def _planned(db, topic: str, **kwargs) -> int:
    return db.insert_planned_topic(topic=topic, angle=f"{topic} angle", **kwargs)


def _topic_row(db, topic_id: int) -> dict:
    row = db.conn.execute(
        "SELECT * FROM planned_topics WHERE id = ?",
        (topic_id,),
    ).fetchone()
    return dict(row)


def test_report_lists_stale_planned_topics_without_updates(db):
    old_id = _planned(db, "old planned", target_date="2026-04-10")
    _planned(db, "recent planned", target_date="2026-04-22")

    results = sweep_planned_topics(
        db,
        older_than_days=7,
        action="report",
        now=NOW,
    )

    assert [result.to_dict() for result in results] == [
        {
            "topic_id": old_id,
            "topic": "old planned",
            "angle": "old planned angle",
            "target_date": "2026-04-10",
            "campaign_id": None,
            "action": "report",
            "status": "eligible",
            "reason": "target_date 2026-04-10 is older than cutoff 2026-04-18",
            "content_idea_id": None,
        }
    ]
    assert _topic_row(db, old_id)["status"] == "planned"


def test_skip_marks_selected_topics_skipped_and_returns_deterministic_output(db):
    old_id = _planned(db, "old planned", target_date="2026-04-01")

    results = sweep_planned_topics(
        db,
        older_than_days=14,
        action="skip",
        now=NOW,
    )

    assert [result.to_dict() for result in results] == [
        {
            "topic_id": old_id,
            "topic": "old planned",
            "angle": "old planned angle",
            "target_date": "2026-04-01",
            "campaign_id": None,
            "action": "skip",
            "status": "skipped",
            "reason": "target_date 2026-04-01 is older than cutoff 2026-04-11",
            "content_idea_id": None,
        }
    ]
    assert _topic_row(db, old_id)["status"] == "skipped"


def test_idea_creates_content_idea_with_planned_topic_metadata_and_preserved_source(db):
    source_material = json.dumps({"commit": "abc123", "notes": ["keep"]})
    old_id = _planned(
        db,
        "source topic",
        target_date="2026-03-31",
        source_material=source_material,
    )

    results = sweep_planned_topics(
        db,
        older_than_days=14,
        action="idea",
        now=NOW,
    )

    assert len(results) == 1
    assert results[0].status == "idea_created"
    idea = db.get_content_idea(results[0].content_idea_id)
    assert idea["note"] == "source topic: source topic angle"
    assert idea["topic"] == "source topic"
    assert idea["source"] == "planned_topic_sweeper"
    metadata = json.loads(idea["source_metadata"])
    assert metadata["planned_topic_id"] == old_id
    assert metadata["target_date"] == "2026-03-31"
    assert metadata["source_material"] == source_material
    assert metadata["parsed_source_material"] == {"commit": "abc123", "notes": ["keep"]}
    assert _topic_row(db, old_id)["status"] == "skipped"


def test_idea_action_avoids_duplicate_open_ideas(db):
    old_id = _planned(db, "duplicate topic", target_date="2026-04-01")
    existing_id = db.add_content_idea(
        "Existing reconsideration",
        topic="duplicate topic",
        source="planned_topic_sweeper",
        source_metadata={"planned_topic_id": old_id},
    )

    results = sweep_planned_topics(
        db,
        older_than_days=14,
        action="idea",
        now=NOW,
    )

    assert [result.to_dict() for result in results] == [
        {
            "topic_id": old_id,
            "topic": "duplicate topic",
            "angle": "duplicate topic angle",
            "target_date": "2026-04-01",
            "campaign_id": None,
            "action": "idea",
            "status": "duplicate_open_idea",
            "reason": "target_date 2026-04-01 is older than cutoff 2026-04-11",
            "content_idea_id": existing_id,
        }
    ]
    assert [idea["id"] for idea in db.get_content_ideas(status="open")] == [existing_id]
    assert _topic_row(db, old_id)["status"] == "planned"


def test_campaign_and_age_filters_can_be_combined(db):
    campaign_id = db.create_campaign(name="Launch")
    other_campaign_id = db.create_campaign(name="Other")
    scoped_id = _planned(db, "scoped", target_date="2026-04-01", campaign_id=campaign_id)
    _planned(db, "too recent", target_date="2026-04-20", campaign_id=campaign_id)
    _planned(db, "other", target_date="2026-04-01", campaign_id=other_campaign_id)
    _planned(db, "uncampaigned", target_date="2026-04-01")

    results = sweep_planned_topics(
        db,
        older_than_days=14,
        campaign_id=campaign_id,
        action="report",
        now=NOW,
    )

    assert [result.topic_id for result in results] == [scoped_id]


def test_cli_json_dry_run_does_not_update(db, capsys):
    old_id = _planned(db, "cli", target_date="2026-04-01")

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("sweep_planned_topics.script_context", fake_script_context):
        main(["--older-than-days", "7", "--action", "skip", "--dry-run", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["topic_id"] == old_id
    assert payload[0]["status"] == "dry_run"
    assert _topic_row(db, old_id)["status"] == "planned"
