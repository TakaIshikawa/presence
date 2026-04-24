"""Tests for the manual content idea inbox."""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from content_ideas import (
    cmd_add,
    cmd_dismiss,
    cmd_list,
    cmd_promote,
    cmd_snooze,
    cmd_unsnooze,
)


def test_add_and_list_content_ideas_orders_high_priority_first(db):
    low_id = db.add_content_idea(
        "Maybe write about small cleanup chores",
        topic="maintenance",
        priority="low",
        source="manual",
    )
    high_id = db.add_content_idea(
        "Show how the validation command changed the implementation plan",
        topic="testing",
        priority="high",
        source="notes",
    )

    ideas = db.get_content_ideas(status="open")

    assert [idea["id"] for idea in ideas] == [high_id, low_id]
    assert ideas[0]["topic"] == "testing"
    assert ideas[0]["status"] == "open"


def test_promote_and_dismiss_update_status(db):
    promote_id = db.add_content_idea("Promote this", topic="testing", priority="high")
    dismiss_id = db.add_content_idea("Dismiss this", priority="high")

    planned_id = db.promote_content_idea(promote_id, target_date="2026-05-01")
    db.dismiss_content_idea(dismiss_id)

    planned = db.conn.execute(
        "SELECT topic, target_date, source_material FROM planned_topics WHERE id = ?",
        (planned_id,),
    ).fetchone()

    assert planned["topic"] == "testing"
    assert planned["target_date"] == "2026-05-01"
    assert json.loads(planned["source_material"]) == {
        "content_idea_id": promote_id,
        "note": "Promote this",
        "source": None,
    }
    assert db.get_content_idea(promote_id)["status"] == "promoted"
    assert db.get_content_idea(dismiss_id)["status"] == "dismissed"
    assert db.get_content_ideas(status="open") == []


def test_content_idea_validation(db):
    with pytest.raises(ValueError, match="note is required"):
        db.add_content_idea(" ")
    with pytest.raises(ValueError, match="priority"):
        db.add_content_idea("Valid note", priority="urgent")
    with pytest.raises(ValueError, match="does not exist"):
        db.dismiss_content_idea(999)
    with pytest.raises(ValueError, match="topic is required"):
        idea_id = db.add_content_idea("Valid note")
        db.promote_content_idea(idea_id, target_date="2026-05-01")


def test_promote_content_idea_rejects_closed_ideas_unless_forced(db):
    dismissed_id = db.add_content_idea("Dismissed idea", topic="testing")
    promoted_id = db.add_content_idea("Promoted idea", topic="testing")

    db.dismiss_content_idea(dismissed_id)
    db.promote_content_idea(promoted_id, target_date="2026-05-01")

    with pytest.raises(ValueError, match="dismissed"):
        db.promote_content_idea(dismissed_id, target_date="2026-05-02")
    with pytest.raises(ValueError, match="promoted"):
        db.promote_content_idea(promoted_id, target_date="2026-05-03")

    forced_id = db.promote_content_idea(
        dismissed_id,
        target_date="2026-05-04",
        topic="architecture",
        angle="reopen the idea",
        force=True,
    )
    forced = db.conn.execute(
        "SELECT topic, angle, target_date FROM planned_topics WHERE id = ?",
        (forced_id,),
    ).fetchone()

    assert forced["topic"] == "architecture"
    assert forced["angle"] == "reopen the idea"
    assert forced["target_date"] == "2026-05-04"
    assert db.get_content_idea(dismissed_id)["status"] == "promoted"


def test_content_ideas_cli_helpers_print_expected_output(db, capsys):
    idea_id = cmd_add(
        db,
        "A concrete note about turning flaky tests into a generation lesson",
        topic="testing",
        priority="high",
        source="scratchpad",
    )

    output = capsys.readouterr().out
    assert f"Added content idea {idea_id}" in output

    listed = cmd_list(db, priority="high")
    output = capsys.readouterr().out
    assert listed[0]["id"] == idea_id
    assert "testing" in output
    assert "flaky tests" in output

    planned_id = cmd_promote(
        db,
        idea_id,
        target_date="2026-05-01",
        campaign_id=None,
        topic=None,
        angle="what flaky tests reveal",
    )
    output = capsys.readouterr().out
    assert f"planned topic {planned_id}" in output
    assert db.get_content_idea(idea_id)["status"] == "promoted"
    planned = db.conn.execute(
        "SELECT topic, angle, source_material FROM planned_topics WHERE id = ?",
        (planned_id,),
    ).fetchone()
    assert planned["topic"] == "testing"
    assert planned["angle"] == "what flaky tests reveal"
    assert json.loads(planned["source_material"])["source"] == "scratchpad"

    cmd_dismiss(db, idea_id)
    output = capsys.readouterr().out
    assert f"Dismissed content idea {idea_id}" in output
    assert db.get_content_idea(idea_id)["status"] == "dismissed"


def test_cmd_snooze_unsnooze_and_list_include_snoozed(db, capsys):
    idea_id = cmd_add(
        db,
        "A useful idea that should wait for the right week",
        topic="planning",
        priority="high",
    )
    future = (datetime.now(timezone.utc) + timedelta(days=7)).date().isoformat()
    capsys.readouterr()

    cmd_snooze(db, idea_id, snoozed_until=future, reason="wait for launch")
    output = capsys.readouterr().out

    assert f"Snoozed content idea {idea_id} until {future}" in output
    assert db.get_content_idea(idea_id)["snooze_reason"] == "wait for launch"

    listed = cmd_list(db, priority="high")
    output = capsys.readouterr().out
    assert listed == []
    assert "No content ideas" in output

    listed = cmd_list(db, priority="high", include_snoozed=True)
    output = capsys.readouterr().out
    assert listed[0]["id"] == idea_id
    assert "snoozed" in output
    assert future in output

    listed = cmd_list(db, status="open", priority="high", snoozed_only=True)
    output = capsys.readouterr().out
    assert listed[0]["id"] == idea_id
    assert "useful idea" in output

    cmd_unsnooze(db, idea_id)
    output = capsys.readouterr().out
    assert f"Unsnoozed content idea {idea_id}" in output
    assert db.get_content_idea(idea_id)["snoozed_until"] is None
    assert cmd_list(db, priority="high")[0]["id"] == idea_id


def test_cmd_add_skips_duplicate_unless_forced(db, capsys):
    existing_id = cmd_add(
        db,
        "  Turn duplicate seeds into one durable idea. ",
        topic="Testing",
        source="manual",
    )
    capsys.readouterr()

    duplicate_id = cmd_add(
        db,
        "Turn duplicate seeds into one durable idea.",
        topic="testing",
        source="manual",
    )
    output = capsys.readouterr().out

    assert duplicate_id == existing_id
    assert "Skipped duplicate content idea" in output
    assert len(db.get_content_ideas(status="open")) == 1

    forced_id = cmd_add(
        db,
        "Turn duplicate seeds into one durable idea.",
        topic="testing",
        source="manual",
        force=True,
    )
    output = capsys.readouterr().out

    assert forced_id != existing_id
    assert "Warning: similar content idea" in output
    assert len(db.get_content_ideas(status="open")) == 2


def test_cmd_promote_skips_similar_active_idea_unless_forced(db, capsys):
    first_id = db.add_content_idea("First note", topic="testing")
    second_id = db.add_content_idea("Second note", topic="TESTING")

    skipped = cmd_promote(db, first_id, target_date="2026-05-01")
    output = capsys.readouterr().out

    assert skipped is None
    assert "Skipped promoting content idea" in output
    assert db.get_content_idea(first_id)["status"] == "open"

    planned_id = cmd_promote(
        db,
        first_id,
        target_date="2026-05-01",
        force=True,
    )
    output = capsys.readouterr().out

    assert planned_id is not None
    assert "Warning: promoting despite similar content idea" in output
    assert db.get_content_idea(first_id)["status"] == "promoted"
    assert db.get_content_idea(second_id)["status"] == "open"

def test_content_idea_aging_action_merges_source_metadata(db):
    idea_id = db.add_content_idea(
        "Keep existing metadata",
        topic="testing",
        source_metadata={"source": "manual", "nested": {"keep": True}},
    )

    db.apply_content_idea_aging_action(
        idea_id,
        action={
            "source": "content_idea_aging",
            "action": "promote_priority",
            "reason": "old enough to promote",
        },
        priority="high",
        updated_at="2026-05-01T12:00:00+00:00",
    )

    idea = db.get_content_idea(idea_id)
    metadata = json.loads(idea["source_metadata"])

    assert idea["priority"] == "high"
    assert idea["updated_at"] == "2026-05-01T12:00:00+00:00"
    assert metadata["source"] == "manual"
    assert metadata["nested"] == {"keep": True}
    assert metadata["aging_actions"] == [
        {
            "source": "content_idea_aging",
            "action": "promote_priority",
            "reason": "old enough to promote",
        }
    ]


def test_content_idea_duplicate_detection_matches_source_identity_metadata(db):
    first_id = db.add_content_idea(
        "Resurface testing because it has gone dormant",
        topic="testing",
        source="stale_topic_resurfacer",
        source_metadata={
            "source": "stale_topic_resurfacer",
            "source_id": "stale-topic:testing",
            "source_content_ids": [1, 2],
        },
    )

    matches = db.find_similar_content_ideas(
        note="A different note for the same stale topic",
        topic="testing",
        source="stale_topic_resurfacer",
        source_metadata={
            "source": "stale_topic_resurfacer",
            "source_id": "stale-topic:testing",
            "source_content_ids": [2, 3],
        },
    )

    assert matches[0]["id"] == first_id
    assert "source_metadata.source_id" in matches[0]["duplicate_reasons"]
