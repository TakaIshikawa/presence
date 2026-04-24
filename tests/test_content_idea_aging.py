"""Tests for age-based content idea escalation."""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from age_content_ideas import cmd_age
from synthesis.content_idea_aging import age_content_ideas


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)


def _set_created_at(db, idea_id: int, value: str) -> None:
    db.conn.execute(
        "UPDATE content_ideas SET created_at = ?, updated_at = ? WHERE id = ?",
        (value, value, idea_id),
    )
    db.conn.commit()


def test_dry_run_lists_aging_actions_without_updating(db):
    normal_id = db.add_content_idea("Escalate this", topic="testing", priority="normal")
    low_id = db.add_content_idea("Dismiss this", topic="ops", priority="low")
    fresh_id = db.add_content_idea("Leave fresh alone", topic="testing", priority="normal")
    _set_created_at(db, normal_id, "2026-04-01T12:00:00+00:00")
    _set_created_at(db, low_id, "2026-03-01T12:00:00+00:00")
    _set_created_at(db, fresh_id, "2026-05-10T12:00:00+00:00")

    actions = age_content_ideas(
        db,
        promote_after_days=30,
        dismiss_low_after_days=60,
        dry_run=True,
        now=NOW,
    )

    assert [action.idea_id for action in actions] == [low_id, normal_id]
    assert actions[0].action == "dismiss_low_priority"
    assert actions[0].age_days == 75
    assert "dismissal threshold" in actions[0].reason
    assert actions[1].action == "promote_priority"
    assert actions[1].age_days == 44
    assert db.get_content_idea(normal_id)["priority"] == "normal"
    assert db.get_content_idea(low_id)["status"] == "open"


def test_aging_promotes_normal_and_dismisses_low_priority(db):
    normal_id = db.add_content_idea(
        "Promising stale idea",
        topic="testing",
        priority="normal",
        source_metadata={"origin": "manual", "keep": True},
    )
    low_id = db.add_content_idea("Low value stale idea", topic="testing", priority="low")
    high_id = db.add_content_idea("High priority stays open", topic="testing", priority="high")
    _set_created_at(db, normal_id, "2026-04-01T12:00:00+00:00")
    _set_created_at(db, low_id, "2026-03-01T12:00:00+00:00")
    _set_created_at(db, high_id, "2026-03-01T12:00:00+00:00")

    actions = age_content_ideas(
        db,
        promote_after_days=30,
        dismiss_low_after_days=60,
        now=NOW,
    )

    assert [action.idea_id for action in actions] == [low_id, normal_id]
    normal = db.get_content_idea(normal_id)
    low = db.get_content_idea(low_id)
    high = db.get_content_idea(high_id)
    assert normal["priority"] == "high"
    assert normal["status"] == "open"
    assert low["priority"] == "low"
    assert low["status"] == "dismissed"
    assert high["priority"] == "high"
    assert high["status"] == "open"
    assert normal["updated_at"] == NOW.isoformat()

    metadata = json.loads(normal["source_metadata"])
    assert metadata["origin"] == "manual"
    assert metadata["keep"] is True
    assert metadata["aging_actions"][0]["source"] == "content_idea_aging"
    assert metadata["aging_actions"][0]["action"] == "promote_priority"
    assert metadata["aging_actions"][0]["to_priority"] == "high"


def test_aging_filters_by_topic(db):
    testing_id = db.add_content_idea("Testing stale idea", topic="Testing", priority="normal")
    ops_id = db.add_content_idea("Ops stale idea", topic="ops", priority="normal")
    _set_created_at(db, testing_id, "2026-04-01T12:00:00+00:00")
    _set_created_at(db, ops_id, "2026-04-01T12:00:00+00:00")

    actions = age_content_ideas(
        db,
        promote_after_days=30,
        dismiss_low_after_days=60,
        topic=" testing ",
        now=NOW,
    )

    assert [action.idea_id for action in actions] == [testing_id]
    assert db.get_content_idea(testing_id)["priority"] == "high"
    assert db.get_content_idea(ops_id)["priority"] == "normal"


def test_cmd_age_prints_json_dry_run_payload(db, capsys):
    idea_id = db.add_content_idea("JSON dry run idea", topic="testing", priority="normal")
    _set_created_at(db, idea_id, "2026-01-01T12:00:00+00:00")

    payload = cmd_age(
        db,
        promote_after_days=30,
        dismiss_low_after_days=60,
        dry_run=True,
        json_output=True,
    )
    output = json.loads(capsys.readouterr().out)

    assert payload[0]["idea_id"] == idea_id
    assert output[0]["idea_id"] == idea_id
    assert output[0]["action"] == "promote_priority"
    assert output[0]["age_days"] >= 30
    assert db.get_content_idea(idea_id)["priority"] == "normal"
