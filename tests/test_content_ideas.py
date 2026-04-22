"""Tests for the manual content idea inbox."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from content_ideas import cmd_add, cmd_dismiss, cmd_list, cmd_promote


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
