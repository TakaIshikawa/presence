"""Tests for the manual content idea inbox."""

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
    promote_id = db.add_content_idea("Promote this", priority="high")
    dismiss_id = db.add_content_idea("Dismiss this", priority="high")

    db.promote_content_idea(promote_id)
    db.dismiss_content_idea(dismiss_id)

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

    cmd_promote(db, idea_id)
    output = capsys.readouterr().out
    assert f"Promoted content idea {idea_id}" in output
    assert db.get_content_idea(idea_id)["status"] == "promoted"

    cmd_dismiss(db, idea_id)
    output = capsys.readouterr().out
    assert f"Dismissed content idea {idea_id}" in output
    assert db.get_content_idea(idea_id)["status"] == "dismissed"
