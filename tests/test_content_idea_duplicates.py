"""Tests for duplicate content idea cluster reporting."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from content_idea_duplicates import format_json_report, format_text_report, main
from synthesis.content_idea_duplicates import (
    find_duplicate_clusters,
    normalize_content_idea_text,
)


def test_normalization_collapses_case_punctuation_and_whitespace():
    assert normalize_content_idea_text("  Testing: CLI   WIRING!! ") == "testing cli wiring"


def test_clusters_highly_similar_open_ideas(db):
    first_id = db.add_content_idea(
        "Turn duplicate content idea seeds into one planning report",
        topic="planning",
    )
    second_id = db.add_content_idea(
        "Turn duplicate content idea seeds into one planning report.",
        topic="planning",
    )
    db.add_content_idea("A completely different idea about launch notes", topic="release")
    dismissed_id = db.add_content_idea(
        "Turn duplicate content idea seeds into one planning report",
        topic="planning",
    )
    db.dismiss_content_idea(dismissed_id)

    clusters = find_duplicate_clusters(db)

    assert len(clusters) == 1
    assert clusters[0].idea_ids == (first_id, second_id)
    assert clusters[0].primary_idea_id == first_id
    assert "lexical_similarity" in clusters[0].reasons


def test_topic_filter_limits_comparison(db):
    db.add_content_idea("Write about duplicate planning signals", topic="planning")
    db.add_content_idea("Write about duplicate planning signals", topic="testing")

    assert find_duplicate_clusters(db, topic="planning") == []


def test_metadata_identifier_match_clusters_different_notes(db):
    first_id = db.add_content_idea(
        "Summarize release highlights",
        topic="release",
        source="release",
        source_metadata={"release_id": 42, "campaign_id": 7},
    )
    second_id = db.add_content_idea(
        "Explain why the release matters",
        topic="announcement",
        source="release",
        source_metadata={"release_id": "42", "campaign_id": 8},
    )

    clusters = find_duplicate_clusters(db, min_similarity=0.99)

    assert len(clusters) == 1
    assert clusters[0].idea_ids == (first_id, second_id)
    assert clusters[0].shared_source_identifiers == {"release_id": "42"}
    assert "source_metadata.release_id" in clusters[0].reasons


def test_low_priority_excluded_by_default_and_included_with_flag(db):
    db.add_content_idea("Write about pruning duplicate ideas", priority="normal")
    db.add_content_idea("Write about pruning duplicate ideas", priority="low")

    assert find_duplicate_clusters(db) == []
    assert len(find_duplicate_clusters(db, include_low_priority=True)) == 1


def test_stable_primary_selection_prefers_priority_then_age(db):
    low_id = db.add_content_idea(
        "Explain stable duplicate report primary selection",
        priority="low",
    )
    normal_id = db.add_content_idea(
        "Explain stable duplicate report primary selection",
        priority="normal",
    )
    high_id = db.add_content_idea(
        "Explain stable duplicate report primary selection",
        priority="high",
    )

    clusters = find_duplicate_clusters(db, include_low_priority=True)

    assert clusters[0].idea_ids == (low_id, normal_id, high_id)
    assert clusters[0].primary_idea_id == high_id
    assert [member.id for member in clusters[0].members] == [high_id, normal_id, low_id]


def test_json_output_is_machine_readable(db):
    primary_id = db.add_content_idea("Write about duplicate idea reports", topic="ops")
    db.add_content_idea("Write about duplicate idea reports", topic="ops")
    clusters = find_duplicate_clusters(db)

    payload = json.loads(format_json_report(clusters))

    assert payload["cluster_count"] == 1
    assert payload["clusters"][0]["primary_idea_id"] == primary_id
    assert payload["clusters"][0]["members"][0]["id"] == primary_id


def test_text_output_names_primary_and_members(db):
    first_id = db.add_content_idea("Write about readable duplicate idea reports")
    second_id = db.add_content_idea("Write about readable duplicate idea reports")

    output = format_text_report(find_duplicate_clusters(db))

    assert "Content Idea Duplicate Clusters" in output
    assert f"primary #{first_id}" in output
    assert f"#{second_id}" in output


def test_cli_wiring_json_output(db, capsys):
    db.add_content_idea("Write about CLI duplicate reports", topic="tools")
    db.add_content_idea("Write about CLI duplicate reports", topic="tools")

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("content_idea_duplicates.script_context", fake_script_context):
        main(["--topic", "tools", "--format", "json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["cluster_count"] == 1
