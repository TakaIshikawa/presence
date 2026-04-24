"""Tests for seeding content ideas from profile milestones."""

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.profile_milestones import (
    detect_profile_milestones,
    seed_profile_milestone_ideas,
)
from seed_profile_milestone_ideas import format_results_table, main


def _profile_snapshot(db, platform: str, followers: int, fetched_at: str) -> int:
    row_id = db.insert_profile_metrics(
        platform=platform,
        follower_count=followers,
        following_count=10,
        tweet_count=20,
        listed_count=None,
    )
    db.conn.execute(
        "UPDATE profile_metrics SET fetched_at = ? WHERE id = ?",
        (fetched_at, row_id),
    )
    db.conn.commit()
    return row_id


def test_detect_profile_milestones_finds_each_threshold_crossing(db):
    _profile_snapshot(db, "x", 95, "2026-04-01T00:00:00+00:00")
    _profile_snapshot(db, "x", 205, "2026-04-02T00:00:00+00:00")

    candidates = detect_profile_milestones(db, platform="x", step=100)

    assert [candidate.threshold for candidate in candidates] == [100, 200]
    assert candidates[0].source_metadata["platform"] == "x"
    assert candidates[0].source_metadata["metric"] == "follower_count"
    assert candidates[0].source_metadata["previous_value"] == 95
    assert candidates[0].source_metadata["current_value"] == 205
    assert candidates[0].source_metadata["fetched_at"] == "2026-04-02T00:00:00+00:00"


def test_seed_profile_milestone_ideas_dry_run_reports_without_writes(db):
    _profile_snapshot(db, "x", 90, "2026-04-01T00:00:00+00:00")
    _profile_snapshot(db, "x", 110, "2026-04-02T00:00:00+00:00")

    results = seed_profile_milestone_ideas(db, platform="x", dry_run=True)

    assert [result.status for result in results] == ["proposed"]
    assert results[0].idea_id is None
    assert db.get_content_ideas(status="open") == []
    assert "proposed=1" in format_results_table(results)


def test_seed_profile_milestone_ideas_inserts_and_deduplicates_by_source_metadata(db):
    _profile_snapshot(db, "x", 90, "2026-04-01T00:00:00+00:00")
    _profile_snapshot(db, "x", 110, "2026-04-02T00:00:00+00:00")

    first = seed_profile_milestone_ideas(db, platform="x")
    second = seed_profile_milestone_ideas(db, platform="x")

    assert first[0].status == "created"
    assert second[0].status == "skipped"
    assert second[0].idea_id == first[0].idea_id
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    assert ideas[0]["source"] == "profile_milestone"
    metadata = json.loads(ideas[0]["source_metadata"])
    assert metadata["source_id"] == "x:follower_count:100"
    assert metadata["threshold"] == 100


def test_seed_profile_milestone_ideas_supports_all_and_single_platform(db):
    _profile_snapshot(db, "x", 90, "2026-04-01T00:00:00+00:00")
    _profile_snapshot(db, "x", 110, "2026-04-02T00:00:00+00:00")
    _profile_snapshot(db, "bluesky", 40, "2026-04-01T00:00:00+00:00")
    _profile_snapshot(db, "bluesky", 60, "2026-04-02T00:00:00+00:00")

    bluesky = seed_profile_milestone_ideas(db, platform="bluesky", step=50)
    all_results = seed_profile_milestone_ideas(db, platform="all", step=50)

    assert [(result.platform, result.threshold, result.status) for result in bluesky] == [
        ("bluesky", 50, "created")
    ]
    assert [(result.platform, result.threshold, result.status) for result in all_results] == [
        ("bluesky", 50, "skipped"),
        ("x", 100, "created"),
    ]


def test_seed_profile_milestone_ideas_cli_outputs_json(db, capsys):
    _profile_snapshot(db, "x", 90, "2026-04-01T00:00:00+00:00")
    _profile_snapshot(db, "x", 110, "2026-04-02T00:00:00+00:00")

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("seed_profile_milestone_ideas.script_context", fake_script_context):
        main(["--platform", "x", "--dry-run", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["status"] == "proposed"
    assert payload[0]["platform"] == "x"
    assert payload[0]["threshold"] == 100
    assert db.get_content_ideas(status="open") == []


def test_profile_milestone_step_must_be_positive(db):
    with pytest.raises(ValueError, match="step"):
        seed_profile_milestone_ideas(db, step=0)
