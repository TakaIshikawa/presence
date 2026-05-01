"""Tests for Claude Code error pattern idea seeding."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from seed_claude_error_ideas import format_results_json, format_results_table, main  # noqa: E402
from synthesis.claude_error_idea_seeder import (  # noqa: E402
    SOURCE_NAME,
    build_claude_error_idea_candidates,
    seed_claude_error_ideas,
)


NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


def _add_message(
    db,
    *,
    message_uuid: str,
    prompt_text: str,
    session_id: str = "sess-1",
    project_path: str = "/repo/presence",
    timestamp: str = "2026-04-22T12:00:00+00:00",
) -> int:
    return db.insert_claude_message(
        session_id=session_id,
        message_uuid=message_uuid,
        project_path=project_path,
        timestamp=timestamp,
        prompt_text=prompt_text,
    )


def test_build_candidates_groups_similar_error_messages_with_supporting_ids(db):
    first_id = _add_message(
        db,
        message_uuid="uuid-1",
        prompt_text="pytest failed with Error: database locked on /tmp/run-123/test.db",
    )
    second_id = _add_message(
        db,
        message_uuid="uuid-2",
        session_id="sess-2",
        timestamp="2026-04-22T13:00:00+00:00",
        prompt_text="pytest failed with Error: database locked on /private/var/run-987/test.db",
    )

    candidates = build_claude_error_idea_candidates(db, days=7, min_count=2, now=NOW)

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.occurrence_count == 2
    assert candidate.message_ids == [first_id, second_id]
    assert candidate.message_uuids == ["uuid-1", "uuid-2"]
    assert candidate.session_ids == ["sess-1", "sess-2"]
    assert "database locked" in candidate.normalized_phrase
    assert "<path>" in candidate.normalized_phrase
    assert candidate.source_metadata["source"] == SOURCE_NAME
    assert candidate.source_metadata["pattern_id"] == candidate.pattern_id
    assert candidate.source_metadata["message_ids"] == [first_id, second_id]


def test_candidates_below_minimum_occurrence_threshold_are_skipped(db):
    _add_message(
        db,
        message_uuid="uuid-1",
        prompt_text="Command failed with exit code 1: uv run pytest tests/test_one.py",
    )
    _add_message(
        db,
        message_uuid="uuid-2",
        prompt_text="A regular planning prompt without failure details",
    )

    candidates = build_claude_error_idea_candidates(db, days=7, min_count=2, now=NOW)

    assert candidates == []


def test_seed_creates_content_idea_with_deterministic_metadata_and_skips_duplicate(db):
    _add_message(
        db,
        message_uuid="uuid-1",
        prompt_text="Tool error: Bash command failed with exit code 2 while running ruff",
    )
    _add_message(
        db,
        message_uuid="uuid-2",
        timestamp="2026-04-22T13:00:00+00:00",
        prompt_text="Tool error: Bash command failed with exit code 1 while running ruff",
    )

    first = seed_claude_error_ideas(db, days=7, min_count=2, now=NOW)
    second = seed_claude_error_ideas(db, days=7, min_count=2, now=NOW)

    assert [result.status for result in first] == ["created"]
    assert [result.status for result in second] == ["skipped"]
    assert second[0].idea_id == first[0].idea_id
    assert second[0].reason == "open duplicate"

    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    idea = ideas[0]
    assert idea["source"] == SOURCE_NAME
    metadata = json.loads(idea["source_metadata"])
    assert metadata == first[0].source_metadata
    assert metadata["pattern_id"].startswith("claude_error_")
    assert metadata["normalized_phrase"] == first[0].normalized_phrase
    assert metadata["message_uuids"] == ["uuid-1", "uuid-2"]


def test_dry_run_returns_identical_candidate_data_without_database_writes(db):
    _add_message(
        db,
        message_uuid="uuid-1",
        prompt_text="Traceback (most recent call last):\n  File app.py, line 7\nValueError: bad config 123",
    )
    _add_message(
        db,
        message_uuid="uuid-2",
        timestamp="2026-04-22T13:00:00+00:00",
        prompt_text="Traceback (most recent call last):\n  File app.py, line 8\nValueError: bad config 456",
    )

    candidates = build_claude_error_idea_candidates(db, days=7, min_count=2, now=NOW)
    results = seed_claude_error_ideas(db, days=7, min_count=2, dry_run=True, now=NOW)

    assert len(candidates) == 1
    assert len(results) == 1
    assert results[0].status == "proposed"
    assert results[0].reason == "dry run"
    assert results[0].source_metadata == candidates[0].source_metadata
    assert db.get_content_ideas(status="open") == []


def test_format_results_table_and_json_include_seed_summary(db):
    _add_message(
        db,
        message_uuid="uuid-1",
        prompt_text="same error, pytest failed with Error: missing migration",
    )
    _add_message(
        db,
        message_uuid="uuid-2",
        timestamp="2026-04-22T13:00:00+00:00",
        prompt_text="same error, pytest failed with Error: missing migration",
    )
    candidates = build_claude_error_idea_candidates(db, days=7, min_count=2, now=NOW)
    results = seed_claude_error_ideas(db, days=7, min_count=2, dry_run=True, now=NOW)

    table = format_results_table(candidates, results)
    payload = json.loads(format_results_json(candidates, results))

    assert "candidates=1" in table
    assert "seed_results created=0 proposed=1 skipped=0" in table
    assert payload["seed_results"][0]["status"] == "proposed"
    assert payload["candidates"][0]["source_metadata"] == results[0].source_metadata


def test_main_prints_dry_run_json_without_persisting(db, capsys):
    _add_message(
        db,
        message_uuid="uuid-1",
        prompt_text="Command failed with exit code 1: uv run pytest tests/test_a.py",
    )
    _add_message(
        db,
        message_uuid="uuid-2",
        timestamp="2026-04-22T13:00:00+00:00",
        prompt_text="Command failed with exit code 2: uv run pytest tests/test_b.py",
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("seed_claude_error_ideas.script_context", fake_script_context):
        main(["--days", "30", "--min-count", "2", "--dry-run", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["seed_results"][0]["status"] == "proposed"
    assert payload["seed_results"][0]["source_metadata"] == payload["candidates"][0]["source_metadata"]
    assert db.get_content_ideas(status="open") == []
