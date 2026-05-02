"""Tests for mining unresolved Claude Code questions into content ideas."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from synthesis.claude_unresolved_question_miner import (
    SOURCE_NAME,
    build_claude_unresolved_question_candidates,
    extract_unresolved_questions_from_text,
    mine_claude_unresolved_questions,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "mine_claude_unresolved_questions.py"
)
spec = importlib.util.spec_from_file_location("mine_claude_unresolved_questions_script", SCRIPT_PATH)
mine_claude_unresolved_questions_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(mine_claude_unresolved_questions_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _add_message(
    db,
    *,
    message_uuid: str,
    prompt_text: str,
    session_id: str = "sess-1",
    project_path: str = "/repo/presence",
    timestamp: str = "2026-04-30T12:00:00+00:00",
) -> int:
    return db.insert_claude_message(
        session_id=session_id,
        message_uuid=message_uuid,
        project_path=project_path,
        timestamp=timestamp,
        prompt_text=prompt_text,
    )


def test_plain_text_helper_extracts_unresolved_questions_with_metadata():
    candidates = extract_unresolved_questions_from_text(
        """
        We got the migration tests passing.
        Open question: should the sync worker retry partial API failures or surface them immediately?
        TODO: investigate whether the schema cache can be invalidated per tenant.
        """,
        session_metadata={
            "session_id": "sess-plain",
            "message_uuid": "uuid-plain",
            "project_path": "/repo/presence",
            "timestamp": "2026-04-30T12:00:00+00:00",
        },
        min_confidence=0.6,
    )

    assert len(candidates) == 2
    first = candidates[0]
    assert first.session_id == "sess-plain"
    assert first.message_uuid == "uuid-plain"
    assert first.question_fingerprint.startswith("claude_unresolved_")
    assert "sync worker retry" in first.question
    assert "explicit unresolved marker" in first.reason
    assert first.confidence >= 0.8
    assert first.source_metadata["source"] == SOURCE_NAME


def test_build_candidates_filters_rhetorical_and_resolved_questions_below_threshold(db):
    kept_id = _add_message(
        db,
        message_uuid="uuid-1",
        prompt_text=(
            "Unclear implementation detail: should generated content ideas preserve "
            "the original Claude session question?"
        ),
    )
    _add_message(
        db,
        message_uuid="uuid-2",
        prompt_text="The parser is done. What could go wrong?",
    )
    _add_message(
        db,
        message_uuid="uuid-3",
        prompt_text="Resolved: should we keep the compatibility wrapper? Answer: yes.",
    )

    candidates = build_claude_unresolved_question_candidates(
        db,
        days=7,
        min_confidence=0.7,
        now=NOW,
    )

    assert len(candidates) == 1
    assert candidates[0].message_id == kept_id
    assert candidates[0].message_uuid == "uuid-1"
    assert "generated content ideas" in candidates[0].question
    assert candidates[0].confidence >= 0.7


def test_dry_run_reports_candidates_without_writing_content_ideas(db):
    _add_message(
        db,
        message_uuid="uuid-1",
        prompt_text="Follow-up: need to decide whether the API exporter batches retries by project.",
    )

    results = mine_claude_unresolved_questions(
        db,
        days=7,
        dry_run=True,
        min_confidence=0.6,
        now=NOW,
    )

    assert [result.status for result in results] == ["proposed"]
    assert results[0].idea_id is None
    assert results[0].source_metadata["question_fingerprint"] == results[0].question_fingerprint
    assert db.get_content_ideas(status="open") == []


def test_repeated_runs_skip_duplicate_for_same_session_question_fingerprint(db):
    _add_message(
        db,
        message_uuid="uuid-1",
        prompt_text="Open question: should the newsletter exporter include unresolved implementation notes?",
    )

    first = mine_claude_unresolved_questions(db, days=7, min_confidence=0.6, now=NOW)
    second = mine_claude_unresolved_questions(db, days=7, min_confidence=0.6, now=NOW)

    assert [result.status for result in first] == ["created"]
    assert [result.status for result in second] == ["skipped"]
    assert second[0].idea_id == first[0].idea_id
    assert second[0].reason == "open duplicate"
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    metadata = json.loads(ideas[0]["source_metadata"])
    assert metadata["source"] == SOURCE_NAME
    assert metadata["question_fingerprint"] == first[0].question_fingerprint


def test_same_question_in_different_session_gets_distinct_fingerprint(db):
    prompt = "Open question: should the CLI report unresolved schema tradeoffs?"
    _add_message(db, message_uuid="uuid-1", session_id="sess-a", prompt_text=prompt)
    _add_message(db, message_uuid="uuid-2", session_id="sess-b", prompt_text=prompt)

    candidates = build_claude_unresolved_question_candidates(
        db,
        days=7,
        min_confidence=0.6,
        now=NOW,
    )

    assert len(candidates) == 2
    assert len({candidate.question_fingerprint for candidate in candidates}) == 2


def test_cli_supports_requested_flags(db, monkeypatch, capsys):
    _add_message(
        db,
        message_uuid="uuid-1",
        prompt_text="TODO: figure out whether release notes should call out test flakes.",
    )
    monkeypatch.setattr(
        mine_claude_unresolved_questions_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        mine_claude_unresolved_questions_script,
        "mine_claude_unresolved_questions",
        lambda db, **kwargs: mine_claude_unresolved_questions(db, now=NOW, **kwargs),
    )

    exit_code = mine_claude_unresolved_questions_script.main(
        ["--days", "7", "--limit", "5", "--dry-run", "--json", "--min-confidence", "0.6"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload[0]["status"] == "proposed"
    assert payload[0]["source_metadata"]["source"] == SOURCE_NAME
    assert db.get_content_ideas(status="open") == []
