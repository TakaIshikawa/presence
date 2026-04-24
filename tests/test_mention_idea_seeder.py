"""Tests for seeding content ideas from inbound mention demand."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from seed_mention_ideas import format_results_table, main, seed_mention_ideas


def _insert_mention(db, tweet_id: str, text: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="user_A",
        inbound_text=text,
        our_tweet_id="our_tw_1",
        our_content_id=None,
        our_post_text="I changed our pytest fixtures and architecture boundaries.",
        draft_text="Drafted direct reply",
        intent="question",
        priority="normal",
        status="pending",
        inbound_url=f"https://x.com/alice/status/{tweet_id}",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def test_seed_mention_ideas_creates_source_metadata_linked_to_mention(db):
    mention_id = _insert_mention(
        db,
        "m-1",
        "How do you decide when pytest fixtures should become shared test helpers?",
        inbound_author_handle="alice",
    )

    results = seed_mention_ideas(db)

    assert [(result.status, result.kind, result.topic) for result in results] == [
        ("created", "mention", "testing")
    ]
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    idea = ideas[0]
    metadata = json.loads(idea["source_metadata"])
    assert idea["source"] == "mention_idea_seeder"
    assert metadata["mention_id"] == mention_id
    assert metadata["mention_ids"] == [mention_id]
    assert metadata["inbound_tweet_id"] == "m-1"
    assert metadata["source_id"] == "x:m-1"
    assert metadata["inbound_url"].endswith("/m-1")
    assert "standalone post" in idea["note"]


def test_seed_mention_ideas_dry_run_reports_candidates_without_writing(db):
    _insert_mention(db, "m-dry", "What testing workflow do you use for flaky retries?")

    results = seed_mention_ideas(db, dry_run=True)

    assert len(results) == 1
    assert results[0].status == "candidate"
    assert results[0].reason == "dry run"
    assert db.get_content_ideas(status="open") == []


def test_seed_mention_ideas_skips_duplicate_open_idea(db):
    _insert_mention(db, "m-dup", "How do you test database migrations safely?")
    first = seed_mention_ideas(db)

    second = seed_mention_ideas(db)

    assert [result.status for result in first] == ["created"]
    assert [result.status for result in second] == ["skipped"]
    assert second[0].idea_id == first[0].idea_id
    assert second[0].reason == "open content idea duplicate"
    assert len(db.get_content_ideas(status="open")) == 1


def test_seed_mention_ideas_skips_existing_planned_topic(db):
    db.insert_planned_topic(
        topic="testing",
        angle="answer audience testing questions",
        target_date="2026-05-01",
    )
    _insert_mention(db, "m-planned", "How should pytest fixtures be organized?")

    results = seed_mention_ideas(db)

    assert [result.status for result in results] == ["skipped"]
    assert results[0].reason == "planned planned topic duplicate"
    assert db.get_content_ideas(status="open") == []


def test_seed_mention_ideas_ignores_low_value_spam_dismissed_and_posted(db):
    _insert_mention(db, "m-good", "How do you design module boundaries for agents?")
    _insert_mention(db, "m-thanks", "Thanks?", intent="appreciation")
    _insert_mention(db, "m-spam", "How do I earn money with crypto?", intent="spam")
    _insert_mention(db, "m-low", "How do you debug this?", priority="low")
    _insert_mention(db, "m-dismissed", "How do you test this?", status="dismissed")
    _insert_mention(db, "m-posted", "How do you test this?", status="posted")
    _insert_mention(
        db,
        "m-flagged",
        "How do you test this?",
        quality_flags=json.dumps(["low_value"]),
    )

    results = seed_mention_ideas(db)

    assert len(results) == 1
    assert results[0].status == "created"
    metadata = results[0].source_metadata
    assert metadata["inbound_tweet_id"] == "m-good"


def test_seed_mention_ideas_groups_recurring_themes(db):
    first_id = _insert_mention(db, "m-theme-1", "How do you test database migrations?")
    second_id = _insert_mention(db, "m-theme-2", "How do you test database migrations?")

    results = seed_mention_ideas(db)

    assert [(result.status, result.kind, result.topic) for result in results] == [
        ("created", "theme", "testing")
    ]
    metadata = results[0].source_metadata
    assert metadata["mention_ids"] == [first_id, second_id]
    assert metadata["mention_count"] == 2


def test_format_results_table_is_concise(db):
    _insert_mention(db, "m-table", "How do you test database migrations?")
    results = seed_mention_ideas(db)

    output = format_results_table(results)

    assert "Status" in output
    assert "created" in output
    assert "testing" in output


def test_main_prints_json(db, capsys):
    _insert_mention(db, "m-json", "How do you test database migrations?")

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("seed_mention_ideas.script_context", fake_script_context):
        main(["--dry-run", "--json"])

    output = capsys.readouterr().out
    payload = json.loads(output)
    assert payload[0]["status"] == "candidate"
    assert payload[0]["source_metadata"]["inbound_tweet_id"] == "m-json"
