"""Tests for seeding content ideas from repeated reply questions."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_question_idea_seeder import (
    SOURCE_NAME,
    build_reply_question_clusters,
    seed_reply_question_ideas,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "seed_reply_question_ideas.py"
spec = importlib.util.spec_from_file_location("seed_reply_question_ideas_script", SCRIPT_PATH)
seed_reply_question_ideas_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(seed_reply_question_ideas_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_question(db, tweet_id: str, text: str, **kwargs) -> int:
    detected_at = kwargs.pop("detected_at", None)
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id=f"user-{tweet_id}",
        inbound_text=text,
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="I wrote about pytest fixtures, database migrations, and release workflow.",
        draft_text="Draft reply",
        intent="question",
        priority="normal",
        status="pending",
        platform="x",
        inbound_url=f"https://x.com/alice/status/{tweet_id}",
    )
    defaults.update(kwargs)
    reply_id = db.insert_reply_draft(**defaults)
    if detected_at is not None:
        db.conn.execute("UPDATE reply_queue SET detected_at = ? WHERE id = ?", (detected_at, reply_id))
        db.conn.commit()
    return reply_id


def test_creates_one_idea_per_qualifying_question_cluster(db):
    first_id = _insert_question(db, "rq-1", "How do you test database migrations safely?")
    second_id = _insert_question(
        db,
        "rq-2",
        "What is your approach for testing database migrations before deploys?",
        inbound_author_handle="bob",
        platform="bluesky",
    )
    _insert_question(db, "rq-3", "How do you choose newsletter subject lines?")
    _insert_question(db, "rq-4", "What makes a good newsletter subject line?")

    results = seed_reply_question_ideas(db, days=30, min_cluster_size=2, now=NOW)

    assert [result.status for result in results] == ["created", "created"]
    ideas = db.get_content_ideas(status="open", limit=10)
    assert len(ideas) == 2
    first = next(result for result in results if result.topic == "testing")
    assert first.reply_ids == [first_id, second_id]
    assert "database migrations" in first.note
    assert ideas[0]["source"] == SOURCE_NAME


def test_duplicate_detection_matches_open_or_promoted_ideas_with_overlapping_reply_ids(db):
    first_id = _insert_question(db, "rq-dup-1", "How do you test database migrations safely?")
    _insert_question(db, "rq-dup-2", "What is your approach for testing database migrations?")
    existing_id = db.add_content_idea(
        note="Existing answer for this reply cluster",
        topic="testing",
        source=SOURCE_NAME,
        status="promoted",
        source_metadata={"source": SOURCE_NAME, "reply_ids": [first_id]},
    )

    results = seed_reply_question_ideas(db, days=30, min_cluster_size=2, now=NOW)

    assert [result.status for result in results] == ["skipped"]
    assert results[0].idea_id == existing_id
    assert results[0].reason == "promoted duplicate"
    assert len(db.get_content_ideas(status=None, limit=10)) == 1


def test_ignores_spam_like_posted_and_already_approved_replied_items_by_default(db):
    good_id = _insert_question(db, "rq-good-1", "How do you debug flaky workflow retries?")
    _insert_question(db, "rq-good-2", "What is your process for debugging flaky workflow retries?")
    _insert_question(
        db,
        "rq-spam",
        "How do I make money from your followers?",
        quality_flags=json.dumps(["spam"]),
    )
    _insert_question(db, "rq-low", "How do I get free promotion?", priority="low")
    _insert_question(db, "rq-posted-status", "How do you debug flaky workflow retries?", status="posted")
    posted_id = _insert_question(db, "rq-posted", "How do you debug flaky workflow retries?")
    approved_id = _insert_question(db, "rq-approved", "How do you debug flaky workflow retries?", status="approved")
    db.conn.execute("UPDATE reply_queue SET posted_at = ? WHERE id = ?", ("2026-05-01T10:00:00+00:00", posted_id))
    db.conn.commit()

    results = seed_reply_question_ideas(db, days=30, min_cluster_size=2, now=NOW)

    assert [result.status for result in results] == ["created"]
    assert good_id in results[0].reply_ids
    assert posted_id not in results[0].reply_ids
    assert approved_id not in results[0].reply_ids
    assert len(results[0].reply_ids) == 2


def test_dry_run_reports_cluster_without_writing_content_idea(db):
    _insert_question(db, "rq-dry-1", "How do you test database migrations?")
    _insert_question(db, "rq-dry-2", "What helps when testing database migrations?")

    results = seed_reply_question_ideas(db, days=30, min_cluster_size=2, dry_run=True, now=NOW)

    assert [result.status for result in results] == ["candidate"]
    assert results[0].idea_id is None
    assert results[0].reason == "dry run"
    assert db.get_content_ideas(status="open") == []


def test_metadata_shape_contains_reply_ids_handles_platforms_and_terms(db):
    first_id = _insert_question(
        db,
        "rq-meta-1",
        "How do you design pytest fixtures for database migrations?",
        inbound_author_handle="alice",
    )
    second_id = _insert_question(
        db,
        "rq-meta-2",
        "What is your pytest fixture design for database migration tests?",
        inbound_author_handle="bob",
        platform="bluesky",
    )

    results = seed_reply_question_ideas(db, days=30, min_cluster_size=2, now=NOW)

    metadata = results[0].source_metadata
    assert metadata["source"] == SOURCE_NAME
    assert metadata["reply_ids"] == [first_id, second_id]
    assert metadata["handles"] == ["alice", "bob"]
    assert metadata["platforms"] == ["bluesky", "x"]
    assert "database" in metadata["cluster_terms"]
    assert "pytest" in metadata["cluster_terms"]
    idea = db.get_content_ideas(status="open")[0]
    persisted = json.loads(idea["source_metadata"])
    assert persisted == metadata


def test_build_clusters_respects_days_limit_and_min_cluster_size(db):
    _insert_question(db, "rq-old-1", "How do you test database migrations?", detected_at="2026-03-01T12:00:00+00:00")
    _insert_question(db, "rq-old-2", "What helps testing database migrations?", detected_at="2026-03-01T13:00:00+00:00")
    _insert_question(db, "rq-new-1", "How do you debug workflow retries?", detected_at="2026-04-30T12:00:00+00:00")
    _insert_question(db, "rq-new-2", "What helps debugging workflow retries?", detected_at="2026-04-30T13:00:00+00:00")
    _insert_question(db, "rq-new-3", "How do you pick launch topics?", detected_at="2026-04-30T14:00:00+00:00")

    clusters = build_reply_question_clusters(db, days=7, min_cluster_size=2, limit=1, now=NOW)

    assert len(clusters) == 1
    assert clusters[0].reply_ids == [3, 4]


def test_cli_supports_requested_flags_and_json_output(db, monkeypatch, capsys):
    _insert_question(db, "rq-cli-1", "How do you test database migrations?")
    _insert_question(db, "rq-cli-2", "What helps testing database migrations?")
    monkeypatch.setattr(seed_reply_question_ideas_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        seed_reply_question_ideas_script,
        "seed_reply_question_ideas",
        lambda db, **kwargs: seed_reply_question_ideas(db, now=NOW, **kwargs),
    )

    exit_code = seed_reply_question_ideas_script.main(
        ["--days", "30", "--min-cluster-size", "2", "--limit", "5", "--dry-run", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload[0]["status"] == "candidate"
    assert payload[0]["source_metadata"]["source"] == SOURCE_NAME
    assert db.get_content_ideas(status="open") == []
