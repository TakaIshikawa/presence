"""Tests for mining recurring reply FAQ ideas."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from engagement.reply_faq_miner import (
    build_reply_faq_miner,
    format_reply_faq_miner_json,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "mine_reply_faq.py"
spec = importlib.util.spec_from_file_location("mine_reply_faq_cli", SCRIPT_PATH)
mine_reply_faq_cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mine_reply_faq_cli)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _reply(
    db,
    tweet_id: str,
    text: str,
    *,
    author: str = "alice",
    author_id: str | None = None,
    intent: str = "question",
    status: str = "pending",
    draft_text: str | None = None,
    detected_at: datetime | None = None,
    quality_score: float | None = 8.0,
    quality_flags: list[str] | None = None,
) -> int:
    reply_id = db.insert_reply_draft(
        inbound_tweet_id=tweet_id,
        inbound_author_handle=author,
        inbound_author_id=author_id or author,
        inbound_text=text,
        our_tweet_id=f"our-{tweet_id}",
        our_content_id=None,
        our_post_text="A post about tests, migrations, and release workflow.",
        draft_text=draft_text,
        intent=intent,
        priority="normal",
        status=status,
        inbound_url=f"https://x.com/{author}/status/{tweet_id}",
        quality_score=quality_score,
        quality_flags=json.dumps(quality_flags) if quality_flags is not None else None,
    )
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        ((detected_at or NOW - timedelta(days=1)).isoformat(), reply_id),
    )
    db.conn.commit()
    return reply_id


def test_clusters_related_questions_and_includes_representative_answer(db):
    first = _reply(
        db,
        "r1",
        "How do you test database migrations safely?",
        author="alice",
    )
    second = _reply(
        db,
        "r2",
        "How should I test DB migration rollbacks?",
        author="bob",
        status="approved",
        draft_text="I use a fixture that snapshots schema state, runs rollback, and asserts both data and indexes.",
    )
    _reply(db, "solo", "How do you pick launch notes?", author="carol")

    report = build_reply_faq_miner(db, days=30, min_count=2, now=NOW)

    assert report["summary"]["cluster_count"] == 1
    cluster = report["clusters"][0]
    assert cluster["reply_ids"] == [first, second]
    assert cluster["reply_count"] == 2
    assert cluster["author_count"] == 2
    assert "database migrations" in cluster["representative_question"].lower()
    assert "schema state" in cluster["answer_excerpt"]
    assert "Create an FAQ explainer" in cluster["suggested_content_idea_note"]
    assert cluster["seed_status"] == "candidate"


def test_ranking_prefers_recurrence_recency_and_author_diversity(db):
    for index, author in enumerate(("alice", "bob", "carol"), start=1):
        _reply(
            db,
            f"fresh-{index}",
            f"How do you test flaky retry workflow {index}?",
            author=author,
            detected_at=NOW - timedelta(hours=index),
        )
    for index in range(2):
        _reply(
            db,
            f"old-{index}",
            f"How do you organize release notes checklist {index}?",
            author="same-author",
            detected_at=NOW - timedelta(days=20 + index),
        )

    report = build_reply_faq_miner(db, days=60, min_count=2, now=NOW)

    assert report["clusters"][0]["reply_count"] == 3
    assert report["clusters"][0]["author_count"] == 3
    assert "flaky retry" in report["clusters"][0]["representative_question"].lower()


def test_excludes_spam_appreciation_and_low_signal_replies(db):
    good_1 = _reply(db, "good-1", "How do you debug migration failures?", author="alice")
    good_2 = _reply(db, "good-2", "How should we debug DB migration errors?", author="bob")
    _reply(db, "thanks", "Thanks?", intent="appreciation")
    _reply(db, "spam", "How do I earn crypto fast?", intent="spam")
    _reply(db, "flagged", "How do you debug migration failures?", quality_flags=["low_value"])
    _reply(db, "low-score", "How do you debug migration failures?", quality_score=2.0)

    report = build_reply_faq_miner(db, days=30, min_count=2, now=NOW)

    assert report["summary"]["eligible_reply_count"] == 2
    assert report["clusters"][0]["reply_ids"] == [good_1, good_2]


def test_dry_run_does_not_create_content_ideas(db):
    _reply(db, "dry-1", "How do you test rollback migrations?", author="alice")
    _reply(db, "dry-2", "How should I test DB migration rollbacks?", author="bob")

    report = build_reply_faq_miner(db, days=30, min_count=2, apply=False, now=NOW)

    assert report["summary"]["candidate_count"] == 1
    assert report["summary"]["created_count"] == 0
    assert db.get_content_ideas(status="open") == []


def test_apply_is_idempotent_and_stores_cluster_metadata(db):
    first = _reply(db, "apply-1", "How do you test rollback migrations?", author="alice")
    second = _reply(db, "apply-2", "How should I test DB migration rollbacks?", author="bob")

    first_report = build_reply_faq_miner(db, days=30, min_count=2, apply=True, now=NOW)
    second_report = build_reply_faq_miner(db, days=30, min_count=2, apply=True, now=NOW)

    assert first_report["summary"]["created_count"] == 1
    assert second_report["summary"]["created_count"] == 0
    assert second_report["summary"]["skipped_count"] == 1
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    idea = ideas[0]
    assert idea["source"] == "reply_faq_miner"
    metadata = json.loads(idea["source_metadata"])
    assert metadata["reply_ids"] == [first, second]
    assert metadata["reply_count"] == 2
    assert metadata["cluster_fingerprint"]
    assert second_report["clusters"][0]["idea_id"] == idea["id"]


def test_cli_json_output(db, capsys):
    _reply(db, "cli-1", "How do you test rollback migrations?", author="alice")
    _reply(db, "cli-2", "How should I test DB migration rollbacks?", author="bob")

    @contextmanager
    def fake_script_context():
        yield SimpleNamespace(), db

    with patch.object(mine_reply_faq_cli, "script_context", fake_script_context), patch.object(
        mine_reply_faq_cli,
        "build_reply_faq_miner",
        wraps=lambda db, **kwargs: build_reply_faq_miner(db, now=NOW, **kwargs),
    ):
        assert mine_reply_faq_cli.main(
            ["--days", "30", "--min-count", "2", "--limit", "5", "--format", "json"]
        ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["days"] == 30
    assert payload["filters"]["min_count"] == 2
    assert payload["filters"]["limit"] == 5
    assert payload["summary"]["cluster_count"] == 1
    assert payload["clusters"][0]["seed_status"] == "candidate"


def test_json_formatter_is_deterministic(db):
    _reply(db, "json-1", "How do you test rollback migrations?", author="alice")
    _reply(db, "json-2", "How should I test DB migration rollbacks?", author="bob")

    report = build_reply_faq_miner(db, days=30, min_count=2, now=NOW)

    assert format_reply_faq_miner_json(report) == format_reply_faq_miner_json(report)
