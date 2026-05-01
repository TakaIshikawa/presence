"""Tests for deterministic reply-to-knowledge linking."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from engagement.reply_knowledge_linker import link_reply_knowledge  # noqa: E402
from link_reply_knowledge import main  # noqa: E402


@dataclass
class FakeKnowledgeItem:
    id: int
    source_type: str = "own_post"
    source_id: str = "source-1"
    author: str = "me"


class FakeSearchProvider:
    def __init__(self, results):
        self.results = results
        self.queries = []

    def search_similar(self, query, **kwargs):
        self.queries.append((query, kwargs))
        return self.results


def _insert_reply(db, *, status: str = "pending") -> int:
    return db.insert_reply_draft(
        inbound_tweet_id=f"tweet-{status}-{len(status)}",
        inbound_author_handle="alice",
        inbound_author_id="alice-id",
        inbound_text="How do you keep release notes grounded?",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="A post about release habits",
        draft_text="I anchor them to concrete decisions and tradeoffs.",
        relationship_context=json.dumps({"stage": "warm", "strength": 0.7}),
        status=status,
    )


def _link_count(db, reply_id: int) -> int:
    return db.conn.execute(
        "SELECT COUNT(*) FROM reply_knowledge_links WHERE reply_queue_id = ?",
        (reply_id,),
    ).fetchone()[0]


def test_dry_run_reports_proposed_links_without_inserting(db):
    reply_id = _insert_reply(db)
    provider = FakeSearchProvider([(FakeKnowledgeItem(10), 0.91)])

    report = link_reply_knowledge(db, reply_id=reply_id, search_provider=provider)

    assert report["dry_run"] is True
    assert report["totals"]["proposed_count"] == 1
    assert report["replies"][0]["proposed_links"][0]["knowledge_id"] == 10
    assert _link_count(db, reply_id) == 0
    query = provider.queries[0][0]
    assert "inbound: How do you keep release notes grounded?" in query
    assert "author: alice" in query
    assert "draft: I anchor them" in query


def test_apply_inserts_new_links_and_is_idempotent(db):
    reply_id = _insert_reply(db)
    provider = FakeSearchProvider(
        [
            (FakeKnowledgeItem(10), 0.91),
            (FakeKnowledgeItem(11), 0.82),
        ]
    )

    first = link_reply_knowledge(
        db,
        reply_id=reply_id,
        dry_run=False,
        search_provider=provider,
    )
    second = link_reply_knowledge(
        db,
        reply_id=reply_id,
        dry_run=False,
        search_provider=provider,
    )

    assert first["totals"]["linked_count"] == 2
    assert second["totals"]["linked_count"] == 0
    assert second["totals"]["existing_count"] == 2
    assert _link_count(db, reply_id) == 2


def test_min_score_excludes_low_scoring_matches(db):
    reply_id = _insert_reply(db)
    provider = FakeSearchProvider(
        [
            (FakeKnowledgeItem(10), 0.90),
            (FakeKnowledgeItem(12), 0.60),
        ]
    )

    report = link_reply_knowledge(
        db,
        reply_id=reply_id,
        min_score=0.72,
        dry_run=False,
        search_provider=provider,
    )

    assert report["totals"]["linked_count"] == 1
    assert report["totals"]["excluded_below_min_score_count"] == 1
    assert report["replies"][0]["excluded_below_min_score"][0]["knowledge_id"] == 12
    assert _link_count(db, reply_id) == 1


def test_batch_targets_replies_by_status(db):
    pending_id = _insert_reply(db, status="pending")
    _insert_reply(db, status="approved")
    provider = FakeSearchProvider([(FakeKnowledgeItem(10), 0.91)])

    report = link_reply_knowledge(
        db,
        status="pending",
        limit=20,
        dry_run=True,
        search_provider=provider,
    )

    assert [item["reply_id"] for item in report["replies"]] == [pending_id]
    assert report["totals"]["replies_scanned"] == 1


def test_empty_search_results_are_counted(db):
    reply_id = _insert_reply(db)
    provider = FakeSearchProvider([])

    report = link_reply_knowledge(db, reply_id=reply_id, search_provider=provider)

    assert report["totals"]["empty_search_count"] == 1
    assert report["totals"]["proposed_count"] == 0
    assert report["replies"][0]["proposed_links"] == []


def test_cli_supports_json_dry_run_with_fake_search_provider(db, capsys):
    reply_id = _insert_reply(db)
    fixed_report = link_reply_knowledge(
        db,
        reply_id=reply_id,
        search_provider=FakeSearchProvider([(FakeKnowledgeItem(10), 0.91)]),
    )

    @contextmanager
    def fake_script_context():
        config = SimpleNamespace(
            embeddings=SimpleNamespace(
                provider="fake",
                api_key="fake-key",
                model="fake-model",
            )
        )
        yield config, db

    with patch("link_reply_knowledge.script_context", fake_script_context), patch(
        "link_reply_knowledge.link_reply_knowledge",
        return_value=fixed_report,
    ), patch("link_reply_knowledge.get_embedding_provider", return_value=object()):
        result = main(["--reply-id", str(reply_id), "--format", "json"])

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["reply_id"] == reply_id
    assert payload["totals"]["proposed_count"] == 1
