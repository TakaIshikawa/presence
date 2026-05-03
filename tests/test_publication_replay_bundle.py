"""Tests for publication replay bundle v2 export."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path

import pytest

from evaluation.publication_replay_bundle import (
    REDACTED,
    build_publication_replay_bundle,
    format_publication_replay_bundle_json,
)


BASE_TIME = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "export_publication_replay_bundle_v2.py"
)
spec = importlib.util.spec_from_file_location("export_publication_replay_bundle_v2", SCRIPT_PATH)
export_publication_replay_bundle_v2 = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(export_publication_replay_bundle_v2)


@contextmanager
def _script_context(db):
    yield None, db


def _seed_bundle(db) -> dict[str, int]:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["abc123"],
        source_messages=["uuid-1"],
        source_activity_ids=["repo#1:issue"],
        content="Debug this failed post from @author: https://example.com/post",
        eval_score=8.0,
        eval_feedback="looks useful",
        content_format="tip",
        image_path="/tmp/replay-card.png",
        image_prompt="A product card for https://example.com",
        image_alt_text="Card mentioning @author",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        ((BASE_TIME - timedelta(hours=2)).isoformat(), content_id),
    )
    queue_id = db.queue_for_publishing(
        content_id,
        (BASE_TIME - timedelta(minutes=30)).isoformat(),
        platform="x",
    )
    db.conn.execute(
        """UPDATE publish_queue
           SET created_at = ?, status = 'failed', error = ?, error_category = 'auth'
           WHERE id = ?""",
        (
            (BASE_TIME - timedelta(hours=1)).isoformat(),
            "failed for @author at https://example.com/error",
            queue_id,
        ),
    )
    db.upsert_content_variant(
        content_id,
        "x",
        "post",
        "Variant for @author https://example.com/variant",
        metadata={"note": "review @author at https://example.com/meta", "status": "draft"},
    )
    db.select_content_variant(content_id, "x", "post")
    db.upsert_publication_failure(
        content_id,
        "x",
        "401 invalid token from @platform",
        error_category="auth",
    )
    old_attempt_id = db.record_publication_attempt(
        queue_id,
        content_id,
        "x",
        False,
        attempted_at=(BASE_TIME - timedelta(minutes=20)).isoformat(),
        error="old failure",
        error_category="auth",
    )
    new_attempt_id = db.record_publication_attempt(
        queue_id,
        content_id,
        "x",
        False,
        attempted_at=(BASE_TIME - timedelta(minutes=5)).isoformat(),
        error="new failure at https://example.com/fail from @platform",
        error_category="auth",
        response_metadata={"message": "retry @platform via https://example.com/retry", "status": 401},
    )
    db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "article-1",
            "https://example.com/source",
            "@source",
            "Source text",
            "Useful detail",
            1,
        ),
    )
    knowledge_id = int(db.conn.execute("SELECT MAX(id) FROM knowledge").fetchone()[0])
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])
    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, like_count, retweet_count, reply_count, quote_count,
            engagement_score, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            "tweet-1",
            4,
            1,
            2,
            0,
            7.5,
            (BASE_TIME - timedelta(minutes=1)).isoformat(),
        ),
    )
    db.conn.commit()
    return {
        "content_id": content_id,
        "queue_id": queue_id,
        "new_attempt_id": new_attempt_id,
        "old_attempt_id": old_attempt_id,
        "knowledge_id": knowledge_id,
    }


def test_content_id_lookup_builds_complete_bundle_newest_attempts_first(db):
    ids = _seed_bundle(db)

    bundle = build_publication_replay_bundle(
        db,
        content_id=ids["content_id"],
        generated_at=BASE_TIME,
    )

    assert bundle["artifact_type"] == "publication_replay_bundle"
    assert bundle["bundle_version"] == 2
    assert bundle["lookup"]["resolved_content_id"] == ids["content_id"]
    assert bundle["lookup"]["resolved_queue_id"] == ids["queue_id"]
    assert bundle["generated_content"]["id"] == ids["content_id"]
    assert len(bundle["generated_content"]["content_digest"]) == 64
    assert len(bundle["publish_queue"]["content_digest"]) == 64
    assert bundle["publish_queue"]["status"] == "failed"
    assert [attempt["id"] for attempt in bundle["publication_attempts"]] == [
        ids["new_attempt_id"],
        ids["old_attempt_id"],
    ]
    assert all(len(attempt["content_digest"]) == 64 for attempt in bundle["publication_attempts"])
    assert bundle["content_variants"][0]["selected"] is True
    assert len(bundle["content_variants"][0]["content_digest"]) == 64
    assert bundle["content_publications"][0]["status"] == "failed"
    assert len(bundle["content_publications"][0]["content_digest"]) == 64
    assert bundle["content_knowledge_links_summary"]["knowledge_ids"] == [ids["knowledge_id"]]
    assert bundle["content_knowledge_links_summary"]["link_count"] == 1
    assert len(bundle["content_knowledge_links_summary"]["links"][0]["content_digest"]) == 64
    assert bundle["recent_post_engagement"][0]["tweet_id"] == "tweet-1"
    assert len(bundle["recent_post_engagement"][0]["content_digest"]) == 64


def test_queue_id_lookup_filters_to_queue_attempts_and_cli_json(db, capsys, monkeypatch):
    ids = _seed_bundle(db)
    db.record_publication_attempt(
        None,
        ids["content_id"],
        "bluesky",
        False,
        attempted_at=(BASE_TIME - timedelta(minutes=2)).isoformat(),
        error="other queue",
    )
    monkeypatch.setattr(
        export_publication_replay_bundle_v2,
        "script_context",
        lambda: _script_context(db),
    )

    result = export_publication_replay_bundle_v2.main(["--queue-id", str(ids["queue_id"])])

    payload = json.loads(capsys.readouterr().out)
    assert result == 0
    assert payload["lookup"]["resolved_content_id"] == ids["content_id"]
    assert payload["lookup"]["resolved_queue_id"] == ids["queue_id"]
    assert [attempt["platform"] for attempt in payload["publication_attempts"]] == ["x", "x"]


def test_redaction_strips_handles_and_urls_from_free_text_only(db):
    ids = _seed_bundle(db)

    bundle = build_publication_replay_bundle(
        db,
        content_id=ids["content_id"],
        redact=True,
        generated_at=BASE_TIME,
    )
    payload = json.loads(format_publication_replay_bundle_json(bundle))
    serialized = json.dumps(payload, sort_keys=True)

    assert "https://example.com/post" not in serialized
    assert "@author" not in serialized
    assert payload["generated_content"]["content"] == (
        f"Debug this failed post from {REDACTED}: {REDACTED}"
    )
    assert payload["generated_content"]["id"] == ids["content_id"]
    assert payload["publish_queue"]["status"] == "failed"
    assert payload["content_variants"][0]["metadata"]["status"] == "draft"
    assert payload["content_knowledge_links_summary"]["links"][0]["source_url"] == (
        "https://example.com/source"
    )


def test_source_digests_are_stable_and_change_when_source_content_changes(db):
    ids = _seed_bundle(db)

    bundle_a = build_publication_replay_bundle(
        db,
        content_id=ids["content_id"],
        generated_at=BASE_TIME,
    )
    bundle_b = build_publication_replay_bundle(
        db,
        content_id=ids["content_id"],
        generated_at=BASE_TIME,
    )

    assert bundle_a["generated_content"]["content_digest"] == (
        bundle_b["generated_content"]["content_digest"]
    )
    assert bundle_a["publication_attempts"][0]["content_digest"] == (
        bundle_b["publication_attempts"][0]["content_digest"]
    )

    db.conn.execute(
        "UPDATE generated_content SET content = ? WHERE id = ?",
        ("Changed replay content", ids["content_id"]),
    )
    db.conn.commit()
    changed = build_publication_replay_bundle(
        db,
        content_id=ids["content_id"],
        generated_at=BASE_TIME,
    )

    assert changed["generated_content"]["content_digest"] != (
        bundle_a["generated_content"]["content_digest"]
    )
    assert changed["publication_attempts"][0]["content_digest"] == (
        bundle_a["publication_attempts"][0]["content_digest"]
    )


def test_unknown_ids_return_actionable_errors(db, capsys, monkeypatch):
    monkeypatch.setattr(
        export_publication_replay_bundle_v2,
        "script_context",
        lambda: _script_context(db),
    )

    with pytest.raises(ValueError, match="generated_content id 999 does not exist"):
        build_publication_replay_bundle(db, content_id=999)
    with pytest.raises(ValueError, match="publish_queue id 999 does not exist"):
        build_publication_replay_bundle(db, queue_id=999)

    result = export_publication_replay_bundle_v2.main(["--content-id", "999"])

    assert result == 1
    assert "error: generated_content id 999 does not exist" in capsys.readouterr().err
