"""Tests for reply author history context building."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from engagement.reply_history import (  # noqa: E402
    build_reply_author_history,
    format_reply_author_history_text,
    truncate_text,
)
from reply_history import main  # noqa: E402


def _insert_reply(db, tweet_id: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="author-a",
        inbound_text="Thanks for sharing this.",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text="Appreciate the note.",
        status="pending",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_timestamps(
    db,
    reply_id: int,
    *,
    detected_at: str,
    reviewed_at: str | None = None,
    posted_at: str | None = None,
) -> None:
    db.conn.execute(
        """UPDATE reply_queue
           SET detected_at = ?, reviewed_at = ?, posted_at = ?
           WHERE id = ?""",
        (detected_at, reviewed_at, posted_at, reply_id),
    )
    db.conn.commit()


def _mock_script_context(db):
    @contextmanager
    def _ctx():
        yield (SimpleNamespace(), db)

    return _ctx


def test_build_history_matches_handle_case_insensitively(db):
    first = _insert_reply(db, "tw-1", inbound_author_handle="@Alice", status="posted")
    second = _insert_reply(db, "tw-2", inbound_author_handle="alice", status="dismissed")
    _insert_reply(db, "tw-other", inbound_author_handle="bob", inbound_author_id="author-b")
    _set_timestamps(db, first, detected_at="2026-04-20 10:00:00")
    _set_timestamps(db, second, detected_at="2026-04-21 10:00:00")

    history = build_reply_author_history(db, handle="ALICE")

    assert history["matched_count"] == 2
    assert history["query"] == {"handle": "alice", "author_id": None}
    assert history["status_counts"] == {"dismissed": 1, "posted": 1}
    assert [item["inbound_tweet_id"] for item in history["recent_interactions"]] == [
        "tw-2",
        "tw-1",
    ]


def test_build_history_matches_author_id(db):
    first = _insert_reply(db, "tw-1", inbound_author_handle="old", inbound_author_id="same")
    second = _insert_reply(db, "tw-2", inbound_author_handle="new", inbound_author_id="same")
    _insert_reply(db, "tw-other", inbound_author_handle="same", inbound_author_id="other")
    _set_timestamps(db, first, detected_at="2026-04-20 10:00:00")
    _set_timestamps(db, second, detected_at="2026-04-22 10:00:00")

    history = build_reply_author_history(db, author_id="same")

    assert history["matched_count"] == 2
    assert history["query"] == {"handle": None, "author_id": "same"}
    assert [item["author_handle"] for item in history["recent_interactions"]] == [
        "new",
        "old",
    ]


def test_status_counts_include_unknown_for_missing_status(db):
    reply_id = _insert_reply(db, "tw-null-status")
    db.conn.execute("UPDATE reply_queue SET status = NULL WHERE id = ?", (reply_id,))
    db.conn.commit()

    history = build_reply_author_history(db, handle="alice")

    assert history["status_counts"] == {"unknown": 1}


def test_relationship_context_highlights_are_parsed_and_deduped(db):
    context = json.dumps(
        {
            "engagement_stage": 3,
            "stage_name": "Active",
            "dunbar_tier": 2,
            "tier_name": "Key Network",
            "relationship_strength": 0.42,
        }
    )
    _insert_reply(db, "tw-1", relationship_context=context)
    _insert_reply(db, "tw-2", relationship_context=context)
    _insert_reply(db, "tw-3", relationship_context="not json{")

    history = build_reply_author_history(db, handle="alice")

    assert history["relationship_highlights"] == [
        "Active (stage 3) | Key Network (tier 2) | strength: 0.42"
    ]


def test_long_inbound_and_draft_texts_are_truncated_consistently(db):
    long_inbound = "inbound-" + ("x" * 80)
    long_draft = "draft-" + ("y" * 80)
    _insert_reply(db, "tw-long", inbound_text=long_inbound, draft_text=long_draft)

    history = build_reply_author_history(db, handle="alice", text_limit=24)
    item = history["recent_interactions"][0]

    assert item["inbound_text"] == truncate_text(long_inbound, 24)
    assert item["draft_text"] == truncate_text(long_draft, 24)
    assert history["prior_draft_snippets"][0]["draft_text"] == truncate_text(long_draft, 24)
    assert item["inbound_text"].endswith("...")
    assert item["draft_text"].endswith("...")


def test_empty_state_is_json_serializable_and_text_mentions_no_history(db):
    history = build_reply_author_history(db, handle="missing")

    assert history["matched_count"] == 0
    assert history["status_counts"] == {}
    assert history["recent_interactions"] == []
    assert history["prior_draft_snippets"] == []
    assert history["last_interaction_timestamp"] is None
    json.dumps(history)
    assert "No prior interactions found." in format_reply_author_history_text(history)


def test_limit_applies_to_recent_interactions_and_draft_snippets(db):
    for i in range(3):
        reply_id = _insert_reply(db, f"tw-{i}", draft_text=f"draft {i}")
        _set_timestamps(db, reply_id, detected_at=f"2026-04-2{i} 10:00:00")

    history = build_reply_author_history(db, handle="alice", limit=2)

    assert [item["inbound_tweet_id"] for item in history["recent_interactions"]] == [
        "tw-2",
        "tw-1",
    ]
    assert [item["draft_text"] for item in history["prior_draft_snippets"]] == [
        "draft 2",
        "draft 1",
    ]


def test_text_format_includes_counts_recent_interactions_and_highlights(db):
    context = json.dumps({"engagement_stage": 1, "stage_name": "Ambient"})
    _insert_reply(
        db,
        "tw-1",
        inbound_author_handle="alice",
        inbound_text="This was useful",
        draft_text="Glad it helped",
        relationship_context=context,
        status="posted",
    )

    text = format_reply_author_history_text(build_reply_author_history(db, handle="alice"))

    assert "Reply history for @alice" in text
    assert "Status counts: {'posted': 1}" in text
    assert "Ambient (stage 1)" in text
    assert "This was useful" in text
    assert "Draft: \"Glad it helped\"" in text


def test_main_json_output(capsys, db):
    _insert_reply(db, "tw-1", inbound_author_id="author-a", status="posted")

    with patch("reply_history.script_context", _mock_script_context(db)):
        assert main(["--author-id", "author-a", "--format", "json", "--limit", "1"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["matched_count"] == 1
    assert payload["status_counts"] == {"posted": 1}
    assert payload["recent_interactions"][0]["inbound_tweet_id"] == "tw-1"


def test_main_text_output(capsys, db):
    _insert_reply(db, "tw-1", inbound_author_handle="alice")

    with patch("reply_history.script_context", _mock_script_context(db)):
        assert main(["--handle", "@alice", "--format", "text"]) == 0

    output = capsys.readouterr().out
    assert "Reply history for @alice" in output
    assert "Matched interactions: 1" in output


def test_builder_requires_identity(db):
    with pytest.raises(ValueError, match="handle or author_id is required"):
        build_reply_author_history(db)


def test_builder_rejects_blank_handle(db):
    with pytest.raises(ValueError, match="handle or author_id is required"):
        build_reply_author_history(db, handle="@")
