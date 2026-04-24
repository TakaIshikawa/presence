"""Tests for Bluesky reply thread context importing."""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.bluesky_thread_context import (  # noqa: E402
    BlueskyThreadContextError,
    build_reply_context_update,
    normalize_at_uri,
)
from import_bluesky_reply_context import import_context  # noqa: E402


ROOT_URI = "at://did:plc:me/app.bsky.feed.post/root1"
PARENT_URI = "at://did:plc:bob/app.bsky.feed.post/parent1"
INBOUND_URI = "at://did:plc:alice/app.bsky.feed.post/reply1"


def _thread_payload(parent_uri: str = PARENT_URI) -> dict:
    return {
        "thread": {
            "post": {
                "uri": ROOT_URI,
                "cid": "root-cid",
                "author": {
                    "did": "did:plc:me",
                    "handle": "me.bsky.social",
                },
                "record": {"text": "Our original Bluesky post"},
            },
            "replies": [
                {
                    "post": {
                        "uri": parent_uri,
                        "cid": "parent-cid",
                        "author": {
                            "did": "did:plc:bob",
                            "handle": "bob.bsky.social",
                        },
                        "record": {"text": "A parent reply in the branch"},
                    },
                    "replies": [
                        {
                            "post": {
                                "uri": INBOUND_URI,
                                "cid": "inbound-cid",
                                "author": {
                                    "did": "did:plc:alice",
                                    "handle": "alice.bsky.social",
                                },
                                "record": {"text": "What about this branch?"},
                            },
                            "replies": [],
                        }
                    ],
                }
            ],
        }
    }


def _insert_reply(db, *, platform: str = "bluesky", metadata: dict | None = None) -> int:
    return db.insert_reply_draft(
        inbound_tweet_id=INBOUND_URI if platform == "bluesky" else "x-reply-1",
        inbound_author_handle="alice.bsky.social",
        inbound_author_id="did:plc:alice",
        inbound_text="What about this branch?",
        our_tweet_id=ROOT_URI if platform == "bluesky" else "123",
        our_content_id=None,
        our_post_text="Our original Bluesky post",
        draft_text="Draft",
        platform=platform,
        inbound_cid="inbound-cid",
        our_platform_id=ROOT_URI if platform == "bluesky" else "123",
        platform_metadata=json.dumps(metadata or {}),
    )


def test_normalize_at_uri_accepts_supported_references():
    assert normalize_at_uri(ROOT_URI) == ROOT_URI
    assert (
        normalize_at_uri("https://bsky.app/profile/me.bsky.social/post/root1")
        == "at://me.bsky.social/app.bsky.feed.post/root1"
    )
    assert (
        normalize_at_uri("did:plc:me/app.bsky.feed.post/root1")
        == "at://did:plc:me/app.bsky.feed.post/root1"
    )
    assert (
        normalize_at_uri("root1", default_handle="me.bsky.social")
        == "at://me.bsky.social/app.bsky.feed.post/root1"
    )


def test_build_reply_context_update_merges_parent_and_root_metadata():
    row = {
        "id": 7,
        "our_platform_id": ROOT_URI,
        "our_tweet_id": ROOT_URI,
        "platform_metadata": json.dumps(
            {
                "root_uri": ROOT_URI,
                "parent_uri": PARENT_URI,
                "reason": "reply",
            }
        ),
    }

    update = build_reply_context_update(row, thread_payload=_thread_payload())

    metadata = json.loads(update.platform_metadata)
    assert metadata["reason"] == "reply"
    assert metadata["root_uri"] == ROOT_URI
    assert metadata["root_cid"] == "root-cid"
    assert metadata["root_post_text"] == "Our original Bluesky post"
    assert metadata["root_author_handle"] == "me.bsky.social"
    assert metadata["parent_post_uri"] == PARENT_URI
    assert metadata["parent_cid"] == "parent-cid"
    assert metadata["parent_post_text"] == "A parent reply in the branch"
    assert metadata["parent_author_handle"] == "bob.bsky.social"


def test_build_reply_context_update_reports_missing_parent_payload():
    row = {
        "id": 7,
        "our_platform_id": ROOT_URI,
        "our_tweet_id": ROOT_URI,
        "platform_metadata": json.dumps(
            {
                "root_uri": ROOT_URI,
                "parent_uri": "at://did:plc:missing/app.bsky.feed.post/nope",
            }
        ),
    }

    with pytest.raises(BlueskyThreadContextError, match="missing parent post"):
        build_reply_context_update(row, thread_payload=_thread_payload())


def test_import_context_dry_run_reports_without_mutating(db):
    reply_id = _insert_reply(
        db,
        metadata={
            "root_uri": ROOT_URI,
            "parent_uri": PARENT_URI,
        },
    )
    before = db.conn.execute(
        "SELECT platform_metadata FROM reply_queue WHERE id = ?",
        (reply_id,),
    ).fetchone()["platform_metadata"]
    client = MagicMock()
    client.get_post_thread.return_value = _thread_payload()

    result = import_context(db, client, dry_run=True)

    assert result["updated"] == 0
    assert result["rows"] == [
        {
            "id": reply_id,
            "status": "would_update",
            "inbound_tweet_id": INBOUND_URI,
            "changed_keys": result["rows"][0]["changed_keys"],
        }
    ]
    after = db.conn.execute(
        "SELECT platform_metadata FROM reply_queue WHERE id = ?",
        (reply_id,),
    ).fetchone()["platform_metadata"]
    assert after == before


def test_import_context_updates_incomplete_bluesky_rows(db):
    reply_id = _insert_reply(
        db,
        metadata={
            "root_uri": ROOT_URI,
            "parent_uri": PARENT_URI,
        },
    )
    client = MagicMock()
    client.get_post_thread.return_value = _thread_payload()

    result = import_context(db, client)

    assert result["updated"] == 1
    row = db.conn.execute(
        "SELECT platform_metadata FROM reply_queue WHERE id = ?",
        (reply_id,),
    ).fetchone()
    metadata = json.loads(row["platform_metadata"])
    assert metadata["parent_post_text"] == "A parent reply in the branch"
    assert metadata["root_post_text"] == "Our original Bluesky post"


def test_import_context_reports_malformed_rows_without_aborting(db):
    broken_id = _insert_reply(
        db,
        metadata={
            "root_uri": ROOT_URI,
            "parent_uri": PARENT_URI,
        },
    )
    good_id = db.insert_reply_draft(
        inbound_tweet_id="at://did:plc:carol/app.bsky.feed.post/reply2",
        inbound_author_handle="carol.bsky.social",
        inbound_author_id="did:plc:carol",
        inbound_text="Following up",
        our_tweet_id=ROOT_URI,
        our_content_id=None,
        our_post_text="Our original Bluesky post",
        draft_text="Draft",
        platform="bluesky",
        inbound_cid="inbound-cid-2",
        our_platform_id=ROOT_URI,
        platform_metadata=json.dumps(
            {
                "root_uri": ROOT_URI,
                "parent_uri": PARENT_URI,
            }
        ),
    )
    client = MagicMock()
    client.get_post_thread.side_effect = [
        {"thread": {}},
        _thread_payload(),
    ]

    result = import_context(db, client)

    assert result["updated"] == 1
    assert result["errors"][0]["id"] == broken_id
    assert result["rows"][0]["status"] == "error"
    assert result["rows"][1]["id"] == good_id
    assert result["rows"][1]["status"] == "updated"


def test_import_context_ignores_x_reply_rows(db):
    _insert_reply(db, platform="x", metadata={"root_uri": "123"})
    client = MagicMock()

    result = import_context(db, client)

    assert result["inspected"] == 0
    assert result["updated"] == 0
    client.get_post_thread.assert_not_called()
