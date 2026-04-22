"""Tests for scripts/discover_bluesky_replies.py."""

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from discover_bluesky_replies import discover


ROOT_URI = "at://did:plc:me/app.bsky.feed.post/root1"
INBOUND_URI = "at://did:plc:alice/app.bsky.feed.post/reply1"


def _config() -> SimpleNamespace:
    return SimpleNamespace(
        bluesky=SimpleNamespace(
            enabled=True,
            handle="me.bsky.social",
            app_password="app-password",
        ),
        rate_limits=SimpleNamespace(bluesky_min_remaining=0),
    )


def _notification(
    uri: str = INBOUND_URI,
    text: str = "How does this work on Bluesky?",
) -> dict:
    return {
        "uri": uri,
        "cid": "inbound-cid",
        "reason": "reply",
        "reason_subject": ROOT_URI,
        "indexed_at": "2026-04-21T12:00:00Z",
        "is_read": False,
        "root_uri": ROOT_URI,
        "root_cid": "root-cid",
        "parent_uri": ROOT_URI,
        "parent_cid": "root-cid",
        "reply_root": {"uri": ROOT_URI, "cid": "root-cid"},
        "reply_parent": {"uri": ROOT_URI, "cid": "root-cid"},
        "author": {
            "did": "did:plc:alice",
            "handle": "alice.bsky.social",
            "display_name": "Alice",
        },
        "record": {
            "text": text,
            "created_at": "2026-04-21T11:59:00Z",
            "reply": {
                "root": {"uri": ROOT_URI, "cid": "root-cid"},
                "parent": {"uri": ROOT_URI, "cid": "root-cid"},
            },
        },
    }


def _insert_bluesky_content(db) -> int:
    cursor = db.conn.execute(
        """INSERT INTO generated_content
           (content_type, content, published, bluesky_uri)
           VALUES (?, ?, ?, ?)""",
        ("x_post", "Our original Bluesky post", 1, ROOT_URI),
    )
    db.conn.commit()
    return cursor.lastrowid


def _client(notification: dict | None = None, next_cursor: str | None = "next-cursor"):
    client = MagicMock()
    client.get_unread_mentions.return_value = (
        [notification] if notification else [],
        next_cursor,
    )
    client.get_conversation_context.return_value = {
        "parent_post_uri": ROOT_URI,
        "parent_post_text": "Our original Bluesky post",
        "sibling_replies": [
            {
                "uri": "at://did:plc:bob/app.bsky.feed.post/sibling",
                "cid": "sibling-cid",
                "text": "I had the same question",
                "author_handle": "bob.bsky.social",
            }
        ],
    }
    return client


def _drafter(reply_text: str = "It works through notifications."):
    drafter = MagicMock()
    drafter.draft_with_lineage.return_value = SimpleNamespace(
        reply_text=reply_text,
        knowledge_ids=[],
    )
    return drafter


def test_cursor_persistence(db):
    db.set_platform_reply_cursor("bluesky", "old-cursor")
    client = _client(notification=None, next_cursor="new-cursor")

    inserted = discover(_config(), db, client, _drafter())

    assert inserted == 0
    client.get_unread_mentions.assert_called_once_with(
        cursor="old-cursor",
        limit=50,
    )
    assert db.get_platform_reply_cursor("bluesky") == "new-cursor"


def test_duplicate_suppression(db):
    _insert_bluesky_content(db)
    existing_id = db.insert_reply_draft(
        inbound_tweet_id=INBOUND_URI,
        inbound_author_handle="alice.bsky.social",
        inbound_author_id="did:plc:alice",
        inbound_text="Already queued",
        our_tweet_id=ROOT_URI,
        our_content_id=1,
        our_post_text="Our original Bluesky post",
        draft_text="Existing draft",
        platform="bluesky",
    )
    drafter = _drafter()

    inserted = discover(_config(), db, _client(_notification()), drafter)

    assert inserted == 0
    assert existing_id > 0
    drafter.draft_with_lineage.assert_not_called()
    count = db.conn.execute("SELECT COUNT(*) FROM reply_queue").fetchone()[0]
    assert count == 1


def test_metadata_storage(db):
    content_id = _insert_bluesky_content(db)

    inserted = discover(_config(), db, _client(_notification()), _drafter())

    assert inserted == 1
    row = db.conn.execute("SELECT * FROM reply_queue").fetchone()
    assert row["platform"] == "bluesky"
    assert row["inbound_tweet_id"] == INBOUND_URI
    assert row["inbound_cid"] == "inbound-cid"
    assert row["our_tweet_id"] == ROOT_URI
    assert row["our_platform_id"] == ROOT_URI
    assert row["our_content_id"] == content_id
    assert row["inbound_url"] == "https://bsky.app/profile/alice.bsky.social/post/reply1"

    metadata = json.loads(row["platform_metadata"])
    assert metadata["reply_root"] == {"uri": ROOT_URI, "cid": "root-cid"}
    assert metadata["reply_parent"] == {"uri": ROOT_URI, "cid": "root-cid"}
    assert metadata["root_uri"] == ROOT_URI
    assert metadata["root_cid"] == "root-cid"
    assert metadata["parent_uri"] == ROOT_URI
    assert metadata["parent_cid"] == "root-cid"
    assert metadata["reply_refs"] == [ROOT_URI]
    assert metadata["parent_post_text"] == "Our original Bluesky post"
    assert metadata["sibling_replies"][0]["author_handle"] == "bob.bsky.social"


def test_draft_creation(db):
    _insert_bluesky_content(db)
    drafter = _drafter("That tradeoff shows up quickly once replies branch.")
    client = _client(_notification(text="What happens when replies branch?"))

    inserted = discover(_config(), db, client, drafter)

    assert inserted == 1
    row = db.conn.execute("SELECT draft_text FROM reply_queue").fetchone()
    assert row["draft_text"] == "That tradeoff shows up quickly once replies branch."
    drafter.draft_with_lineage.assert_called_once_with(
        our_post="Our original Bluesky post",
        their_reply="What happens when replies branch?",
        their_handle="alice.bsky.social",
        self_handle="me.bsky.social",
        person_context=None,
        conversation_context={
            "parent_post_uri": ROOT_URI,
            "parent_post_text": "Our original Bluesky post",
            "sibling_replies": [
                {
                    "uri": "at://did:plc:bob/app.bsky.feed.post/sibling",
                    "cid": "sibling-cid",
                    "text": "I had the same question",
                    "author_handle": "bob.bsky.social",
                }
            ],
        },
    )
