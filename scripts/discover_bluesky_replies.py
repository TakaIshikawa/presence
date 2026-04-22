#!/usr/bin/env python3
"""Discover inbound Bluesky replies and queue drafted responses for review."""

from __future__ import annotations

import json
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from output.bluesky_client import BlueskyClient
from output.api_rate_guard import should_skip_optional_api_call
from engagement.reply_drafter import ReplyDrafter
from knowledge.embeddings import VoyageEmbeddings
from knowledge.store import KnowledgeStore

logger = logging.getLogger(__name__)


def _bluesky_post_url(handle: str, uri: str) -> str | None:
    if not handle or not uri:
        return None
    return f"https://bsky.app/profile/{handle}/post/{uri.split('/')[-1]}"


def _reply_refs(notification: dict) -> list[str]:
    refs = []
    for key in ("parent_uri", "root_uri", "reason_subject"):
        uri = notification.get(key)
        if uri and uri not in refs:
            refs.append(uri)
    record = notification.get("record") or {}
    reply = record.get("reply") or {}
    for name in ("parent", "root"):
        ref = reply.get(name) or {}
        uri = ref.get("uri")
        if uri and uri not in refs:
            refs.append(uri)
    return refs


def _reply_ref_metadata(notification: dict) -> dict:
    metadata = {}
    for source_key, target_key in (
        ("reply_root", "reply_root"),
        ("reply_parent", "reply_parent"),
    ):
        ref = notification.get(source_key)
        if isinstance(ref, dict) and (ref.get("uri") or ref.get("cid")):
            metadata[target_key] = {
                k: v for k, v in {
                    "uri": ref.get("uri"),
                    "cid": ref.get("cid"),
                }.items() if v
            }

    record = notification.get("record") or {}
    reply = record.get("reply") or {}
    for name in ("root", "parent"):
        target_key = f"reply_{name}"
        if target_key in metadata:
            continue
        ref = reply.get(name) or {}
        if ref.get("uri") or ref.get("cid"):
            metadata[target_key] = {
                k: v for k, v in {
                    "uri": ref.get("uri"),
                    "cid": ref.get("cid"),
                }.items() if v
            }

    for key in ("root_uri", "root_cid", "parent_uri", "parent_cid"):
        value = notification.get(key)
        if value:
            metadata[key] = value
    return metadata


def _conversation_context_metadata(context: dict | None) -> dict:
    if not isinstance(context, dict) or not context:
        return {}
    metadata = {}
    for key in (
        "parent_post_uri",
        "parent_post_text",
        "quoted_text",
        "sibling_replies",
    ):
        value = context.get(key)
        if value:
            metadata[key] = value
    return metadata


def _metadata_for_notification(
    notification: dict,
    conversation_context: dict | None,
) -> str:
    record = notification.get("record") or {}
    metadata = {
        "reason": notification.get("reason"),
        "reason_subject": notification.get("reason_subject"),
        "indexed_at": notification.get("indexed_at"),
        "is_read": notification.get("is_read"),
        "record_created_at": record.get("created_at"),
        "reply_refs": _reply_refs(notification),
    }
    metadata.update(_reply_ref_metadata(notification))
    metadata.update(_conversation_context_metadata(conversation_context))
    return json.dumps({k: v for k, v in metadata.items() if v is not None})


def _find_our_content(db, notification: dict) -> tuple[dict | None, str | None]:
    for uri in _reply_refs(notification):
        content = db.get_content_by_bluesky_uri(uri)
        if content:
            return content, uri
    return None, None


def discover(config, db, client: BlueskyClient, drafter: ReplyDrafter) -> int:
    """Discover unread Bluesky mentions/replies and insert reply drafts."""
    bluesky_config = getattr(config, "bluesky", None)
    if not bluesky_config or not getattr(bluesky_config, "enabled", False):
        return 0
    if should_skip_optional_api_call(
        config,
        db,
        "bluesky",
        operation="Bluesky inbound reply discovery",
        logger=logger,
    ):
        return 0

    cursor = db.get_platform_reply_cursor("bluesky")
    notifications, next_cursor = client.get_unread_mentions(cursor=cursor, limit=50)
    inserted = 0

    for notification in notifications:
        inbound_uri = notification.get("uri")
        if not inbound_uri or db.is_reply_processed(inbound_uri):
            continue

        author = notification.get("author") or {}
        author_handle = author.get("handle") or "unknown"
        if author_handle.lower() == bluesky_config.handle.lower():
            continue

        our_content, our_uri = _find_our_content(db, notification)
        if not our_content or not our_uri:
            continue

        record = notification.get("record") or {}
        inbound_text = record.get("text") or ""
        parent_uri = notification.get("parent_uri") or our_uri
        root_uri = notification.get("root_uri") or our_uri

        try:
            conversation_context = client.get_conversation_context(
                root_uri=root_uri,
                parent_uri=parent_uri,
                inbound_uri=inbound_uri,
            )
            if not isinstance(conversation_context, dict):
                conversation_context = {}
        except Exception as e:
            logger.debug("Failed to fetch Bluesky conversation context: %s", e)
            conversation_context = {}

        if parent_uri == our_uri and "parent_post_text" not in conversation_context:
            conversation_context["parent_post_uri"] = our_uri
            conversation_context["parent_post_text"] = our_content["content"]

        try:
            draft_result = drafter.draft_with_lineage(
                our_post=our_content["content"],
                their_reply=inbound_text,
                their_handle=author_handle,
                self_handle=bluesky_config.handle,
                person_context=None,
                conversation_context=conversation_context,
            )
        except Exception as e:
            logger.warning("Failed to draft Bluesky reply for %s: %s", inbound_uri, e)
            continue

        reply_queue_id = db.insert_reply_draft(
            inbound_tweet_id=inbound_uri,
            inbound_author_handle=author_handle,
            inbound_author_id=author.get("did") or "",
            inbound_text=inbound_text,
            our_tweet_id=our_uri,
            our_content_id=our_content["id"],
            our_post_text=our_content["content"],
            draft_text=draft_result.reply_text,
            platform="bluesky",
            inbound_url=_bluesky_post_url(author_handle, inbound_uri),
            inbound_cid=notification.get("cid"),
            our_platform_id=our_uri,
            platform_metadata=_metadata_for_notification(
                notification,
                conversation_context,
            ),
        )

        if draft_result.knowledge_ids:
            try:
                db.insert_reply_knowledge_links(
                    reply_queue_id,
                    draft_result.knowledge_ids,
                )
            except sqlite3.Error as e:
                logger.warning("Failed to store Bluesky reply knowledge links: %s", e)

        inserted += 1

    if next_cursor and next_cursor != cursor:
        db.set_platform_reply_cursor("bluesky", next_cursor)

    return inserted


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (config, db):
        bluesky_config = getattr(config, "bluesky", None)
        if not bluesky_config or not getattr(bluesky_config, "enabled", False):
            logger.info("Bluesky is disabled")
            return

        knowledge_store = None
        if getattr(config, "embeddings", None):
            embedder = VoyageEmbeddings(
                api_key=config.embeddings.api_key,
                model=config.embeddings.model,
            )
            knowledge_store = KnowledgeStore(db.conn, embedder)

        drafter = ReplyDrafter(
            api_key=config.anthropic.api_key,
            model=config.synthesis.model,
            timeout=config.timeouts.anthropic_seconds,
            knowledge_store=knowledge_store,
            restricted_prompt_behavior=getattr(
                getattr(config, "curated_sources", None),
                "restricted_prompt_behavior",
                "strict",
            ),
            db=db,
        )
        client = BlueskyClient(
            bluesky_config.handle,
            bluesky_config.app_password,
        )

        inserted = discover(config, db, client, drafter)
        logger.info("Inserted %d Bluesky reply drafts.", inserted)
        update_monitoring("discover-bluesky-replies")


if __name__ == "__main__":
    main()
