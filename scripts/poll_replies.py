#!/usr/bin/env python3
"""Poll for replies to our published posts and draft responses."""

import json
import signal
import sqlite3
import sys
import logging
from pathlib import Path
from types import FrameType

import anthropic
import tweepy
from atproto.exceptions import AtProtocolError

WATCHDOG_TIMEOUT = 600  # 10 minutes

logger = logging.getLogger(__name__)


def _timeout_handler(signum: int, frame: FrameType | None) -> None:
    logger.error("WATCHDOG: Reply poll exceeded 10-minute timeout, exiting")
    sys.exit(1)

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from output.x_client import XClient
from output.bluesky_client import BlueskyClient
from output.x_api_guard import (
    get_x_api_block_reason,
    mark_x_api_blocked_if_needed,
)
from engagement.reply_drafter import ReplyDrafter
from knowledge.embeddings import VoyageEmbeddings
from knowledge.store import KnowledgeStore


def _get_authenticated_x_identity(db, x_client: XClient) -> tuple[str, str]:
    """Return cached authenticated X user id/handle, resolving once if needed."""
    cached_id = db.get_meta("x_authenticated_user_id")
    cached_handle = db.get_meta("x_authenticated_username")
    if isinstance(cached_id, str) and cached_id and isinstance(cached_handle, str) and cached_handle:
        return cached_id, cached_handle

    user_id, username = x_client.get_authenticated_user()
    db.set_meta("x_authenticated_user_id", user_id)
    db.set_meta("x_authenticated_username", username)
    return user_id, username


def _bluesky_post_url(handle: str, uri: str) -> str | None:
    if not handle or not uri:
        return None
    return f"https://bsky.app/profile/{handle}/post/{uri.split('/')[-1]}"


def _bluesky_reply_refs(notification: dict) -> list[str]:
    refs = []
    record = notification.get("record") or {}
    reply = record.get("reply") or {}
    for key in ("parent", "root"):
        ref = reply.get(key) or {}
        uri = ref.get("uri")
        if uri and uri not in refs:
            refs.append(uri)
    reason_subject = notification.get("reason_subject")
    if reason_subject and reason_subject not in refs:
        refs.append(reason_subject)
    return refs


def _poll_bluesky_replies(
    config,
    db,
    drafter: ReplyDrafter,
    replies_today: int,
    max_daily: int,
) -> tuple[int, int]:
    """Poll Bluesky notifications and queue draft replies."""
    bluesky_config = getattr(config, "bluesky", None)
    if not bluesky_config or not getattr(bluesky_config, "enabled", False):
        return 0, 0

    remaining_cap = max_daily - replies_today
    if remaining_cap <= 0:
        return 0, 0

    client = BlueskyClient(
        bluesky_config.handle,
        bluesky_config.app_password,
    )
    cursor = db.get_platform_reply_cursor("bluesky")
    logger.info(f"Polling Bluesky notifications cursor={cursor or 'None'}")

    try:
        notifications, next_cursor = client.get_notifications(
            cursor=cursor,
            limit=50,
        )
    except AtProtocolError as e:
        logger.error(f"Error fetching Bluesky notifications: {e}")
        return 0, 0

    if not notifications:
        logger.info("No new Bluesky notifications")
        if next_cursor and next_cursor != cursor:
            db.set_platform_reply_cursor("bluesky", next_cursor)
        return 0, 0

    logger.info(f"Found {len(notifications)} Bluesky notifications")
    drafted = 0
    skipped = 0

    for notification in notifications:
        if drafted >= remaining_cap:
            logger.info(f"Daily reply cap reached during Bluesky processing ({replies_today + drafted}/{max_daily})")
            break

        reason = notification.get("reason")
        if reason not in {"mention", "reply"}:
            skipped += 1
            continue

        inbound_uri = notification.get("uri")
        if not inbound_uri:
            skipped += 1
            continue

        if db.is_reply_processed(inbound_uri):
            skipped += 1
            continue

        author = notification.get("author") or {}
        author_handle = author.get("handle") or "unknown"
        if author_handle == bluesky_config.handle:
            skipped += 1
            continue

        our_content = None
        our_uri = None
        for candidate_uri in _bluesky_reply_refs(notification):
            our_content = db.get_content_by_bluesky_uri(candidate_uri)
            if our_content:
                our_uri = candidate_uri
                break

        if not our_content or not our_uri:
            skipped += 1
            continue

        record = notification.get("record") or {}
        inbound_text = record.get("text") or ""
        logger.info(f"  Drafting Bluesky reply to @{author_handle}: \"{inbound_text[:60]}...\"")

        try:
            draft_result = drafter.draft_with_lineage(
                our_post=our_content["content"],
                their_reply=inbound_text,
                their_handle=author_handle,
                self_handle=bluesky_config.handle,
                person_context=None,
            )
            draft = draft_result.reply_text
            knowledge_ids = draft_result.knowledge_ids
        except (anthropic.APIError, anthropic.APIConnectionError, anthropic.APITimeoutError, anthropic.RateLimitError) as e:
            logger.error(f"  Error drafting Bluesky reply: {e}")
            continue

        metadata = {
            "reason": reason,
            "reason_subject": notification.get("reason_subject"),
            "indexed_at": notification.get("indexed_at"),
            "record_created_at": record.get("created_at"),
            "reply_refs": _bluesky_reply_refs(notification),
        }
        reply_queue_id = db.insert_reply_draft(
            inbound_tweet_id=inbound_uri,
            inbound_author_handle=author_handle,
            inbound_author_id=author.get("did") or "",
            inbound_text=inbound_text,
            our_tweet_id=our_uri,
            our_content_id=our_content["id"],
            our_post_text=our_content["content"],
            draft_text=draft,
            platform="bluesky",
            inbound_url=_bluesky_post_url(author_handle, inbound_uri),
            inbound_cid=notification.get("cid"),
            our_platform_id=our_uri,
            platform_metadata=json.dumps(metadata),
        )

        if knowledge_ids:
            try:
                db.insert_reply_knowledge_links(reply_queue_id, knowledge_ids)
            except sqlite3.Error as e:
                logger.warning(f"  Failed to store Bluesky knowledge links: {e}")

        logger.info(f"  Bluesky draft: \"{draft[:80]}...\"" if len(draft) > 80 else f"  Bluesky draft: \"{draft}\"")
        drafted += 1

    if next_cursor and next_cursor != cursor:
        db.set_platform_reply_cursor("bluesky", next_cursor)

    return drafted, skipped


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(WATCHDOG_TIMEOUT)

    with script_context() as (config, db):
        block_reason = get_x_api_block_reason(db)
        if block_reason:
            logger.warning(f"X API circuit breaker active; skipping replies: {block_reason}")
            update_monitoring("poll-replies")
            return

        # Check if replies are enabled
        if config.replies and not config.replies.enabled:
            logger.info("Replies disabled in config, skipping")
            update_monitoring("poll-replies")
            return

        x_client = XClient(
            config.x.api_key,
            config.x.api_secret,
            config.x.access_token,
            config.x.access_token_secret
        )

        # Initialize knowledge store for semantic reply enrichment
        knowledge_store = None
        if config.embeddings:
            embedder = VoyageEmbeddings(
                api_key=config.embeddings.api_key,
                model=config.embeddings.model,
            )
            knowledge_store = KnowledgeStore(db.conn, embedder)
            logger.info("Knowledge store initialized for reply enrichment")

        drafter = ReplyDrafter(
            api_key=config.anthropic.api_key,
            model=config.synthesis.model,
            timeout=config.timeouts.anthropic_seconds,
            knowledge_store=knowledge_store,
        )

        # Cultivate integration (optional — works without it)
        bridge = None
        evaluator = None
        if config.cultivate and config.cultivate.enabled:
            from engagement.cultivate_bridge import CultivateBridge
            bridge = CultivateBridge.try_connect(config.cultivate.db_path)
            if bridge:
                logger.info("Cultivate integration active")
            else:
                logger.info("Cultivate DB not found, continuing without enrichment")

            from engagement.reply_evaluator import ReplyEvaluator
            evaluator = ReplyEvaluator(
                api_key=config.anthropic.api_key,
                model=config.synthesis.model,
            )

        # Check daily reply cap
        max_daily = config.replies.max_daily_replies if config.replies else 10
        replies_today = db.count_replies_today()
        if replies_today >= max_daily:
            logger.info(f"Daily reply cap reached ({replies_today}/{max_daily}), skipping")
            if bridge:
                bridge.close()
            update_monitoring("poll-replies")
            return

        # Get our user ID for filtering
        try:
            my_user_id, my_handle = _get_authenticated_x_identity(db, x_client)
        except tweepy.TweepyException as e:
            mark_x_api_blocked_if_needed(db, e)
            logger.error(f"Error fetching authenticated user: {e}")
            if bridge:
                bridge.close()
            update_monitoring("poll-replies")
            return

        # Load cursor
        since_id = db.get_last_mention_id()
        logger.info(f"Polling mentions since_id={since_id or 'None'}")

        # Fetch mentions
        try:
            mentions, users_by_id = x_client.get_mentions(
                since_id=since_id, max_results=50, user_id=my_user_id
            )
        except tweepy.TweepyException as e:
            mark_x_api_blocked_if_needed(db, e)
            logger.error(f"Error fetching mentions: {e}")
            if bridge:
                bridge.close()
            update_monitoring("poll-replies")
            return

        if not mentions:
            logger.info("No new mentions")
            if bridge:
                bridge.close()
            bsky_drafted, bsky_skipped = _poll_bluesky_replies(
                config,
                db,
                drafter,
                replies_today,
                max_daily,
            )
            update_monitoring("poll-replies")
            logger.info(f"Done. {bsky_drafted} drafted, {bsky_skipped} skipped.")
            return

        logger.info(f"Found {len(mentions)} mentions")

        # Track highest mention ID for cursor
        max_mention_id = since_id
        drafted = 0
        skipped = 0
        remaining_cap = max_daily - replies_today

        try:
            for mention in mentions:
                tweet_id = mention["id"]

                # Update cursor (mention IDs are monotonically increasing)
                if max_mention_id is None or tweet_id > max_mention_id:
                    max_mention_id = tweet_id

                # Skip if already processed
                if db.is_reply_processed(tweet_id):
                    skipped += 1
                    continue

                # Skip self-mentions (our own tweets)
                if mention["author_id"] == my_user_id:
                    skipped += 1
                    continue

                # Skip if not a reply to us (just a mention in someone else's tweet)
                if mention["in_reply_to_user_id"] != my_user_id:
                    skipped += 1
                    continue

                # Find the conversation root — this should be one of our published tweets
                # The conversation_id is the root tweet of the thread
                conversation_id = mention.get("conversation_id")
                our_content = None
                our_tweet_id = None

                if conversation_id:
                    our_content = db.get_content_by_tweet_id(conversation_id)
                    if our_content:
                        our_tweet_id = conversation_id

                if not our_content:
                    # Not a reply to one of our tracked posts
                    skipped += 1
                    continue

                # Check daily cap
                if drafted >= remaining_cap:
                    logger.info(f"Daily reply cap reached during processing ({replies_today + drafted}/{max_daily})")
                    break

                # Get author handle
                author_handle = "unknown"
                author_id = mention["author_id"]
                if author_id in users_by_id:
                    author_handle = users_by_id[author_id]["username"]

                # Cultivate: look up relationship context
                person_context = None
                if bridge and config.cultivate.enrich_replies:
                    person_context = bridge.get_person_context(author_handle)
                    if person_context:
                        logger.info(f"  Context: {person_context.stage_name} (stage {person_context.engagement_stage}), "
                              f"{person_context.tier_name} (tier {person_context.dunbar_tier})")

                # Cultivate: forward mention event
                if bridge and config.cultivate.forward_mentions:
                    try:
                        bridge.record_mention_event(
                            tweet_id=tweet_id,
                            author_x_id=author_id,
                            author_handle=author_handle,
                            text=mention["text"],
                            created_at=mention.get("created_at", ""),
                        )
                    except (sqlite3.Error, sqlite3.OperationalError) as e:
                        logger.warning(f"  Warning: failed to forward mention to cultivate: {e}")

                # Draft reply (enriched with relationship context and knowledge if available)
                logger.info(f"  Drafting reply to @{author_handle}: \"{mention['text'][:60]}...\"")
                try:
                    draft_result = drafter.draft_with_lineage(
                        our_post=our_content["content"],
                        their_reply=mention["text"],
                        their_handle=author_handle,
                        self_handle=my_handle,
                        person_context=person_context,
                    )
                    draft = draft_result.reply_text
                    knowledge_ids = draft_result.knowledge_ids
                    if knowledge_ids:
                        logger.info(f"  Used {len(knowledge_ids)} knowledge insights")
                except (anthropic.APIError, anthropic.APIConnectionError, anthropic.APITimeoutError, anthropic.RateLimitError) as e:
                    logger.error(f"  Error drafting reply: {e}")
                    continue

                # Quality evaluation (if evaluator configured)
                relationship_context = None
                quality_score = None
                quality_flags = None
                if person_context:
                    relationship_context = person_context.to_json()
                if evaluator:
                    threshold = config.cultivate.reply_quality_threshold
                    eval_result = evaluator.evaluate(
                        draft=draft,
                        our_post=our_content["content"],
                        their_reply=mention["text"],
                        threshold=threshold,
                        person_context=person_context,
                    )
                    quality_score = eval_result.score
                    quality_flags = json.dumps(eval_result.flags) if eval_result.flags else None
                    if not eval_result.passes:
                        flag_str = ", ".join(eval_result.flags) if eval_result.flags else "low score"
                        logger.info(f"  Quality flag: {eval_result.score:.1f}/10 ({flag_str})")

                # Store in queue
                reply_queue_id = db.insert_reply_draft(
                    inbound_tweet_id=tweet_id,
                    inbound_author_handle=author_handle,
                    inbound_author_id=author_id,
                    inbound_text=mention["text"],
                    our_tweet_id=our_tweet_id,
                    our_content_id=our_content["id"],
                    our_post_text=our_content["content"],
                    draft_text=draft,
                    relationship_context=relationship_context,
                    quality_score=quality_score,
                    quality_flags=quality_flags,
                )

                # Store knowledge lineage
                if knowledge_ids:
                    try:
                        db.insert_reply_knowledge_links(reply_queue_id, knowledge_ids)
                    except sqlite3.Error as e:
                        logger.warning(f"  Failed to store knowledge links: {e}")

                logger.info(f"  Draft: \"{draft[:80]}...\"" if len(draft) > 80 else f"  Draft: \"{draft}\"")
                drafted += 1

            # Update cursor
            if max_mention_id and max_mention_id != since_id:
                db.set_last_mention_id(max_mention_id)

        finally:
            # Ensure bridge is closed even if an error occurs
            if bridge:
                bridge.close()

        bsky_drafted, bsky_skipped = _poll_bluesky_replies(
            config,
            db,
            drafter,
            replies_today + drafted,
            max_daily,
        )
        drafted += bsky_drafted
        skipped += bsky_skipped

    update_monitoring("poll-replies")
    logger.info(f"Done. {drafted} drafted, {skipped} skipped.")


if __name__ == "__main__":
    main()
