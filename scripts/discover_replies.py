#!/usr/bin/env python3
"""Discover proactive reply opportunities from curated timelines and optional search."""

import json
import logging
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from output.x_client import XClient
from output.x_api_guard import (
    get_x_api_block_reason,
    mark_x_api_blocked_if_needed,
)
from engagement.reply_drafter import ReplyDrafter
from knowledge.embeddings import VoyageEmbeddings, deserialize_embedding, cosine_similarity
from knowledge.store import KnowledgeStore
from knowledge.curated_accounts import get_active_x_accounts

logger = logging.getLogger(__name__)

DEFAULT_MAX_ACCOUNTS_PER_RUN = 25
DEFAULT_TWEETS_PER_ACCOUNT = 5


def _last_x_error(x_client) -> str | None:
    error = getattr(x_client, "last_error", None)
    return error if isinstance(error, str) and error else None


def _cached_user_id(db, x_client, username: str) -> str | None:
    """Resolve a username once and persist the mapping in meta."""
    normalized = username.lstrip("@").lower()
    key = f"x_user_id:{normalized}"
    cached = db.get_meta(key)
    if cached:
        return cached

    user_id = x_client.get_user_id(normalized)
    if user_id:
        db.set_meta(key, user_id)
    return user_id


def _is_recent(tweet: dict, max_age_hours: int) -> bool:
    """Check if a tweet is within the age window."""
    created = tweet.get("created_at", "")
    if not created:
        return False
    try:
        ts = datetime.fromisoformat(created.replace("Z", "+00:00"))
        cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        return ts >= cutoff
    except (ValueError, TypeError):
        return False


def _batch_score_relevance(
    candidates: list[dict],
    knowledge_store: KnowledgeStore,
    batch_size: int = 20,
) -> None:
    """Score candidates against knowledge base using batch embedding.

    Embeds candidate texts in small batches with rate-limit pauses,
    loads knowledge embeddings once, and computes cosine similarity
    locally. Mutates each candidate dict to set 'relevance' and
    'knowledge_context' keys.
    """
    import time
    from knowledge.store import KnowledgeItem

    if not candidates:
        return

    # Load full knowledge rows once (for both scoring and context)
    cursor = knowledge_store.conn.execute(
        """SELECT * FROM knowledge
           WHERE embedding IS NOT NULL AND approved = 1
             AND source_type IN ('own_post', 'own_conversation')"""
    )
    knowledge_rows = cursor.fetchall()

    if not knowledge_rows:
        logger.info("No knowledge embeddings found — all relevance scores 0.0")
        for c in candidates:
            c["relevance"] = 0.0
            c["knowledge_context"] = []
        return

    knowledge_embeddings = [
        deserialize_embedding(row["embedding"]) for row in knowledge_rows
    ]

    # Batch embed candidate texts with rate-limit pauses
    texts = [c["text"] for c in candidates]
    all_embeddings = []
    total_batches = (len(texts) - 1) // batch_size + 1
    for i in range(0, len(texts), batch_size):
        if i > 0:
            time.sleep(21)  # Stay under 3 RPM
        batch = texts[i : i + batch_size]
        batch_num = i // batch_size + 1
        logger.info(f"Embedding batch {batch_num}/{total_batches} ({len(batch)} texts)")

        # Retry with exponential backoff on rate limit
        for attempt in range(3):
            try:
                embeddings = knowledge_store.embedder.embed_batch(batch)
                all_embeddings.extend(embeddings)
                break
            except Exception as e:
                if attempt < 2:
                    wait = 30 * (attempt + 1)
                    logger.warning(f"Batch {batch_num} attempt {attempt + 1} failed, retrying in {wait}s: {e}")
                    time.sleep(wait)
                else:
                    logger.warning(f"Batch {batch_num} failed after 3 attempts: {e}")
                    for idx, c in enumerate(candidates):
                        if idx < len(all_embeddings):
                            best = max(cosine_similarity(all_embeddings[idx], k) for k in knowledge_embeddings)
                            c["relevance"] = best
                            c["knowledge_context"] = []
                        else:
                            c["relevance"] = 0.0
                            c["knowledge_context"] = []
                    return

    # Score each candidate and pre-fetch top-3 knowledge items
    for c, emb in zip(candidates, all_embeddings):
        scored = []
        for idx, k_emb in enumerate(knowledge_embeddings):
            sim = cosine_similarity(emb, k_emb)
            if sim >= 0.40:
                scored.append((idx, sim))
        scored.sort(key=lambda x: x[1], reverse=True)

        c["relevance"] = scored[0][1] if scored else 0.0

        # Build top-3 KnowledgeItem tuples for the drafter
        top_items = []
        for row_idx, sim in scored[:3]:
            row = knowledge_rows[row_idx]
            item = KnowledgeItem(
                id=row["id"],
                source_type=row["source_type"],
                source_id=row["source_id"],
                source_url=row["source_url"],
                author=row["author"],
                content=row["content"],
                insight=row["insight"],
                embedding=None,
                attribution_required=bool(row["attribution_required"]),
                approved=bool(row["approved"]),
                created_at=row["created_at"],
            )
            top_items.append((item, sim))
        c["knowledge_context"] = top_items


def discover(config, db, x_client, knowledge_store, drafter, bridge=None):
    """Run the discovery pipeline. Returns count of actions inserted."""
    block_reason = get_x_api_block_reason(db)
    if block_reason:
        logger.warning(f"X API circuit breaker active; skipping discovery: {block_reason}")
        return 0

    proactive = config.proactive

    # 1. Source: Curated timelines (config + DB-approved accounts)
    candidates = []
    max_accounts = getattr(proactive, "max_accounts_per_run", DEFAULT_MAX_ACCOUNTS_PER_RUN)
    tweets_per_account = getattr(proactive, "tweets_per_account", DEFAULT_TWEETS_PER_ACCOUNT)
    accounts = get_active_x_accounts(
        config,
        db,
        limit=max_accounts,
        cursor_key="discover_replies_x_account_cursor",
    )
    if accounts:
        logger.info(
            "Fetching curated timelines for %d accounts (cap=%d, tweets/account=%d)",
            len(accounts),
            max_accounts,
            tweets_per_account,
        )
        for account in accounts:
            try:
                user_id = _cached_user_id(db, x_client, account.identifier)
                if not user_id:
                    if mark_x_api_blocked_if_needed(db, _last_x_error(x_client)):
                        logger.warning("X API blocked while resolving accounts; stopping discovery")
                        break
                    logger.warning(f"Could not resolve user ID for @{account.identifier}")
                    continue
                tweets = x_client.get_user_tweets(user_id, count=tweets_per_account)
                if mark_x_api_blocked_if_needed(db, _last_x_error(x_client)):
                    logger.warning("X API blocked while fetching timelines; stopping discovery")
                    break
                for tweet in tweets:
                    tweet["discovery_source"] = "curated_timeline"
                    tweet["author_handle"] = account.identifier
                    candidates.append(tweet)
            except Exception as e:
                if mark_x_api_blocked_if_needed(db, e):
                    logger.warning("X API blocked while fetching timelines; stopping discovery")
                    break
                logger.warning(f"Failed to fetch timeline for @{account.identifier}: {e}")

    # 2. Source: Search (if enabled)
    if proactive.search_enabled and proactive.search_keywords:
        for kw in proactive.search_keywords:
            try:
                results = x_client.search_tweets(kw, max_results=20)
                if mark_x_api_blocked_if_needed(db, _last_x_error(x_client)):
                    logger.warning("X API blocked while searching; stopping search")
                    break
                for tweet in results:
                    tweet["discovery_source"] = "search"
                    tweet["author_handle"] = tweet.get("author_username", "")
                    candidates.append(tweet)
            except Exception as e:
                logger.warning(f"Search failed for '{kw}': {e}")

    logger.info(f"Sourced {len(candidates)} candidate tweets")

    # 3. Filter
    try:
        my_handle = x_client.username
    except Exception as e:
        mark_x_api_blocked_if_needed(db, e)
        logger.warning(f"Could not fetch authenticated X username; skipping discovery: {e}")
        return 0
    filtered = []
    for c in candidates:
        tweet_id = c.get("id", "")
        author = c.get("author_handle", "")

        if not tweet_id or not c.get("text"):
            continue
        if author.lower() == my_handle.lower():
            continue
        if db.proactive_action_exists(tweet_id, "reply"):
            continue
        if c.get("reply_settings") not in ("everyone", None, ""):
            continue
        if not _is_recent(c, proactive.max_tweet_age_hours):
            continue
        # Skip conversation replies (text starts with @mention) —
        # these often have reply restrictions causing 403 on post
        if c.get("text", "").startswith("@"):
            continue

        filtered.append(c)

    logger.info(f"After filtering: {len(filtered)} candidates")

    # 4. Score: semantic relevance to our knowledge base (batch)
    if knowledge_store:
        _batch_score_relevance(filtered, knowledge_store)
    else:
        for c in filtered:
            c["relevance"] = 0.0

    # 5. Rank and select top candidates above threshold
    filtered.sort(key=lambda c: c["relevance"], reverse=True)
    top = [c for c in filtered if c["relevance"] >= proactive.min_relevance]
    top = top[:10]  # cap per run

    logger.info(f"Above relevance threshold ({proactive.min_relevance}): {len(top)} candidates")

    # 6. Draft replies for top candidates
    inserted = 0
    account_cooldown_hours = getattr(proactive, "account_cooldown_hours", 0)
    for c in top:
        if db.count_recent_proactive_posts_to_author(
            c["author_handle"], account_cooldown_hours
        ) > 0:
            logger.info(
                "  Skipping @%s — contacted within last %s hours",
                c["author_handle"],
                account_cooldown_hours,
            )
            continue

        # Per-account weekly cap
        if db.count_weekly_replies_to_author(c["author_handle"]) >= proactive.reply_cap_per_account:
            logger.info(f"  Skipping @{c['author_handle']} — weekly cap reached")
            continue

        # Enrich with cultivate context if available
        person_ctx = None
        if bridge:
            try:
                person_ctx = bridge.get_person_context(c["author_handle"])
            except Exception as e:
                logger.debug(f"Failed to get person context for @{c['author_handle']}: {e}")

        try:
            draft = drafter.draft_proactive(
                their_tweet=c["text"],
                their_handle=c["author_handle"],
                self_handle=my_handle,
                person_context=person_ctx,
                knowledge_items=c.get("knowledge_context"),
            )
        except Exception as e:
            logger.warning(f"  Draft failed for tweet {c['id']}: {e}")
            continue

        db.insert_proactive_action(
            action_type="reply",
            target_tweet_id=c["id"],
            target_tweet_text=c["text"],
            target_author_handle=c["author_handle"],
            target_author_id=c.get("author_id"),
            discovery_source=c["discovery_source"],
            relevance_score=c["relevance"],
            draft_text=draft.reply_text,
            relationship_context=person_ctx.to_json() if person_ctx else None,
            knowledge_ids=json.dumps(draft.knowledge_ids) if draft.knowledge_ids else None,
        )
        inserted += 1
        logger.info(
            f"  [{c['relevance']:.2f}] @{c['author_handle']}: "
            f"{c['text'][:60]}... → {draft.reply_text[:60]}..."
        )

    return inserted


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (config, db):
        if not config.proactive or not config.proactive.enabled:
            logger.info("Proactive engagement is disabled")
            return

        x_client = XClient(
            config.x.api_key,
            config.x.api_secret,
            config.x.access_token,
            config.x.access_token_secret,
        )

        # Initialize knowledge store for relevance scoring + reply drafting
        knowledge_store = None
        if config.embeddings:
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
        )

        # Optional cultivate bridge
        bridge = None
        if config.cultivate and config.cultivate.enabled:
            try:
                from engagement.cultivate_bridge import CultivateBridge

                bridge = CultivateBridge.try_connect(config.cultivate.db_path)
                if bridge:
                    logger.info("Cultivate bridge connected for relationship context")
            except Exception as e:
                logger.warning(f"Cultivate bridge unavailable: {e}")

        inserted = discover(config, db, x_client, knowledge_store, drafter, bridge)

        if bridge:
            try:
                bridge.close()
            except Exception as e:
                logger.debug(f"Error closing cultivate bridge: {e}")

        logger.info(f"\nDone. Inserted {inserted} proactive reply drafts.")
        update_monitoring("discover-replies")


if __name__ == "__main__":
    main()
