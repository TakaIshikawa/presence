#!/usr/bin/env python3
"""Discover candidate X accounts to follow based on knowledge base relevance.

Mines the proactive_actions table for authors whose tweets we've already
engaged with, scores their recent content against our knowledge base,
and inserts high-relevance accounts as candidates for human review.
"""

import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from output.x_client import XClient
from knowledge.embeddings import VoyageEmbeddings, EmbeddingError
from knowledge.store import KnowledgeStore
from output.api_rate_guard import should_skip_optional_api_call

logger = logging.getLogger(__name__)


def _get_candidate_handles(db) -> list[str]:
    """Extract distinct author handles from posted proactive actions."""
    cursor = db.conn.execute(
        """SELECT DISTINCT target_author_handle
           FROM proactive_actions
           WHERE target_author_handle IS NOT NULL
             AND target_author_handle != ''
           ORDER BY target_author_handle"""
    )
    return [row[0] for row in cursor.fetchall()]


def _score_account(
    x_client: XClient,
    knowledge_store: KnowledgeStore,
    handle: str,
    min_tweet_similarity: float = 0.3,
) -> tuple[float, int]:
    """Score an account's relevance to our knowledge base.

    Returns (avg_relevance, sample_count) where sample_count is the number
    of tweets above min_tweet_similarity.
    """
    user_id = x_client.get_user_id(handle)
    if not user_id:
        logger.warning(f"Could not resolve user ID for @{handle}")
        return 0.0, 0

    tweets = x_client.get_user_tweets(user_id, count=10)
    if not tweets:
        return 0.0, 0

    scores = []
    for tweet in tweets:
        text = tweet.get("text", "")
        if not text or text.startswith("RT @") or len(text) < 30:
            continue
        try:
            results = knowledge_store.search_similar(
                query=text,
                source_types=["own_post", "own_conversation"],
                limit=1,
                min_similarity=0.0,
            )
            if results:
                similarity = results[0][1]
                if similarity >= min_tweet_similarity:
                    scores.append(similarity)
        except (EmbeddingError, sqlite3.OperationalError) as e:
            logger.warning(f"Relevance scoring failed for tweet: {e}")
            continue

    if not scores:
        return 0.0, 0

    return sum(scores) / len(scores), len(scores)


def discover(config, db, x_client, knowledge_store) -> int:
    """Run account discovery. Returns count of candidates inserted."""
    proactive = config.proactive

    if should_skip_optional_api_call(
        config,
        db,
        "x",
        operation="account discovery scoring",
        logger=logger,
    ):
        return 0

    # 1. Get candidate handles from proactive_actions
    handles = _get_candidate_handles(db)
    logger.info(f"Found {len(handles)} distinct authors in proactive_actions")

    # 2. Filter out handles already in curated_sources (any status)
    new_handles = [
        h for h in handles
        if not db.candidate_exists("x_account", h)
    ]
    logger.info(f"After dedup: {len(new_handles)} new handles to evaluate")

    if not new_handles:
        return 0

    # 3. Cap per run
    new_handles = new_handles[:proactive.max_candidates_per_run]

    # 4. Score and insert
    inserted = 0
    for handle in new_handles:
        logger.info(f"Evaluating @{handle}...")

        relevance, sample_count = _score_account(
            x_client, knowledge_store, handle,
        )

        logger.info(
            f"  @{handle}: relevance={relevance:.2f}, samples={sample_count}"
        )

        if (sample_count > 0
                and relevance >= proactive.min_discovery_relevance
                and sample_count >= proactive.min_discovery_samples):
            result = db.insert_candidate_source(
                source_type="x_account",
                identifier=handle,
                name=handle,
                discovery_source="proactive_mining",
                relevance_score=relevance,
                sample_count=sample_count,
            )
            if result is not None:
                logger.info(f"  Inserted candidate: @{handle}")
                inserted += 1
        else:
            logger.info(f"  Below threshold, skipping @{handle}")

    return inserted


def sync_following(db, x_client, config=None) -> int:
    """Sync the authenticated user's following list into curated_sources.

    Accounts the user follows are auto-activated (no review needed).
    Existing entries (from config, mining, or prior syncs) are left untouched.
    Returns count of newly inserted accounts.
    """
    if config is not None and should_skip_optional_api_call(
        config,
        db,
        "x",
        operation="following-list sync",
        logger=logger,
    ):
        return 0

    following = x_client.get_following()
    if not following:
        logger.info("No following accounts returned (or API error)")
        return 0

    logger.info(f"Fetched {len(following)} accounts from following list")
    inserted = db.sync_following_sources(following)
    logger.info(f"Inserted {inserted} new accounts from following list")
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

        if not config.proactive.account_discovery_enabled:
            logger.info("Account discovery is disabled")
            return

        x_client = XClient(
            config.x.api_key,
            config.x.api_secret,
            config.x.access_token,
            config.x.access_token_secret,
        )

        # Phase 1: Sync following list (auto-activate)
        sync_following(db, x_client, config=config)

        # Phase 2: Mine proactive_actions for new candidates
        knowledge_store = None
        if config.embeddings:
            embedder = VoyageEmbeddings(
                api_key=config.embeddings.api_key,
                model=config.embeddings.model,
            )
            knowledge_store = KnowledgeStore(db.conn, embedder)

        if not knowledge_store:
            logger.error("Knowledge store required for account discovery")
            return

        inserted = discover(config, db, x_client, knowledge_store)
        logger.info(f"Done. Inserted {inserted} candidate accounts.")
        update_monitoring("discover-accounts")


if __name__ == "__main__":
    main()
