#!/usr/bin/env python3
"""Fetch engagement metrics for published posts from X and Bluesky APIs."""

import logging
import sys
from pathlib import Path

import tweepy

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from evaluation.engagement_scorer import compute_engagement_score
from output.x_api_guard import (
    get_x_api_block_reason,
    mark_x_api_blocked_if_needed,
)

# Max tweets to fetch per batch (X API limit for GET /2/tweets)
BATCH_SIZE = 100

logger = logging.getLogger(__name__)


def get_bearer_token(api_key: str, api_secret: str) -> str:
    """Get OAuth 2.0 bearer token from consumer credentials."""
    import base64
    import requests

    credentials = base64.b64encode(f"{api_key}:{api_secret}".encode()).decode()
    resp = requests.post(
        "https://api.twitter.com/oauth2/token",
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        },
        data="grant_type=client_credentials",
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def backfill_tweet_ids(db) -> int:
    """Backfill tweet_ids for published posts that only have URLs."""
    cursor = db.conn.execute(
        """SELECT id, published_url FROM generated_content
           WHERE published = 1 AND tweet_id IS NULL
             AND published_url LIKE '%/status/%'"""
    )
    count = 0
    for row in cursor.fetchall():
        tweet_id = str(row[1]).split("/status/")[-1]
        db.conn.execute(
            "UPDATE generated_content SET tweet_id = ? WHERE id = ?",
            (tweet_id, row[0]),
        )
        count += 1
    if count:
        db.conn.commit()
    return count


def fetch_bluesky_profile_metrics(config, db) -> bool:
    """Fetch and store a Bluesky profile metrics snapshot when enabled."""
    if not config.bluesky or getattr(config.bluesky, "enabled", False) is not True:
        return False

    try:
        from output.bluesky_client import BlueskyClient

        bluesky_client = BlueskyClient(
            handle=config.bluesky.handle,
            app_password=config.bluesky.app_password,
        )
        metrics = bluesky_client.get_profile_metrics()
        if not metrics:
            return False

        db.insert_profile_metrics("bluesky", **metrics)
        logger.info(
            f"Bluesky profile: {metrics['follower_count']} followers, "
            f"{metrics['following_count']} following, "
            f"{metrics['tweet_count']} posts"
        )
        return True
    except Exception as e:
        logger.warning(f"Bluesky profile metrics fetch failed (non-fatal): {e}")
        return False


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    with script_context() as (config, db):
        block_reason = get_x_api_block_reason(db)
        if block_reason:
            logger.warning(f"X API circuit breaker active; skipping engagement fetch: {block_reason}")
            return

        # Backfill tweet_ids from URLs for pre-pipeline posts
        backfilled = backfill_tweet_ids(db)
        if backfilled:
            logger.info(f"Backfilled {backfilled} tweet_ids from URLs")

        # Use bearer token for read endpoints (OAuth 2.0 App-Only)
        try:
            bearer_token = get_bearer_token(config.x.api_key, config.x.api_secret)
        except Exception as e:
            mark_x_api_blocked_if_needed(db, e)
            logger.error(f"Failed to get X bearer token: {e}")
            return
        client = tweepy.Client(bearer_token=bearer_token)

        # Get posts that need metrics (published in last 30 days, not fetched in 6h)
        posts = db.get_posts_needing_metrics(max_age_days=30)
        if not posts:
            logger.info("No posts need metrics fetching")
            return

        logger.info(f"Fetching metrics for {len(posts)} posts")

        # Batch tweet IDs for efficient API usage
        tweet_id_to_post = {p["tweet_id"]: p for p in posts}
        tweet_ids = list(tweet_id_to_post.keys())

        fetched = 0
        for i in range(0, len(tweet_ids), BATCH_SIZE):
            batch = tweet_ids[i : i + BATCH_SIZE]

            try:
                response = client.get_tweets(
                    ids=batch, tweet_fields=["public_metrics"]
                )
            except tweepy.TweepyException as e:
                blocked_until = mark_x_api_blocked_if_needed(db, e)
                logger.error(f"API error fetching batch {i // BATCH_SIZE + 1}: {e}")
                if blocked_until:
                    break
                continue

            if not response.data:
                logger.warning(f"No data returned for batch {i // BATCH_SIZE + 1}")
                continue

            for tweet in response.data:
                post = tweet_id_to_post.get(str(tweet.id))
                if not post:
                    continue

                metrics = tweet.public_metrics or {}
                like_count = metrics.get("like_count", 0)
                retweet_count = metrics.get("retweet_count", 0)
                reply_count = metrics.get("reply_count", 0)
                quote_count = metrics.get("quote_count", 0)

                score = compute_engagement_score(
                    like_count, retweet_count, reply_count, quote_count
                )

                db.insert_engagement(
                    content_id=post["id"],
                    tweet_id=post["tweet_id"],
                    like_count=like_count,
                    retweet_count=retweet_count,
                    reply_count=reply_count,
                    quote_count=quote_count,
                    engagement_score=score,
                )
                db.backfill_prediction_actuals(post["id"], score)
                fetched += 1
                logger.info(
                    f"  {post['tweet_id']}: {like_count}L {retweet_count}RT {reply_count}R {quote_count}Q = {score:.1f}"
                )

        # Fetch Bluesky engagement metrics if enabled
        bluesky_fetched = 0
        if config.bluesky and getattr(config.bluesky, "enabled", False) is True:
            logger.info("\n--- Fetching Bluesky engagement ---")
            bluesky_posts = db.get_content_needing_bluesky_engagement(max_age_days=7)

            if bluesky_posts:
                logger.info(f"Fetching Bluesky metrics for {len(bluesky_posts)} posts")

                from output.bluesky_client import BlueskyClient
                bluesky_client = BlueskyClient(
                    handle=config.bluesky.handle,
                    app_password=config.bluesky.app_password
                )

                for post in bluesky_posts:
                    metrics = bluesky_client.get_post_metrics(post["bluesky_uri"])

                    if metrics is None:
                        logger.warning(f"  Failed to fetch metrics for {post['bluesky_uri']}")
                        continue

                    score = compute_engagement_score(
                        metrics['like_count'],
                        metrics['repost_count'],
                        metrics['reply_count'],
                        metrics['quote_count']
                    )

                    db.insert_bluesky_engagement(
                        content_id=post["id"],
                        bluesky_uri=post["bluesky_uri"],
                        like_count=metrics['like_count'],
                        repost_count=metrics['repost_count'],
                        reply_count=metrics['reply_count'],
                        quote_count=metrics['quote_count'],
                        engagement_score=score,
                    )
                    bluesky_fetched += 1
                    logger.info(
                        f"  {post['bluesky_uri']}: {metrics['like_count']}L "
                        f"{metrics['repost_count']}RP {metrics['reply_count']}R "
                        f"{metrics['quote_count']}Q = {score:.1f}"
                    )
            else:
                logger.info("No Bluesky posts need metrics fetching")

        # Auto-classify posts that have settled (>= 48h old)
        classified = db.auto_classify_posts(min_age_hours=48)
        if classified["resonated"] or classified["low_resonance"]:
            logger.info(f"\nAuto-classified: {classified['resonated']} resonated, "
                        f"{classified['low_resonance']} low_resonance")

        # Fetch profile metrics (piggyback on engagement job)
        try:
            from output.x_client import XClient

            x_client = XClient(
                config.x.api_key,
                config.x.api_secret,
                config.x.access_token,
                config.x.access_token_secret,
            )
            metrics = x_client.get_profile_metrics()
            if metrics:
                db.insert_profile_metrics("x", **metrics)
                logger.info(f"Profile: {metrics['follower_count']} followers, "
                            f"{metrics['following_count']} following")
            else:
                mark_x_api_blocked_if_needed(db, x_client.last_error)
        except Exception as e:
            mark_x_api_blocked_if_needed(db, e)
            logger.warning(f"Profile metrics fetch failed (non-fatal): {e}")

        fetch_bluesky_profile_metrics(config, db)

        logger.info(f"\nDone. Fetched X metrics for {fetched} posts, Bluesky metrics for {bluesky_fetched} posts.")


if __name__ == "__main__":
    main()
