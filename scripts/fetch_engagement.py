#!/usr/bin/env python3
"""Fetch engagement metrics for published posts from X API."""

import logging
import sys
import tweepy
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from evaluation.engagement_scorer import compute_engagement_score

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


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    with script_context() as (config, db):
        # Backfill tweet_ids from URLs for pre-pipeline posts
        backfilled = backfill_tweet_ids(db)
        if backfilled:
            logger.info(f"Backfilled {backfilled} tweet_ids from URLs")

        # Use bearer token for read endpoints (OAuth 2.0 App-Only)
        bearer_token = get_bearer_token(config.x.api_key, config.x.api_secret)
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
            except Exception as e:
                logger.error(f"API error fetching batch {i // BATCH_SIZE + 1}: {e}")
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
                fetched += 1
                logger.info(
                    f"  {post['tweet_id']}: {like_count}L {retweet_count}RT {reply_count}R {quote_count}Q = {score:.1f}"
                )

        # Auto-classify posts that have settled (>= 48h old)
        classified = db.auto_classify_posts(min_age_hours=48)
        if classified["resonated"] or classified["low_resonance"]:
            logger.info(f"\nAuto-classified: {classified['resonated']} resonated, "
                        f"{classified['low_resonance']} low_resonance")

        logger.info(f"\nDone. Fetched metrics for {fetched} posts.")


if __name__ == "__main__":
    main()
