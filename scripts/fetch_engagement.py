#!/usr/bin/env python3
"""Fetch engagement metrics for published posts from X API."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from storage.db import Database
from output.x_client import XClient

# Engagement score weights (absolute counts, no impression normalization)
WEIGHT_LIKE = 1.0
WEIGHT_RETWEET = 3.0
WEIGHT_REPLY = 4.0
WEIGHT_QUOTE = 5.0

# Max tweets to fetch per batch (X API limit for GET /2/tweets)
BATCH_SIZE = 100


def compute_engagement_score(
    like_count: int,
    retweet_count: int,
    reply_count: int,
    quote_count: int
) -> float:
    return (
        like_count * WEIGHT_LIKE
        + retweet_count * WEIGHT_RETWEET
        + reply_count * WEIGHT_REPLY
        + quote_count * WEIGHT_QUOTE
    )


def main():
    config = load_config()

    db = Database(config.paths.database)
    db.connect()
    db.init_schema(str(Path(__file__).parent.parent / "schema.sql"))

    x_client = XClient(
        config.x.api_key,
        config.x.api_secret,
        config.x.access_token,
        config.x.access_token_secret
    )

    # Get posts that need metrics (published in last 30 days, not fetched in 6h)
    posts = db.get_posts_needing_metrics(max_age_days=30)
    if not posts:
        print("No posts need metrics fetching")
        db.close()
        return

    print(f"Fetching metrics for {len(posts)} posts")

    # Batch tweet IDs for efficient API usage
    tweet_id_to_post = {p["tweet_id"]: p for p in posts}
    tweet_ids = list(tweet_id_to_post.keys())

    fetched = 0
    for i in range(0, len(tweet_ids), BATCH_SIZE):
        batch = tweet_ids[i:i + BATCH_SIZE]

        try:
            response = x_client.client.get_tweets(
                ids=batch,
                tweet_fields=["public_metrics"]
            )
        except Exception as e:
            print(f"API error fetching batch {i // BATCH_SIZE + 1}: {e}")
            continue

        if not response.data:
            print(f"No data returned for batch {i // BATCH_SIZE + 1}")
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
                engagement_score=score
            )
            fetched += 1
            print(f"  {post['tweet_id']}: {like_count}L {retweet_count}RT {reply_count}R {quote_count}Q = {score:.1f}")

    db.close()
    print(f"\nDone. Fetched metrics for {fetched} posts.")


if __name__ == "__main__":
    main()
