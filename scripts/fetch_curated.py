#!/usr/bin/env python3
"""Fetch content from curated external sources."""

import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from storage.db import Database
from knowledge.embeddings import get_embedding_provider
from knowledge.store import KnowledgeStore
from knowledge.ingest import InsightExtractor, ingest_curated_post
from output.x_client import XClient


def fetch_user_tweets(x_client, username: str, limit: int = 10) -> list[dict]:
    """Fetch recent tweets from a user."""
    try:
        # Get user ID first
        user = x_client.client.get_user(username=username)
        if not user.data:
            print(f"  User @{username} not found")
            return []

        user_id = user.data.id

        # Get recent tweets
        tweets = x_client.client.get_users_tweets(
            user_id,
            max_results=limit,
            tweet_fields=["created_at", "public_metrics"]
        )

        if not tweets.data:
            return []

        return [
            {
                "id": str(tweet.id),
                "text": tweet.text,
                "url": f"https://x.com/{username}/status/{tweet.id}"
            }
            for tweet in tweets.data
        ]
    except Exception as e:
        print(f"  Error fetching @{username}: {e}")
        return []


def main():
    config = load_config()

    if not config.embeddings:
        print("Error: embeddings not configured")
        sys.exit(1)

    if not config.curated_sources:
        print("No curated sources configured")
        sys.exit(0)

    # Initialize
    db = Database(config.paths.database)
    db.connect()
    db.init_schema(str(Path(__file__).parent.parent / "schema.sql"))

    embedder = get_embedding_provider(
        config.embeddings.provider,
        config.embeddings.api_key,
        config.embeddings.model
    )

    store = KnowledgeStore(db.conn, embedder)
    extractor = InsightExtractor(config.anthropic.api_key, config.synthesis.model)

    x_client = XClient(
        config.x.api_key,
        config.x.api_secret,
        config.x.access_token,
        config.x.access_token_secret
    )

    # Fetch from curated X accounts
    print("=== Fetching from curated X accounts ===")
    for account in config.curated_sources.x_accounts:
        print(f"\nFetching @{account.identifier}...")
        tweets = fetch_user_tweets(x_client, account.identifier, limit=5)

        for tweet in tweets:
            if store.exists("curated_x", tweet["id"]):
                print(f"  Skipping {tweet['id']} (already exists)")
                continue

            # Skip retweets and very short tweets
            if tweet["text"].startswith("RT @") or len(tweet["text"]) < 50:
                continue

            print(f"  Processing tweet {tweet['id']}...")
            try:
                ingest_curated_post(
                    store=store,
                    extractor=extractor,
                    post_id=tweet["id"],
                    content=tweet["text"],
                    url=tweet["url"],
                    author=account.identifier,
                    license_type=account.license
                )
                print(f"    ✓ Ingested")
                time.sleep(1)  # Rate limiting
            except Exception as e:
                print(f"    ✗ Error: {e}")

    db.close()
    print("\n=== Done ===")


if __name__ == "__main__":
    main()
