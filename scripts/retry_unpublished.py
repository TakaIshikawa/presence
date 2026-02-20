#!/usr/bin/env python3
"""Retry posting unpublished content that passed evaluation."""

import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from storage.db import Database
from output.x_client import XClient

# Rate limiting: seconds between X posts
POST_DELAY_SECONDS = 30


def main():
    config = load_config()

    db = Database(config.paths.database)
    db.connect()

    x_client = XClient(
        config.x.api_key,
        config.x.api_secret,
        config.x.access_token,
        config.x.access_token_secret
    )

    # Get unpublished content that passed threshold
    min_score = config.synthesis.eval_threshold * 10
    unpublished = db.get_unpublished_content("x_post", min_score)

    print(f"Found {len(unpublished)} unpublished posts to retry")

    posts_made = 0
    for item in unpublished:
        print(f"\nRetrying: {item['content'][:60]}...")

        # Rate limiting
        if posts_made > 0:
            print(f"  Waiting {POST_DELAY_SECONDS}s...")
            time.sleep(POST_DELAY_SECONDS)

        result = x_client.post(item['content'])
        if result.success:
            db.mark_published(item['id'], result.url)
            print(f"  Posted: {result.url}")
            posts_made += 1
        else:
            print(f"  Failed: {result.error}")
            if "429" in str(result.error):
                print("  Rate limited, stopping")
                break

    db.close()
    print(f"\nDone. {posts_made} posts made.")


if __name__ == "__main__":
    main()
