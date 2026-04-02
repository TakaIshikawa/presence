#!/usr/bin/env python3
"""Collect benchmark tweets from followed accounts for evaluator backtesting."""

import sys
import time
import argparse
import tweepy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from evaluation.validation_db import ValidationDatabase

WEIGHT_LIKE = 1.0
WEIGHT_RETWEET = 3.0
WEIGHT_REPLY = 4.0
WEIGHT_QUOTE = 5.0

RATE_LIMIT_SLEEP = 60


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


def compute_engagement_score(like, rt, reply, quote) -> float:
    return (
        like * WEIGHT_LIKE
        + rt * WEIGHT_RETWEET
        + reply * WEIGHT_REPLY
        + quote * WEIGHT_QUOTE
    )


def fetch_following(client: tweepy.Client, user_id: str) -> list[dict]:
    """Fetch accounts the user follows with pagination."""
    accounts = []
    pagination_token = None

    while True:
        try:
            response = client.get_users_following(
                id=user_id,
                max_results=1000,
                pagination_token=pagination_token,
                user_fields=["description", "public_metrics"],
                user_auth=True,
            )
        except tweepy.TooManyRequests:
            print(f"  Rate limited on following list, sleeping {RATE_LIMIT_SLEEP}s...")
            time.sleep(RATE_LIMIT_SLEEP)
            continue

        if response.data:
            for user in response.data:
                metrics = user.public_metrics or {}
                accounts.append({
                    "user_id": str(user.id),
                    "username": user.username,
                    "display_name": user.name or "",
                    "bio": user.description or "",
                    "follower_count": metrics.get("followers_count", 0),
                    "following_count": metrics.get("following_count", 0),
                    "tweet_count": metrics.get("tweet_count", 0),
                })

        if response.meta and "next_token" in response.meta:
            pagination_token = response.meta["next_token"]
        else:
            break

    return accounts


def fetch_account_tweets(
    client: tweepy.Client, user_id: str, max_tweets: int
) -> list[dict]:
    """Fetch recent original tweets for an account with engagement metrics."""
    tweets = []
    pagination_token = None
    remaining = max_tweets

    while remaining > 0:
        batch_size = min(remaining, 100)
        if batch_size < 5:
            break  # X API requires min 5 per request
        try:
            response = client.get_users_tweets(
                id=user_id,
                max_results=batch_size,
                pagination_token=pagination_token,
                tweet_fields=["public_metrics", "created_at"],
                exclude=["retweets", "replies"],
            )
        except tweepy.TooManyRequests:
            print(f"    Rate limited, sleeping {RATE_LIMIT_SLEEP}s...")
            time.sleep(RATE_LIMIT_SLEEP)
            continue
        except tweepy.TweepyException as e:
            print(f"    API error: {e}")
            break

        if not response.data:
            break

        for tweet in response.data:
            metrics = tweet.public_metrics or {}
            likes = metrics.get("like_count", 0)
            rts = metrics.get("retweet_count", 0)
            replies = metrics.get("reply_count", 0)
            quotes = metrics.get("quote_count", 0)

            tweets.append({
                "tweet_id": str(tweet.id),
                "text": tweet.text,
                "like_count": likes,
                "retweet_count": rts,
                "reply_count": replies,
                "quote_count": quotes,
                "engagement_score": compute_engagement_score(likes, rts, replies, quotes),
                "tweet_created_at": str(tweet.created_at) if tweet.created_at else None,
            })

        remaining -= len(response.data)
        if response.meta and "next_token" in response.meta:
            pagination_token = response.meta["next_token"]
        else:
            break

    return tweets


def main():
    parser = argparse.ArgumentParser(
        description="Collect benchmark tweets from followed accounts"
    )
    parser.add_argument(
        "--max-accounts", type=int, default=20,
        help="Max accounts to collect from (default: 20)",
    )
    parser.add_argument(
        "--tweets-per-account", type=int, default=50,
        help="Tweets to fetch per account (default: 50)",
    )
    parser.add_argument(
        "--min-followers", type=int, default=1000,
        help="Skip accounts with fewer followers (default: 1000)",
    )
    parser.add_argument(
        "--db-path", default="./validation.db",
        help="Path to validation database (default: ./validation.db)",
    )
    args = parser.parse_args()

    config = load_config()

    # User-auth client for get_me() and get_users_following()
    client = tweepy.Client(
        consumer_key=config.x.api_key,
        consumer_secret=config.x.api_secret,
        access_token=config.x.access_token,
        access_token_secret=config.x.access_token_secret,
    )

    # Bearer-token client for reading other users' tweets
    bearer_token = get_bearer_token(config.x.api_key, config.x.api_secret)
    read_client = tweepy.Client(bearer_token=bearer_token)

    db = ValidationDatabase(args.db_path)
    db.connect()
    db.init_schema()

    # Get authenticated user's ID
    me = client.get_me()
    my_id = str(me.data.id)
    print(f"Authenticated as @{me.data.username} (id={my_id})")

    # Fetch following list
    print("Fetching following list...")
    following = fetch_following(client, my_id)
    print(f"Found {len(following)} accounts")

    # Filter by follower count and take top N
    following = [a for a in following if a["follower_count"] >= args.min_followers]
    following.sort(key=lambda a: a["follower_count"], reverse=True)
    following = following[: args.max_accounts]
    print(
        f"Selected {len(following)} accounts "
        f"(min {args.min_followers} followers)"
    )

    total_tweets = 0
    for i, account in enumerate(following):
        print(
            f"\n[{i + 1}/{len(following)}] @{account['username']} "
            f"({account['follower_count']:,} followers)"
        )

        # Upsert account
        db.upsert_account(**account)
        acct_row = db.get_account_by_user_id(account["user_id"])
        account_id = acct_row["id"]

        # Fetch tweets (bearer token for reading other users' timelines)
        tweets = fetch_account_tweets(
            read_client, account["user_id"], args.tweets_per_account
        )

        inserted = 0
        for tweet in tweets:
            result = db.insert_tweet(account_id=account_id, **tweet)
            if result is not None:
                inserted += 1

        total_tweets += inserted
        print(f"  Collected {inserted} new tweets (of {len(tweets)} fetched)")
        time.sleep(1)

    db.close()
    print(f"\nDone. Total new tweets: {total_tweets}")


if __name__ == "__main__":
    main()
