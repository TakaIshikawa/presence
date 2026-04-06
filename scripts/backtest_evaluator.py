#!/usr/bin/env python3
"""Run evaluator backtesting on collected benchmark tweets."""

import sys
import time
import logging
import argparse
import tweepy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from evaluation.engagement_predictor import EngagementPredictor
from evaluation.validation_db import ValidationDatabase

BATCH_SIZE = 5
TWEET_FETCH_BATCH = 100

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s'
    )
    parser = argparse.ArgumentParser(description="Run evaluator backtest")
    parser.add_argument(
        "--version", required=True,
        help="Evaluator version label (e.g., v1, v2_harsh)",
    )
    parser.add_argument(
        "--model", default=None,
        help="Model override (default: synthesis.eval_model from config)",
    )
    parser.add_argument(
        "--prompt-version", default="v1",
        help="Prompt template version (default: v1)",
    )
    parser.add_argument(
        "--limit", type=int, default=500,
        help="Max tweets to evaluate (default: 500)",
    )
    parser.add_argument(
        "--db-path", default="./validation.db",
        help="Path to validation database (default: ./validation.db)",
    )
    parser.add_argument(
        "--no-purge", action="store_true",
        help="Skip purging full tweet text after evaluation",
    )
    args = parser.parse_args()

    config = load_config()
    model = args.model or config.synthesis.eval_model

    predictor = EngagementPredictor(
        api_key=config.anthropic.api_key, model=model
    )

    db = ValidationDatabase(args.db_path)
    db.connect()
    db.init_schema()

    # Refetch text for any previously purged tweets
    purged_ids = db.get_purged_tweet_ids()
    if purged_ids:
        logger.info("Refetching text for %d purged tweets", len(purged_ids))
        bearer_client = tweepy.Client(
            consumer_key=config.x.api_key,
            consumer_secret=config.x.api_secret,
            access_token=config.x.access_token,
            access_token_secret=config.x.access_token_secret,
        )
        refetched = 0
        for i in range(0, len(purged_ids), TWEET_FETCH_BATCH):
            batch = purged_ids[i : i + TWEET_FETCH_BATCH]
            try:
                resp = bearer_client.get_tweets(ids=batch, user_auth=True)
                if resp.data:
                    for tweet in resp.data:
                        db.update_tweet_text(str(tweet.id), tweet.text)
                        refetched += 1
            except tweepy.TooManyRequests:
                logger.warning("Rate limited, waiting 60s")
                time.sleep(60)
            except tweepy.TweepyException as e:
                logger.error("API error: %s", e)
        logger.info("Restored text for %d tweets", refetched)

    tweets = db.get_unevaluated_tweets(args.version, limit=args.limit)
    tweets = [t for t in tweets if t["text"]]
    if not tweets:
        logger.info("No unevaluated tweets for version '%s'", args.version)
        db.close()
        return

    logger.info(
        "Evaluating %d tweets with version '%s' using %s",
        len(tweets), args.version, model
    )

    # Group by account for context
    by_account: dict[str, list[dict]] = {}
    for t in tweets:
        by_account.setdefault(t["username"], []).append(t)

    evaluated = 0
    for username, account_tweets in by_account.items():
        follower_count = account_tweets[0]["follower_count"]
        bio = account_tweets[0].get("bio", "") or ""
        context = (
            f"Account: @{username}, {follower_count:,} followers. "
            f"Bio: {bio[:200]}"
        )

        logger.info("@%s (%d tweets)", username, len(account_tweets))

        for i in range(0, len(account_tweets), BATCH_SIZE):
            batch = account_tweets[i : i + BATCH_SIZE]
            tweet_inputs = [
                {"id": t["tweet_id"], "text": t["text"]} for t in batch
            ]

            try:
                predictions = predictor.predict_batch(
                    tweets=tweet_inputs,
                    account_context=context,
                    prompt_version=args.prompt_version,
                )
            except Exception as e:
                logger.error("Error on batch %d: %s", i // BATCH_SIZE + 1, e)
                continue

            for pred in predictions:
                db.insert_evaluation(
                    tweet_id=pred.tweet_id,
                    evaluator_version=args.version,
                    model=model,
                    predicted_score=pred.predicted_score,
                    hook_strength=pred.hook_strength,
                    specificity=pred.specificity,
                    emotional_resonance=pred.emotional_resonance,
                    novelty=pred.novelty,
                    actionability=pred.actionability,
                    raw_response=pred.raw_response,
                )
                evaluated += 1

            scores = [f"{p.predicted_score:.0f}" for p in predictions]
            logger.info("Batch %d: scores %s", i // BATCH_SIZE + 1, scores)

    # Purge tweet text after evaluation (X data retention compliance)
    if not args.no_purge:
        purged = db.purge_tweet_text()
        if purged:
            logger.info("Purged text from %d tweets (IDs + metrics retained)", purged)

    db.close()
    logger.info("Done. Evaluated %d tweets.", evaluated)


if __name__ == "__main__":
    main()
