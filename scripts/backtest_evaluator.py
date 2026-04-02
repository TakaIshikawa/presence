#!/usr/bin/env python3
"""Run evaluator backtesting on collected benchmark tweets."""

import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from evaluation.engagement_predictor import EngagementPredictor
from evaluation.validation_db import ValidationDatabase

BATCH_SIZE = 5


def main():
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
    args = parser.parse_args()

    config = load_config()
    model = args.model or config.synthesis.eval_model

    predictor = EngagementPredictor(
        api_key=config.anthropic.api_key, model=model
    )

    db = ValidationDatabase(args.db_path)
    db.connect()
    db.init_schema()

    tweets = db.get_unevaluated_tweets(args.version, limit=args.limit)
    if not tweets:
        print(f"No unevaluated tweets for version '{args.version}'")
        db.close()
        return

    print(
        f"Evaluating {len(tweets)} tweets with version '{args.version}' "
        f"using {model}"
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

        print(f"\n@{username} ({len(account_tweets)} tweets)")

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
                print(f"  Error on batch {i // BATCH_SIZE + 1}: {e}")
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
            print(f"  Batch {i // BATCH_SIZE + 1}: scores {scores}")

    db.close()
    print(f"\nDone. Evaluated {evaluated} tweets.")


if __name__ == "__main__":
    main()
