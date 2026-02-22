#!/usr/bin/env python3
"""Poll for new commits and generate X posts."""

import sys
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from storage.db import Database
from ingestion.github_commits import GitHubClient
from ingestion.claude_logs import get_prompts_around_timestamp
from synthesis.generator import ContentGenerator
from synthesis.evaluator import ContentEvaluator
from output.x_client import XClient

# Rate limiting: be conservative to avoid X rate limits
POST_DELAY_SECONDS = 60  # 1 minute between posts
MAX_POSTS_PER_POLL = 1   # Only 1 post per 10-min poll cycle


def main():
    config = load_config()

    # Initialize components
    db = Database(config.paths.database)
    db.connect()
    db.init_schema(str(Path(__file__).parent.parent / "schema.sql"))

    github = GitHubClient(config.github.token, config.github.username)
    generator = ContentGenerator(config.anthropic.api_key, config.synthesis.model)
    evaluator = ContentEvaluator(config.anthropic.api_key, config.synthesis.model)
    x_client = XClient(
        config.x.api_key,
        config.x.api_secret,
        config.x.access_token,
        config.x.access_token_secret
    )

    # Get last poll time from DB, or use fallback window
    last_poll = db.get_last_poll_time()
    if last_poll:
        # Add timezone info if missing
        if last_poll.tzinfo is None:
            last_poll = last_poll.replace(tzinfo=timezone.utc)
        since = last_poll
    else:
        # First run: check last 60 minutes
        since = datetime.now(timezone.utc) - timedelta(minutes=60)

    current_poll_time = datetime.now(timezone.utc)
    print(f"Polling for commits since {since.isoformat()}")

    posts_made = 0
    rate_limited = False

    # First, try to post any unpublished content from previous runs (1 per cycle max)
    min_score = config.synthesis.eval_threshold * 10
    unpublished = db.get_unpublished_content("x_post", min_score)
    if unpublished and posts_made < MAX_POSTS_PER_POLL:
        print(f"Retrying {len(unpublished)} unpublished posts (1 per cycle)...")
        item = unpublished[0]
        result = x_client.post(item["content"])
        if result.success:
            db.mark_published(item["id"], result.url)
            print(f"  ✓ Posted queued: {result.url}")
            posts_made += 1
        elif "429" in str(result.error):
            print(f"  Still rate limited, will retry next cycle")
            rate_limited = True

    for commit in github.get_all_recent_commits(since=since):
        if db.is_commit_processed(commit.sha):
            continue

        print(f"New commit: [{commit.repo_name}] {commit.sha[:8]} - {commit.message[:50]}")

        # Store commit
        db.insert_commit(
            repo_name=commit.repo_name,
            commit_sha=commit.sha,
            commit_message=commit.message,
            timestamp=commit.timestamp.isoformat(),
            author=commit.author
        )

        # Find related Claude prompts
        prompts = get_prompts_around_timestamp(
            commit.timestamp,
            window_minutes=30,
            claude_dir=config.paths.claude_logs
        )

        if not prompts:
            print("  No related prompts found, skipping post generation")
            continue

        # Use the most recent prompt before the commit
        relevant_prompts = [p for p in prompts if p.timestamp <= commit.timestamp]
        if not relevant_prompts:
            relevant_prompts = prompts

        prompt_text = relevant_prompts[-1].prompt_text

        # Generate X post
        print("  Generating X post...")
        generated = generator.generate_x_post(
            prompt=prompt_text,
            commit_message=commit.message,
            repo_name=commit.repo_name
        )

        # Evaluate
        print("  Evaluating...")
        eval_result = evaluator.evaluate(
            content_type="x_post",
            content=generated.content,
            source_prompts=[prompt_text],
            source_commits=[commit.message]
        )

        print(f"  Score: {eval_result.overall}/10 - {eval_result.feedback}")

        # Store generated content
        content_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[commit.sha],
            source_messages=[relevant_prompts[-1].message_uuid],
            content=generated.content,
            eval_score=eval_result.overall,
            eval_feedback=eval_result.feedback
        )

        # Post if passes threshold (respect max posts per cycle and rate limit)
        if eval_result.passes_threshold(config.synthesis.eval_threshold):
            if rate_limited:
                print(f"  Rate limited, queued for later")
                continue

            if posts_made >= MAX_POSTS_PER_POLL:
                print(f"  Reached max posts per cycle ({MAX_POSTS_PER_POLL}), queued for next poll")
                continue

            # Rate limiting: wait between posts
            if posts_made > 0:
                print(f"  Rate limiting: waiting {POST_DELAY_SECONDS}s...")
                time.sleep(POST_DELAY_SECONDS)

            print("  Posting to X...")
            result = x_client.post(generated.content)
            if result.success:
                db.mark_published(content_id, result.url)
                print(f"  Posted: {result.url}")
                posts_made += 1
            else:
                print(f"  Post failed: {result.error}")
                # If rate limited, stop posting but continue processing commits
                if "429" in str(result.error):
                    print("  Rate limited by X, queuing remaining")
                    rate_limited = True
        else:
            print("  Below threshold, not posting")

    # Update last poll time
    db.set_last_poll_time(current_poll_time)

    db.close()
    print(f"Done. {posts_made} posts made.")


if __name__ == "__main__":
    main()
