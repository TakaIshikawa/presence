#!/usr/bin/env python3
"""Poll for new commits and generate X posts (batched)."""

import sys
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
        if last_poll.tzinfo is None:
            last_poll = last_poll.replace(tzinfo=timezone.utc)
        since = last_poll
    else:
        # First run: check last 90 minutes
        since = datetime.now(timezone.utc) - timedelta(minutes=90)

    current_poll_time = datetime.now(timezone.utc)
    print(f"Polling for commits since {since.isoformat()}")

    posted = False
    rate_limited = False

    # First, try to post any unpublished content from previous runs
    min_score = config.synthesis.eval_threshold * 10
    unpublished = db.get_unpublished_content("x_post", min_score)
    if unpublished:
        print(f"Retrying {len(unpublished)} unpublished posts...")
        item = unpublished[0]
        result = x_client.post(item["content"])
        if result.success:
            db.mark_published(item["id"], result.url)
            print(f"  Posted queued: {result.url}")
            posted = True
        elif "429" in str(result.error):
            print(f"  Still rate limited, will retry next cycle")
            rate_limited = True

    # Collect all new commits and their prompts
    new_commits = []
    all_prompts = []

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

        # Collect commit data
        new_commits.append({
            "sha": commit.sha,
            "repo_name": commit.repo_name,
            "message": commit.message
        })

        # Collect related prompts
        if prompts:
            relevant = [p for p in prompts if p.timestamp <= commit.timestamp]
            if not relevant:
                relevant = prompts
            all_prompts.append({
                "text": relevant[-1].prompt_text,
                "uuid": relevant[-1].message_uuid
            })

    # Generate one batched post if we have commits with prompts
    if new_commits and all_prompts:
        print(f"\nSynthesizing {len(new_commits)} commits into one post...")

        prompt_texts = [p["text"] for p in all_prompts]

        generated = generator.generate_x_post_batched(
            prompts=prompt_texts,
            commits=new_commits
        )

        # Evaluate
        print("Evaluating...")
        eval_result = evaluator.evaluate(
            content_type="x_post",
            content=generated.content,
            source_prompts=prompt_texts,
            source_commits=[c["message"] for c in new_commits]
        )

        print(f"Score: {eval_result.overall}/10 - {eval_result.feedback}")

        # Store generated content
        content_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[c["sha"] for c in new_commits],
            source_messages=[p["uuid"] for p in all_prompts],
            content=generated.content,
            eval_score=eval_result.overall,
            eval_feedback=eval_result.feedback
        )

        # Post if passes threshold
        if eval_result.passes_threshold(config.synthesis.eval_threshold):
            if rate_limited:
                print("Rate limited, queued for later")
            elif posted:
                print("Already posted this cycle, queued for next")
            else:
                print("Posting to X...")
                result = x_client.post(generated.content)
                if result.success:
                    db.mark_published(content_id, result.url)
                    print(f"Posted: {result.url}")
                    posted = True
                else:
                    print(f"Post failed: {result.error}")
        else:
            print("Below threshold, not posting")

    elif new_commits:
        print(f"\n{len(new_commits)} commits but no related prompts found")
    else:
        print("No new commits")

    # Update last poll time
    db.set_last_poll_time(current_poll_time)

    db.close()
    print(f"\nDone. {'1 post made' if posted else 'No posts made'}.")


if __name__ == "__main__":
    main()
