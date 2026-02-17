#!/usr/bin/env python3
"""Poll for new commits and generate X posts."""

import sys
from pathlib import Path
from datetime import datetime, timedelta

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

    # Get commits from last polling interval
    since = datetime.now() - timedelta(minutes=config.polling.interval_minutes)

    print(f"Polling for commits since {since.isoformat()}")

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

        # Post if passes threshold
        if eval_result.passes_threshold(config.synthesis.eval_threshold):
            print("  Posting to X...")
            result = x_client.post(generated.content)
            if result.success:
                db.mark_published(content_id, result.url)
                print(f"  Posted: {result.url}")
            else:
                print(f"  Post failed: {result.error}")
        else:
            print("  Below threshold, not posting")

    db.close()
    print("Done")


if __name__ == "__main__":
    main()
