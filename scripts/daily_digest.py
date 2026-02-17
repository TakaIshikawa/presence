#!/usr/bin/env python3
"""Generate and post daily digest thread."""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from storage.db import Database
from ingestion.claude_logs import ClaudeLogParser
from synthesis.generator import ContentGenerator
from synthesis.evaluator import ContentEvaluator
from output.x_client import XClient, parse_thread_content


def main():
    config = load_config()

    # Initialize components
    db = Database(config.paths.database)
    db.connect()
    db.init_schema(str(Path(__file__).parent.parent / "schema.sql"))

    generator = ContentGenerator(config.anthropic.api_key, config.synthesis.model)
    evaluator = ContentEvaluator(config.anthropic.api_key, config.synthesis.model)
    x_client = XClient(
        config.x.api_key,
        config.x.api_secret,
        config.x.access_token,
        config.x.access_token_secret
    )

    # Get today's date range
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today + timedelta(days=1)

    print(f"Generating daily digest for {today.date()}")

    # Get today's commits
    commits = db.get_commits_in_range(today, tomorrow)
    if not commits:
        print("No commits today, skipping digest")
        db.close()
        return

    print(f"Found {len(commits)} commits")

    # Get today's Claude prompts
    parser = ClaudeLogParser(config.paths.claude_logs)
    prompts = [
        msg for msg in parser.parse_global_history()
        if today <= msg.timestamp < tomorrow
    ]
    prompt_texts = [p.prompt_text for p in prompts]

    print(f"Found {len(prompts)} prompts")

    if not prompt_texts:
        print("No prompts found, skipping digest")
        db.close()
        return

    # Generate thread
    print("Generating thread...")
    generated = generator.generate_x_thread(
        prompts=prompt_texts,
        commits=commits
    )

    # Evaluate
    print("Evaluating...")
    eval_result = evaluator.evaluate(
        content_type="x_thread",
        content=generated.content,
        source_prompts=prompt_texts[:5],  # Limit for eval
        source_commits=[c["commit_message"] for c in commits[:5]]
    )

    print(f"Score: {eval_result.overall}/10 - {eval_result.feedback}")

    # Store
    content_id = db.insert_generated_content(
        content_type="x_thread",
        source_commits=[c["commit_sha"] for c in commits],
        source_messages=[p.message_uuid for p in prompts],
        content=generated.content,
        eval_score=eval_result.overall,
        eval_feedback=eval_result.feedback
    )

    # Post if passes threshold
    if eval_result.passes_threshold(config.synthesis.eval_threshold):
        print("Posting thread to X...")
        tweets = parse_thread_content(generated.content)
        result = x_client.post_thread(tweets)
        if result.success:
            db.mark_published(content_id, result.url)
            print(f"Posted: {result.url}")
        else:
            print(f"Post failed: {result.error}")
    else:
        print("Below threshold, not posting")
        print("Generated content:")
        print(generated.content)

    db.close()
    print("Done")


if __name__ == "__main__":
    main()
