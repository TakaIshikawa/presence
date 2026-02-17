#!/usr/bin/env python3
"""Generate and publish weekly blog post."""

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
from output.blog_writer import BlogWriter


def main():
    config = load_config()

    # Initialize components
    db = Database(config.paths.database)
    db.connect()
    db.init_schema(str(Path(__file__).parent.parent / "schema.sql"))

    generator = ContentGenerator(config.anthropic.api_key, config.synthesis.model)
    evaluator = ContentEvaluator(config.anthropic.api_key, config.synthesis.model)
    blog_writer = BlogWriter(config.paths.static_site)

    # Get this week's date range (last 7 days)
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    week_ago = today - timedelta(days=7)

    print(f"Generating weekly digest for {week_ago.date()} to {today.date()}")

    # Get this week's commits
    commits = db.get_commits_in_range(week_ago, today)
    if not commits:
        print("No commits this week, skipping digest")
        db.close()
        return

    print(f"Found {len(commits)} commits")

    # Get this week's Claude prompts
    parser = ClaudeLogParser(config.paths.claude_logs)
    prompts = [
        msg for msg in parser.parse_global_history()
        if week_ago <= msg.timestamp < today
    ]
    prompt_texts = [p.prompt_text for p in prompts]

    print(f"Found {len(prompts)} prompts")

    if not prompt_texts:
        print("No prompts found, skipping digest")
        db.close()
        return

    # Generate blog post
    print("Generating blog post...")
    generated = generator.generate_blog_post(
        prompts=prompt_texts,
        commits=commits
    )

    # Evaluate
    print("Evaluating...")
    eval_result = evaluator.evaluate(
        content_type="blog_post",
        content=generated.content,
        source_prompts=prompt_texts[:10],  # Limit for eval
        source_commits=[c["commit_message"] for c in commits[:10]]
    )

    print(f"Score: {eval_result.overall}/10 - {eval_result.feedback}")

    # Store
    content_id = db.insert_generated_content(
        content_type="blog_post",
        source_commits=[c["commit_sha"] for c in commits],
        source_messages=[p.message_uuid for p in prompts],
        content=generated.content,
        eval_score=eval_result.overall,
        eval_feedback=eval_result.feedback
    )

    # Publish if passes threshold
    if eval_result.passes_threshold(config.synthesis.eval_threshold):
        print("Writing blog post...")
        result = blog_writer.write_post(generated.content)

        if result.success:
            print(f"Blog post written: {result.file_path}")

            # Commit and push
            print("Committing and pushing...")
            title = generated.content.split("\n")[0].replace("TITLE:", "").strip()
            if blog_writer.commit_and_push(title):
                db.mark_published(content_id, result.url)
                print(f"Published: {result.url}")
            else:
                print("Git push failed")
        else:
            print(f"Write failed: {result.error}")
    else:
        print("Below threshold, not publishing")
        print("Generated content:")
        print(generated.content[:500] + "...")

    db.close()
    print("Done")


if __name__ == "__main__":
    main()
