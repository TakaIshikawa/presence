#!/usr/bin/env python3
"""Generate and post daily digest thread via multi-stage pipeline."""

import sys
import subprocess
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from storage.db import Database
from ingestion.claude_logs import ClaudeLogParser
from synthesis.pipeline import SynthesisPipeline
from output.x_client import XClient, parse_thread_content


def main():
    config = load_config()

    # Initialize components
    db = Database(config.paths.database)
    db.connect()
    db.init_schema(str(Path(__file__).parent.parent / "schema.sql"))

    pipeline = SynthesisPipeline(
        api_key=config.anthropic.api_key,
        generator_model=config.synthesis.model,
        evaluator_model=config.synthesis.eval_model,
        db=db,
        num_candidates=config.synthesis.num_candidates,
    )
    x_client = XClient(
        config.x.api_key,
        config.x.api_secret,
        config.x.access_token,
        config.x.access_token_secret
    )

    # Get today's date range (UTC)
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
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

    # Convert commit dicts from db rows to have expected keys
    commit_dicts = [
        {"repo_name": c.get("repo_name", ""), "message": c.get("commit_message", ""),
         "sha": c.get("commit_sha", "")}
        for c in commits
    ]

    # Run pipeline
    print(f"Running pipeline: {len(commits)} commits, {config.synthesis.num_candidates} candidates...")
    result = pipeline.run(
        prompts=prompt_texts,
        commits=commit_dicts,
        content_type="x_thread",
        threshold=config.synthesis.eval_threshold,
    )

    # Log pipeline stages
    best_idx = result.comparison.ranking[0] if result.comparison.ranking else 0
    print(f"  Best candidate: {chr(65 + best_idx)} (score: {result.comparison.best_score}/10)")
    if result.refinement:
        print(f"  Refinement: picked {result.refinement.picked} (score: {result.refinement.final_score}/10)")
    print(f"  Final score: {result.final_score}/10")

    # Store
    content_id = db.insert_generated_content(
        content_type="x_thread",
        source_commits=[c["sha"] for c in commit_dicts],
        source_messages=[p.message_uuid for p in prompts],
        content=result.final_content,
        eval_score=result.final_score,
        eval_feedback=result.comparison.best_feedback,
    )

    # Record pipeline run
    db.insert_pipeline_run(
        batch_id=result.batch_id,
        content_type="x_thread",
        candidates_generated=len(result.candidates),
        best_candidate_index=best_idx,
        best_score_before_refine=result.comparison.best_score,
        best_score_after_refine=result.refinement.final_score if result.refinement else None,
        refinement_picked=result.refinement.picked if result.refinement else None,
        final_score=result.final_score,
        content_id=content_id,
    )

    # Post if passes threshold
    passes = result.final_score >= config.synthesis.eval_threshold * 10
    if passes:
        print("Posting thread to X...")
        tweets = parse_thread_content(result.final_content)
        post_result = x_client.post_thread(tweets)
        if post_result.success:
            db.mark_published(content_id, post_result.url, tweet_id=post_result.tweet_id)
            print(f"Posted: {post_result.url}")
        else:
            print(f"Post failed: {post_result.error}")
    else:
        if result.comparison.reject_reason:
            print(f"Rejected: {result.comparison.reject_reason}")
        else:
            print("Below threshold, not posting")
        print("Generated content:")
        print(result.final_content)

    db.close()
    _update_monitoring()
    print("Done")


def _update_monitoring():
    """Sync run state to operations.yaml for tact maintainer monitoring."""
    try:
        sync_script = Path(__file__).parent / "update_operations_state.py"
        if sync_script.exists():
            subprocess.run(
                [sys.executable, str(sync_script), "--operation", "run-daily"],
                check=False, capture_output=True,
            )
    except Exception:
        pass


if __name__ == "__main__":
    main()
