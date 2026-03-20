#!/usr/bin/env python3
"""Poll for new commits and generate X posts via multi-stage pipeline."""

import sys
import subprocess
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from storage.db import Database
from ingestion.github_commits import GitHubClient
from ingestion.claude_logs import get_prompts_around_timestamp
from synthesis.pipeline import SynthesisPipeline
from output.x_client import XClient


def main():
    config = load_config()

    # Initialize components
    db = Database(config.paths.database)
    db.connect()
    db.init_schema(str(Path(__file__).parent.parent / "schema.sql"))

    github = GitHubClient(config.github.token, config.github.username)
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
        retry_num = (item.get("retry_count") or 0) + 1
        result = x_client.post(item["content"])
        if result.success:
            db.mark_published(item["id"], result.url, tweet_id=result.tweet_id)
            print(f"  Posted queued: {result.url}")
            posted = True
        elif "429" in str(result.error):
            print(f"  Still rate limited, will retry next cycle")
            rate_limited = True
        else:
            count = db.increment_retry(item["id"])
            if count >= 3:
                print(f"  Post failed: {result.error} (attempt {retry_num}/3 — abandoned)")
            else:
                print(f"  Post failed: {result.error} (attempt {retry_num}/3)")

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

    # Run multi-stage pipeline if we have commits with prompts
    if new_commits and all_prompts:
        prompt_texts = [p["text"] for p in all_prompts]

        print(f"\nRunning pipeline: {len(new_commits)} commits, {config.synthesis.num_candidates} candidates...")
        pipeline_result = pipeline.run(
            prompts=prompt_texts,
            commits=new_commits,
            content_type="x_post",
            threshold=config.synthesis.eval_threshold,
        )

        # Log pipeline stages
        best_idx = pipeline_result.comparison.ranking[0] if pipeline_result.comparison.ranking else 0
        print(f"  Candidates generated: {len(pipeline_result.candidates)}")
        print(f"  Best candidate: {chr(65 + best_idx)} (score: {pipeline_result.comparison.best_score}/10)")
        if pipeline_result.refinement:
            print(f"  Refinement: picked {pipeline_result.refinement.picked} (score: {pipeline_result.refinement.final_score}/10)")
        print(f"  Final score: {pipeline_result.final_score}/10")
        print(f"  Content: {pipeline_result.final_content[:100]}...")

        # Store generated content
        content_id = db.insert_generated_content(
            content_type="x_post",
            source_commits=[c["sha"] for c in new_commits],
            source_messages=[p["uuid"] for p in all_prompts],
            content=pipeline_result.final_content,
            eval_score=pipeline_result.final_score,
            eval_feedback=pipeline_result.comparison.best_feedback,
        )

        # Record pipeline run
        db.insert_pipeline_run(
            batch_id=pipeline_result.batch_id,
            content_type="x_post",
            candidates_generated=len(pipeline_result.candidates),
            best_candidate_index=best_idx,
            best_score_before_refine=pipeline_result.comparison.best_score,
            best_score_after_refine=pipeline_result.refinement.final_score if pipeline_result.refinement else None,
            refinement_picked=pipeline_result.refinement.picked if pipeline_result.refinement else None,
            final_score=pipeline_result.final_score,
            content_id=content_id,
        )

        # Post if passes threshold
        passes = pipeline_result.final_score >= config.synthesis.eval_threshold * 10
        if passes:
            if rate_limited:
                print("Rate limited, queued for later")
            elif posted:
                print("Already posted this cycle, queued for next")
            else:
                print("Posting to X...")
                result = x_client.post(pipeline_result.final_content)
                if result.success:
                    db.mark_published(content_id, result.url, tweet_id=result.tweet_id)
                    print(f"Posted: {result.url}")
                    posted = True
                else:
                    print(f"Post failed: {result.error}")
        else:
            if pipeline_result.comparison.reject_reason:
                print(f"Rejected: {pipeline_result.comparison.reject_reason}")
            else:
                print("Below threshold, not posting")

    elif new_commits:
        print(f"\n{len(new_commits)} commits but no related prompts found")
    else:
        print("No new commits")

    # Update last poll time
    db.set_last_poll_time(current_poll_time)

    # Update operations.yaml for tact maintainer monitoring
    try:
        script_path = Path(__file__).parent / "check_poll_health.sh"
        if script_path.exists():
            subprocess.run([str(script_path)], check=False, capture_output=True)
    except Exception as e:
        # Don't fail the whole job if monitoring update fails
        print(f"Warning: Could not update monitoring state: {e}")

    db.close()
    print(f"\nDone. {'1 post made' if posted else 'No posts made'}.")


if __name__ == "__main__":
    main()
