#!/usr/bin/env python3
"""Poll for new commits and generate X posts when enough material accumulates."""

import signal
import sys
import subprocess
from pathlib import Path
from datetime import datetime, timedelta, timezone

WATCHDOG_TIMEOUT = 600  # 10 minutes


def _timeout_handler(signum, frame):
    print("WATCHDOG: Poll process exceeded 10-minute timeout, exiting")
    sys.exit(1)

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from storage.db import Database
from ingestion.github_commits import GitHubClient
from ingestion.claude_logs import ClaudeLogParser
from synthesis.pipeline import SynthesisPipeline
from output.x_client import XClient


def estimate_tokens(texts: list[str]) -> int:
    """Estimate token count from text (chars / 4 approximation)."""
    return sum(len(t) for t in texts) // 4


def check_readiness(
    accumulated_tokens: int,
    threshold: int,
    hours_since_post: float,
    max_gap_hours: int,
    has_prompts: bool,
) -> bool:
    """Decide whether enough material has accumulated to run the pipeline.

    Returns True when:
    - Token count meets or exceeds the threshold, OR
    - Time since last post meets or exceeds the gap cap AND there are prompts.
    """
    gap_exceeded = hours_since_post >= max_gap_hours
    return accumulated_tokens >= threshold or (gap_exceeded and has_prompts)


def is_daily_cap_reached(posts_today: int, max_daily: int) -> bool:
    """Return True if the daily post limit has been reached."""
    return posts_today >= max_daily


def get_retryable_content(db, min_score: float, content_type: str = "x_post") -> list[dict]:
    """Return unpublished content eligible for retry (retry_count < MAX_RETRIES)."""
    return db.get_unpublished_content(content_type, min_score)


def main():
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(WATCHDOG_TIMEOUT)

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
    unpublished = get_retryable_content(db, min_score)
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

    # --- Phase 1: Ingest new commits ---
    new_commit_count = 0
    for commit in github.get_all_recent_commits(since=since):
        if db.is_commit_processed(commit.sha):
            continue

        print(f"New commit: [{commit.repo_name}] {commit.sha[:8]} - {commit.message[:50]}")

        db.insert_commit(
            repo_name=commit.repo_name,
            commit_sha=commit.sha,
            commit_message=commit.message,
            timestamp=commit.timestamp.isoformat(),
            author=commit.author
        )
        new_commit_count += 1

    if new_commit_count:
        print(f"Ingested {new_commit_count} new commits")
    else:
        print("No new commits")

    # --- Phase 2: Readiness check ---
    # Daily post cap
    posts_today = db.count_posts_today("x_post")
    max_daily = config.polling.max_daily_posts
    if is_daily_cap_reached(posts_today, max_daily):
        print(f"Daily post limit reached ({posts_today}/{max_daily}), waiting for tomorrow")
        db.set_last_poll_time(current_poll_time)
        _update_monitoring()
        db.close()
        print("Done. No posts made.")
        return

    last_post_time = db.get_last_published_time("x_post")
    now = datetime.now(timezone.utc)

    if last_post_time is None:
        # No posts ever — use a 24h lookback as baseline
        last_post_time = now - timedelta(hours=24)

    hours_since_post = (now - last_post_time).total_seconds() / 3600

    # Gather all commits since last post
    commits_since = db.get_commits_in_range(last_post_time, now)
    if not commits_since:
        print(f"No commits since last post ({hours_since_post:.1f}h ago)")
        db.set_last_poll_time(current_poll_time)
        _update_monitoring()
        db.close()
        print("Done. No posts made.")
        return

    # Gather all Claude prompts since last post
    parser = ClaudeLogParser(config.paths.claude_logs)
    prompts_since = list(parser.get_messages_since(last_post_time))

    # Compute accumulated tokens
    commit_texts = [c.get("commit_message", "") for c in commits_since]
    prompt_texts = [p.prompt_text for p in prompts_since]
    accumulated_tokens = estimate_tokens(commit_texts + prompt_texts)

    threshold = config.polling.readiness_token_threshold
    max_gap = config.polling.max_post_gap_hours

    print(f"Readiness: {accumulated_tokens} tokens (threshold: {threshold}), "
          f"{hours_since_post:.1f}h since last post (cap: {max_gap}h)")

    ready = check_readiness(accumulated_tokens, threshold, hours_since_post, max_gap, bool(prompts_since))

    if not ready:
        print(f"Not ready — waiting for more material")
        db.set_last_poll_time(current_poll_time)
        _update_monitoring()
        db.close()
        print("Done. No posts made.")
        return

    gap_exceeded = hours_since_post >= max_gap

    if gap_exceeded:
        print(f"Time cap exceeded ({hours_since_post:.1f}h >= {max_gap}h), forcing pipeline")
    else:
        print(f"Token threshold met ({accumulated_tokens} >= {threshold})")

    # --- Phase 3: Run pipeline with accumulated material ---
    if not prompts_since:
        print(f"{len(commits_since)} commits but no related prompts found")
        db.set_last_poll_time(current_poll_time)
        _update_monitoring()
        db.close()
        print("Done. No posts made.")
        return

    commit_dicts = [
        {"sha": c.get("commit_sha", ""), "repo_name": c.get("repo_name", ""),
         "message": c.get("commit_message", "")}
        for c in commits_since
    ]
    prompt_text_list = [p.prompt_text for p in prompts_since]
    prompt_uuids = [p.message_uuid for p in prompts_since]

    # Inject historical context if configured
    if config.historical and config.historical.enabled:
        from synthesis.theme_selector import ThemeSelector
        theme_selector = ThemeSelector(db)
        if theme_selector.should_inject("x_post", config.historical.injection_frequency):
            ctx = theme_selector.select(
                commit_dicts, "x_post",
                lookback_days=config.historical.lookback_days,
                min_age_days=config.historical.min_age_days,
                max_commits=config.historical.max_historical_commits,
            )
            if ctx:
                print(f"  Historical context: {ctx.theme_description}")
                for hc in ctx.commits:
                    hc["historical"] = True
                    commit_dicts.append(hc)

    print(f"\nRunning pipeline: {len(commit_dicts)} commits, {len(prompt_text_list)} prompts, "
          f"{config.synthesis.num_candidates} candidates...")
    pipeline_result = pipeline.run(
        prompts=prompt_text_list,
        commits=commit_dicts,
        content_type="x_post",
        threshold=config.synthesis.eval_threshold,
    )

    # Log pipeline stages
    best_idx = pipeline_result.comparison.ranking[0] if pipeline_result.comparison.ranking else 0
    print(f"  Candidates generated: {len(pipeline_result.candidates)}")
    print(f"  Best candidate: {chr(65 + best_idx)} (score: {pipeline_result.comparison.best_score:.1f}/10)")
    print(f"  Groundedness: {pipeline_result.comparison.groundedness}/10")
    if pipeline_result.refinement:
        print(f"  Refinement: picked {pipeline_result.refinement.picked} (score: {pipeline_result.refinement.final_score}/10)")
    print(f"  Final score: {pipeline_result.final_score:.1f}/10")
    print(f"  Content: {pipeline_result.final_content[:100]}...")

    # Store generated content
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[c["sha"] for c in commit_dicts],
        source_messages=prompt_uuids,
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

    # Update last poll time
    db.set_last_poll_time(current_poll_time)
    _update_monitoring()
    db.close()
    print(f"\nDone. {'1 post made' if posted else 'No posts made'}.")


def _update_monitoring():
    """Update operations.yaml for tact maintainer monitoring."""
    try:
        script_path = Path(__file__).parent / "check_poll_health.sh"
        if script_path.exists():
            subprocess.run([str(script_path)], check=False, capture_output=True)
        sync_script = Path(__file__).parent / "update_operations_state.py"
        if sync_script.exists():
            subprocess.run(
                [sys.executable, str(sync_script), "--operation", "run-poll"],
                check=False, capture_output=True,
            )
    except Exception as e:
        print(f"Warning: Could not update monitoring state: {e}")


if __name__ == "__main__":
    main()
