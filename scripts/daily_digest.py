#!/usr/bin/env python3
"""Generate and post daily digest thread via multi-stage pipeline."""

import sys
import logging
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

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
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
        anthropic_timeout=config.timeouts.anthropic_seconds,
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

    logger.info(f"Generating daily digest for {today.date()}")

    # Get today's commits
    commits = db.get_commits_in_range(today, tomorrow)
    if not commits:
        logger.info("No commits today, skipping digest")
        db.close()
        return

    logger.info(f"Found {len(commits)} commits")

    # Get today's Claude prompts
    parser = ClaudeLogParser(config.paths.claude_logs)
    prompts = [
        msg for msg in parser.parse_global_history()
        if today <= msg.timestamp < tomorrow
    ]
    prompt_texts = [p.prompt_text for p in prompts]

    logger.info(f"Found {len(prompts)} prompts")

    if not prompt_texts:
        logger.info("No prompts found, skipping digest")
        db.close()
        return

    # Convert commit dicts from db rows to have expected keys
    commit_dicts = [
        {"repo_name": c.get("repo_name", ""), "message": c.get("commit_message", ""),
         "sha": c.get("commit_sha", "")}
        for c in commits
    ]

    # Inject historical context if configured
    if config.historical and config.historical.enabled:
        from synthesis.theme_selector import ThemeSelector
        theme_selector = ThemeSelector(db)
        if theme_selector.should_inject("x_thread", config.historical.injection_frequency):
            ctx = theme_selector.select(
                commit_dicts, "x_thread",
                lookback_days=config.historical.lookback_days,
                min_age_days=config.historical.min_age_days,
                max_commits=config.historical.max_historical_commits,
            )
            if ctx:
                logger.info(f"  Historical context: {ctx.theme_description}")
                for hc in ctx.commits:
                    hc["historical"] = True
                    commit_dicts.append(hc)

    # Run pipeline
    logger.info(f"Running pipeline: {len(commits)} commits, {config.synthesis.num_candidates} candidates...")
    result = pipeline.run(
        prompts=prompt_texts,
        commits=commit_dicts,
        content_type="x_thread",
        threshold=config.synthesis.eval_threshold,
    )

    # Log pipeline stages
    best_idx = result.comparison.ranking[0] if result.comparison.ranking else 0
    logger.info(f"  Best candidate: {chr(65 + best_idx)} (score: {result.comparison.best_score}/10)")
    if result.refinement:
        logger.info(f"  Refinement: picked {result.refinement.picked} (score: {result.refinement.final_score}/10)")
    logger.info(f"  Final score: {result.final_score}/10")

    # Store
    content_id = db.insert_generated_content(
        content_type="x_thread",
        source_commits=[c["sha"] for c in commit_dicts],
        source_messages=[p.message_uuid for p in prompts],
        content=result.final_content,
        eval_score=result.final_score,
        eval_feedback=result.comparison.best_feedback,
    )

    # Determine outcome and post if passes threshold
    passes = result.final_score >= config.synthesis.eval_threshold * 10
    outcome = None
    rejection_reason = None

    if not result.candidates:
        outcome = "all_filtered"
        rejection_reason = result.comparison.reject_reason
    elif not passes:
        outcome = "below_threshold"
        rejection_reason = result.comparison.reject_reason or (
            f"Score {result.final_score:.1f} below threshold "
            f"{config.synthesis.eval_threshold * 10}"
        )
        if result.comparison.reject_reason:
            logger.warning(f"Rejected: {result.comparison.reject_reason}")
        else:
            logger.warning("Below threshold, not posting")
        logger.debug("Generated content:")
        logger.debug(result.final_content)
    else:
        logger.info("Posting thread to X...")
        tweets = parse_thread_content(result.final_content)
        post_result = x_client.post_thread(tweets)
        if post_result.success:
            db.mark_published(content_id, post_result.url, tweet_id=post_result.tweet_id)
            logger.info(f"Posted: {post_result.url}")
            outcome = "published"
        else:
            logger.error(f"Post failed: {post_result.error}")
            outcome = "below_threshold"
            rejection_reason = f"Post failed: {post_result.error}"

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
        outcome=outcome,
        rejection_reason=rejection_reason,
        filter_stats=result.filter_stats,
    )

    db.close()
    _update_monitoring()
    logger.info("Done")


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
