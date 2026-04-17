#!/usr/bin/env python3
"""Poll for new commits and generate X threads when enough material accumulates."""

import signal
import sys
import types
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone

WATCHDOG_TIMEOUT = 600  # 10 minutes

logger = logging.getLogger(__name__)


def _timeout_handler(signum: int, frame: types.FrameType | None) -> None:
    logger.error("WATCHDOG: Poll process exceeded 10-minute timeout, exiting")
    sys.exit(1)

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from storage.db import Database
from ingestion.github_commits import GitHubClient
from ingestion.claude_logs import ClaudeLogParser
from synthesis.pipeline import SynthesisPipeline
from output.x_client import XClient, parse_thread_content
from output.bluesky_client import BlueskyClient
from knowledge.embeddings import VoyageEmbeddings, serialize_embedding
from knowledge.store import KnowledgeStore
from evaluation.engagement_predictor import EngagementPredictor


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


def get_retryable_content(db: Database, min_score: float, content_type: str = "x_thread") -> list[dict]:
    """Return unpublished content eligible for retry (retry_count < MAX_RETRIES)."""
    return db.get_unpublished_content(content_type, min_score)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(WATCHDOG_TIMEOUT)

    with script_context() as (config, db):
        github = GitHubClient(config.github.token, config.github.username, timeout=config.timeouts.github_seconds)

        # Initialize embedder for semantic dedup
        embedder = None
        semantic_threshold = 0.82
        if config.embeddings:
            embedder = VoyageEmbeddings(
                api_key=config.embeddings.api_key,
                model=config.embeddings.model,
            )
            semantic_threshold = config.embeddings.semantic_dedup_threshold

        # Initialize knowledge store for trend context
        knowledge_store = None
        if embedder and config.curated_sources:
            knowledge_store = KnowledgeStore(db.conn, embedder)

        # Initialize engagement predictor
        engagement_predictor = EngagementPredictor(
            api_key=config.anthropic.api_key,
            model=config.synthesis.eval_model,
            timeout=config.timeouts.anthropic_seconds,
        )

        pipeline = SynthesisPipeline(
            api_key=config.anthropic.api_key,
            generator_model=config.synthesis.model,
            evaluator_model=config.synthesis.eval_model,
            db=db,
            num_candidates=config.synthesis.num_candidates,
            anthropic_timeout=config.timeouts.anthropic_seconds,
            embedder=embedder,
            semantic_threshold=semantic_threshold,
            knowledge_store=knowledge_store,
            engagement_predictor=engagement_predictor,
            format_weighting_enabled=config.synthesis.format_weighting_enabled,
        )
        x_client = XClient(
            config.x.api_key,
            config.x.api_secret,
            config.x.access_token,
            config.x.access_token_secret
        )

        # Initialize Bluesky client if configured
        bluesky_client = None
        if config.bluesky and config.bluesky.enabled:
            bluesky_client = BlueskyClient(
                config.bluesky.handle,
                config.bluesky.app_password
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
        logger.info(f"Polling for commits since {since.isoformat()}")

        posted = False
        rate_limited = False

        # First, try to post any unpublished content from previous runs
        min_score = config.synthesis.eval_threshold * 10
        unpublished = get_retryable_content(db, min_score)
        if unpublished:
            logger.info(f"Retrying {len(unpublished)} unpublished posts...")
            item = unpublished[0]
            retry_num = (item.get("retry_count") or 0) + 1
            tweets = parse_thread_content(item["content"])
            result = x_client.post_thread(tweets)
            if result.success:
                db.mark_published(item["id"], result.url, tweet_id=result.tweet_id)
                logger.info(f"  Posted queued: {result.url}")
                posted = True

                # Cross-post to Bluesky if configured
                if bluesky_client:
                    from output.cross_poster import CrossPoster
                    cross_poster = CrossPoster(bluesky_client=bluesky_client)
                    bsky_tweets = [cross_poster.adapt_for_bluesky(t, "x_thread") for t in tweets]
                    bsky_result = bluesky_client.post_thread(bsky_tweets)
                    if bsky_result.success:
                        db.mark_published_bluesky(item["id"], bsky_result.uri)
                        logger.info(f"  Cross-posted to Bluesky: {bsky_result.url}")
                    else:
                        logger.warning(f"  Bluesky cross-post failed (non-fatal): {bsky_result.error}")
            elif "429" in str(result.error):
                logger.info(f"  Still rate limited, will retry next cycle")
                rate_limited = True
            else:
                count = db.increment_retry(item["id"])
                if count >= 3:
                    logger.warning(f"  Post failed: {result.error} (attempt {retry_num}/3 — abandoned)")
                else:
                    logger.warning(f"  Post failed: {result.error} (attempt {retry_num}/3)")

        # --- Phase 1: Ingest new commits ---
        new_commit_count = 0
        for commit in github.get_all_recent_commits(since=since):
            if db.is_commit_processed(commit.sha):
                continue

            logger.info(f"New commit: [{commit.repo_name}] {commit.sha[:8]} - {commit.message[:50]}")

            commit_id = db.insert_commit(
                repo_name=commit.repo_name,
                commit_sha=commit.sha,
                commit_message=commit.message,
                timestamp=commit.timestamp.isoformat(),
                author=commit.author
            )

            # Correlate commit with nearby Claude prompts
            link_ids = db.link_commit_to_prompts(commit_id, commit.timestamp)
            if link_ids:
                logger.info(f"  Linked to {len(link_ids)} prompt(s)")

            new_commit_count += 1

        if new_commit_count:
            logger.info(f"Ingested {new_commit_count} new commits")
        else:
            logger.info("No new commits")

        # --- Phase 2: Readiness check ---
        # Daily post cap
        posts_today = db.count_posts_today("x_thread")
        max_daily = config.polling.max_daily_posts
        if is_daily_cap_reached(posts_today, max_daily):
            logger.info(f"Daily post limit reached ({posts_today}/{max_daily}), waiting for tomorrow")
            db.set_last_poll_time(current_poll_time)
            update_monitoring("run-poll")
            logger.info("Done. No posts made.")
            return

        last_post_time = db.get_last_published_time("x_thread")
        now = datetime.now(timezone.utc)

        if last_post_time is None:
            # No posts ever — use a 24h lookback as baseline
            last_post_time = now - timedelta(hours=24)

        hours_since_post = (now - last_post_time).total_seconds() / 3600

        # Gather all commits since last post
        commits_since = db.get_commits_in_range(last_post_time, now)
        if not commits_since:
            logger.info(f"No commits since last post ({hours_since_post:.1f}h ago)")
            db.set_last_poll_time(current_poll_time)
            update_monitoring("run-poll")
            logger.info("Done. No posts made.")
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

        logger.info(f"Readiness: {accumulated_tokens} tokens (threshold: {threshold}), "
              f"{hours_since_post:.1f}h since last post (cap: {max_gap}h)")

        ready = check_readiness(accumulated_tokens, threshold, hours_since_post, max_gap, bool(prompts_since))

        if not ready:
            logger.info(f"Not ready — waiting for more material")
            db.set_last_poll_time(current_poll_time)
            update_monitoring("run-poll")
            logger.info("Done. No posts made.")
            return

        gap_exceeded = hours_since_post >= max_gap

        if gap_exceeded:
            logger.info(f"Time cap exceeded ({hours_since_post:.1f}h >= {max_gap}h), forcing pipeline")
        else:
            logger.info(f"Token threshold met ({accumulated_tokens} >= {threshold})")

        # --- Phase 3: Run pipeline with accumulated material ---
        if not prompts_since:
            logger.info(f"{len(commits_since)} commits but no related prompts found")
            db.set_last_poll_time(current_poll_time)
            update_monitoring("run-poll")
            logger.info("Done. No posts made.")
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

        logger.info(f"\nRunning pipeline: {len(commit_dicts)} commits, {len(prompt_text_list)} prompts, "
              f"{config.synthesis.num_candidates} candidates...")
        pipeline_result = pipeline.run(
            prompts=prompt_text_list,
            commits=commit_dicts,
            content_type="x_thread",
            threshold=config.synthesis.eval_threshold,
        )

        # Log pipeline stages
        best_idx = pipeline_result.comparison.ranking[0] if pipeline_result.comparison.ranking else 0
        logger.info(f"  Candidates generated: {len(pipeline_result.candidates)}")
        logger.info(f"  Best candidate: {chr(65 + best_idx)} (score: {pipeline_result.comparison.best_score:.1f}/10)")
        logger.info(f"  Groundedness: {pipeline_result.comparison.groundedness}/10")
        if pipeline_result.refinement:
            logger.info(f"  Refinement: picked {pipeline_result.refinement.picked} (score: {pipeline_result.refinement.final_score}/10)")
        logger.info(f"  Final score: {pipeline_result.final_score:.1f}/10")
        logger.info(f"  Content: {pipeline_result.final_content[:100]}...")

        # Store generated content
        content_id = db.insert_generated_content(
            content_type="x_thread",
            source_commits=[c["sha"] for c in commit_dicts],
            source_messages=prompt_uuids,
            content=pipeline_result.final_content,
            eval_score=pipeline_result.final_score,
            eval_feedback=pipeline_result.comparison.best_feedback,
            content_format=pipeline_result.content_format,
        )

        # Store knowledge lineage
        if pipeline_result.knowledge_ids and content_id:
            try:
                db.insert_content_knowledge_links(content_id, pipeline_result.knowledge_ids)
                logger.info(f"  Linked {len(pipeline_result.knowledge_ids)} knowledge items")
            except Exception as e:
                logger.warning(f"  Failed to store knowledge links: {e}")

        # Store engagement prediction if available
        if pipeline_result.engagement_prediction_detail and content_id:
            try:
                detail = pipeline_result.engagement_prediction_detail
                db.insert_prediction(
                    content_id=content_id,
                    predicted_score=detail["predicted_score"],
                    hook_strength=detail.get("hook_strength"),
                    specificity=detail.get("specificity"),
                    emotional_resonance=detail.get("emotional_resonance"),
                    novelty=detail.get("novelty"),
                    actionability=detail.get("actionability"),
                    prompt_version=detail.get("prompt_version", "v1"),
                )
                logger.info(f"  Stored engagement prediction: {detail['predicted_score']:.1f}")
            except Exception as e:
                logger.warning(f"Failed to store prediction (non-fatal): {e}")

        # Determine outcome and post if passes threshold
        passes = pipeline_result.final_score >= config.synthesis.eval_threshold * 10
        outcome = None
        rejection_reason = None

        if not pipeline_result.candidates:
            outcome = "all_filtered"
            rejection_reason = pipeline_result.comparison.reject_reason
        elif not passes:
            outcome = "below_threshold"
            rejection_reason = pipeline_result.comparison.reject_reason or (
                f"Score {pipeline_result.final_score:.1f} below threshold "
                f"{config.synthesis.eval_threshold * 10}"
            )
        elif passes:
            if rate_limited:
                logger.info("Rate limited, queued for later")
                outcome = "below_threshold"
                rejection_reason = "Rate limited, queued for retry"
            elif posted:
                logger.info("Already posted this cycle, queued for next")
                outcome = "below_threshold"
                rejection_reason = "Already posted this cycle"
            else:
                # Check if we should queue for optimal posting time
                should_queue = False
                if config.scheduling and config.scheduling.enabled:
                    from evaluation.posting_schedule import PostingScheduleAnalyzer
                    analyzer = PostingScheduleAnalyzer(db, min_samples=config.scheduling.min_samples)
                    current_hour = now.hour
                    current_dow = now.weekday()
                    should_queue = analyzer.should_queue(current_hour, current_dow)

                if should_queue:
                    # Queue for optimal time instead of posting now
                    from evaluation.posting_schedule import PostingScheduleAnalyzer
                    analyzer = PostingScheduleAnalyzer(db, min_samples=config.scheduling.min_samples)
                    next_slot = analyzer.next_optimal_slot(exclude_hours=2)
                    if next_slot:
                        db.queue_for_publishing(content_id, next_slot.isoformat(), platform='all')
                        logger.info(f"Queued for optimal time: {next_slot.isoformat()}")
                        outcome = "queued"
                        rejection_reason = None
                    else:
                        # No optimal slot found, post now as fallback
                        should_queue = False

                if not should_queue:
                    # Post immediately
                    logger.info("Posting thread to X...")
                    tweets = parse_thread_content(pipeline_result.final_content)
                    result = x_client.post_thread(tweets)
                    if result.success:
                        db.mark_published(content_id, result.url, tweet_id=result.tweet_id)
                        logger.info(f"Posted: {result.url}")
                        posted = True
                        outcome = "published"

                        # Cross-post to Bluesky if configured
                        if bluesky_client:
                            from output.cross_poster import CrossPoster
                            cross_poster = CrossPoster(bluesky_client=bluesky_client)
                            bsky_tweets = [cross_poster.adapt_for_bluesky(t, "x_thread") for t in tweets]
                            bsky_result = bluesky_client.post_thread(bsky_tweets)
                            if bsky_result.success:
                                db.mark_published_bluesky(content_id, bsky_result.uri)
                                logger.info(f"Cross-posted to Bluesky: {bsky_result.url}")
                            else:
                                logger.warning(f"Bluesky cross-post failed (non-fatal): {bsky_result.error}")
                    else:
                        logger.error(f"Post failed: {result.error}")
                        outcome = "below_threshold"
                        rejection_reason = f"Post failed: {result.error}"

        if outcome != "published":
            if pipeline_result.comparison.reject_reason:
                logger.warning(f"Rejected: {pipeline_result.comparison.reject_reason}")
            elif outcome == "below_threshold" and not rejection_reason:
                logger.warning("Below threshold, not posting")

        # Embed content for future semantic dedup
        if embedder and content_id:
            try:
                vectors = embedder.embed_batch([pipeline_result.final_content])
                if vectors:
                    db.set_content_embedding(content_id, serialize_embedding(vectors[0]))
            except Exception as e:
                logger.warning(f"Embedding failed (non-fatal): {e}")

        # Record pipeline run
        db.insert_pipeline_run(
            batch_id=pipeline_result.batch_id,
            content_type="x_thread",
            candidates_generated=len(pipeline_result.candidates),
            best_candidate_index=best_idx,
            best_score_before_refine=pipeline_result.comparison.best_score,
            best_score_after_refine=pipeline_result.refinement.final_score if pipeline_result.refinement else None,
            refinement_picked=pipeline_result.refinement.picked if pipeline_result.refinement else None,
            final_score=pipeline_result.final_score,
            content_id=content_id,
            outcome=outcome,
            rejection_reason=rejection_reason,
            filter_stats=pipeline_result.filter_stats,
        )

        # Update last poll time
        db.set_last_poll_time(current_poll_time)

    update_monitoring("run-poll")
    logger.info(f"Done. {'1 post made' if posted else 'No posts made'}.")


if __name__ == "__main__":
    main()
