#!/usr/bin/env python3
"""Generate and optionally post a long-form essay-style X post via multi-stage pipeline."""

import argparse
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from ingestion.claude_logs import ClaudeLogParser
from synthesis.pipeline import SynthesisPipeline
from output.x_client import XClient
from knowledge.embeddings import VoyageEmbeddings, serialize_embedding
from knowledge.store import KnowledgeStore

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate and store the long-form post without publishing to X or Bluesky.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    with script_context() as (config, db):
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
            claim_check_enabled=config.synthesis.claim_check_enabled,
            restricted_prompt_behavior=getattr(
                config.curated_sources, "restricted_prompt_behavior", "strict"
            ) if config.curated_sources else "strict",
        )
        x_client = XClient(
            config.x.api_key,
            config.x.api_secret,
            config.x.access_token,
            config.x.access_token_secret,
        )

        # Initialize Bluesky client if configured
        bluesky_client = None
        if config.bluesky and config.bluesky.enabled:
            from output.bluesky_client import BlueskyClient
            bluesky_client = BlueskyClient(
                config.bluesky.handle,
                config.bluesky.app_password,
            )

        # Get today's date range (UTC)
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today + timedelta(days=1)

        logger.info(f"Generating long-form post for {today.date()}")

        # Get today's commits
        commits = db.get_commits_in_range(today, tomorrow)
        if not commits:
            logger.info("No commits today, skipping long-form post")
            return

        logger.info(f"Found {len(commits)} commits")

        # Get today's Claude prompts
        parser = ClaudeLogParser(config.paths.claude_logs, config.paths.allowed_projects)
        prompts = [
            msg for msg in parser.parse_global_history()
            if today <= msg.timestamp < tomorrow
        ]
        parser.log_skipped_project_counts("long_post")
        prompt_texts = [p.prompt_text for p in prompts]

        logger.info(f"Found {len(prompts)} prompts")

        if not prompt_texts:
            logger.info("No prompts found, skipping long-form post")
            return

        # Convert commit dicts
        commit_dicts = [
            {"repo_name": c.get("repo_name", ""), "message": c.get("commit_message", ""),
             "sha": c.get("commit_sha", "")}
            for c in commits
        ]

        # Inject historical context if configured
        if config.historical and config.historical.enabled:
            from synthesis.theme_selector import ThemeSelector
            theme_selector = ThemeSelector(db)
            if theme_selector.should_inject("x_long_post", config.historical.injection_frequency):
                ctx = theme_selector.select(
                    commit_dicts, "x_long_post",
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
            content_type="x_long_post",
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
            content_type="x_long_post",
            source_commits=[c["sha"] for c in commit_dicts],
            source_messages=[p.message_uuid for p in prompts],
            content=result.final_content,
            eval_score=result.final_score,
            eval_feedback=result.comparison.best_feedback,
            content_format=result.content_format,
        )
        if result.planned_topic_id and content_id:
            db.mark_planned_topic_generated(result.planned_topic_id, content_id)
            logger.info(f"  Linked planned topic {result.planned_topic_id}")

        # Store knowledge lineage
        if result.knowledge_ids and content_id:
            try:
                db.insert_content_knowledge_links(content_id, result.knowledge_ids)
                logger.info(f"  Linked {len(result.knowledge_ids)} knowledge items")
            except Exception as e:
                logger.warning(f"  Failed to store knowledge links: {e}")

        # Embed content for future semantic dedup
        if embedder and content_id:
            try:
                vectors = embedder.embed_batch([result.final_content])
                if vectors:
                    db.set_content_embedding(content_id, serialize_embedding(vectors[0]))
            except Exception as e:
                logger.warning(f"Embedding failed (non-fatal): {e}")

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
        elif args.dry_run:
            outcome = "dry_run"
            logger.info("Dry run enabled, skipping publish")
            logger.info("Generated long-form post:")
            logger.info(result.final_content)
        else:
            # Post as single long-form tweet
            logger.info("Posting long-form post to X...")
            post_result = x_client.post(result.final_content)
            if post_result.success:
                db.mark_published(content_id, post_result.url, tweet_id=post_result.tweet_id)
                logger.info(f"Posted: {post_result.url}")
                outcome = "published"

                # Cross-post to Bluesky if configured
                if bluesky_client:
                    from output.cross_poster import CrossPoster
                    cross_poster = CrossPoster(bluesky_client=bluesky_client)
                    bsky_text = cross_poster.adapt_for_bluesky(result.final_content, "x_long_post")
                    bsky_result = bluesky_client.post(bsky_text)
                    if bsky_result.success:
                        db.mark_published_bluesky(content_id, bsky_result.uri)
                        logger.info(f"Cross-posted to Bluesky: {bsky_result.url}")
                    else:
                        logger.warning(f"Bluesky cross-post failed (non-fatal): {bsky_result.error}")
            else:
                logger.error(f"Post failed: {post_result.error}")
                outcome = "below_threshold"
                rejection_reason = f"Post failed: {post_result.error}"

        # Record pipeline run
        db.insert_pipeline_run(
            batch_id=result.batch_id,
            content_type="x_long_post",
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

    update_monitoring("run-long-post")
    logger.info("Done")


if __name__ == "__main__":
    main()
