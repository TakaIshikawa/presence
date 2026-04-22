#!/usr/bin/env python3
"""Generate and optionally post a visual X post (text + image) via visual pipeline."""

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
from synthesis.image_generator import ImageGenerator
from synthesis.visual_pipeline import VisualPipeline
from output.x_client import XClient
from knowledge.embeddings import VoyageEmbeddings, serialize_embedding
from knowledge.store import KnowledgeStore

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate and store the visual post without publishing to X.",
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

        # Image generator
        provider = "pillow"
        if config.image_gen:
            provider = config.image_gen.provider
        output_dir = config.image_gen.output_dir if config.image_gen else None
        image_generator = ImageGenerator(
            provider=provider,
            output_dir=output_dir,
        )

        visual_pipeline = VisualPipeline(
            synthesis_pipeline=pipeline,
            image_generator=image_generator,
            api_key=config.anthropic.api_key,
            model=config.synthesis.model,
            timeout=config.timeouts.anthropic_seconds,
        )

        x_client = XClient(
            config.x.api_key,
            config.x.api_secret,
            config.x.access_token,
            config.x.access_token_secret,
        )

        # Get today's date range (UTC)
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today + timedelta(days=1)

        logger.info(f"Generating visual post for {today.date()}")

        # Get today's commits
        commits = db.get_commits_in_range(today, tomorrow)
        if not commits:
            logger.info("No commits today, skipping visual post")
            return

        logger.info(f"Found {len(commits)} commits")

        # Get today's Claude prompts
        parser = ClaudeLogParser(config.paths.claude_logs, config.paths.allowed_projects)
        prompts = [
            msg for msg in parser.parse_global_history()
            if today <= msg.timestamp < tomorrow
        ]
        parser.log_skipped_project_counts("visual_post")
        prompt_texts = [p.prompt_text for p in prompts]

        logger.info(f"Found {len(prompts)} prompts")

        if not prompt_texts:
            logger.info("No prompts found, skipping visual post")
            return

        # Convert commit dicts
        commit_dicts = [
            {"repo_name": c.get("repo_name", ""), "message": c.get("commit_message", ""),
             "sha": c.get("commit_sha", "")}
            for c in commits
        ]

        # Run visual pipeline
        logger.info(f"Running visual pipeline: {len(commits)} commits...")
        result = visual_pipeline.run(
            prompts=prompt_texts,
            commits=commit_dicts,
            threshold=config.synthesis.eval_threshold,
        )

        if not result:
            logger.warning("Visual pipeline returned no result")
            return

        pr = result.pipeline_result
        best_idx = pr.comparison.ranking[0] if pr.comparison.ranking else 0
        logger.info(f"  Best candidate: {chr(65 + best_idx)} (score: {pr.comparison.best_score}/10)")
        logger.info(f"  Final score: {pr.final_score}/10")
        logger.info(f"  Image: {result.image.style} at {result.image.path}")

        # Store
        content_id = db.insert_generated_content(
            content_type="x_visual",
            source_commits=[c["sha"] for c in commit_dicts],
            source_messages=[p.message_uuid for p in prompts],
            content=pr.final_content,
            eval_score=pr.final_score,
            eval_feedback=pr.comparison.best_feedback,
            content_format=pr.content_format,
            image_path=result.image.path,
            image_prompt=result.image_prompt,
        )
        if pr.planned_topic_id and content_id:
            db.mark_planned_topic_generated(pr.planned_topic_id, content_id)
            logger.info(f"  Linked planned topic {pr.planned_topic_id}")

        # Embed content for future semantic dedup
        if embedder and content_id:
            try:
                vectors = embedder.embed_batch([pr.final_content])
                if vectors:
                    db.set_content_embedding(content_id, serialize_embedding(vectors[0]))
            except Exception as e:
                logger.warning(f"Embedding failed (non-fatal): {e}")

        # Determine outcome
        passes = pr.final_score >= config.synthesis.eval_threshold * 10
        outcome = None
        rejection_reason = None

        if not pr.candidates:
            outcome = "all_filtered"
            rejection_reason = pr.comparison.reject_reason
        elif not passes:
            outcome = "below_threshold"
            rejection_reason = pr.comparison.reject_reason or (
                f"Score {pr.final_score:.1f} below threshold "
                f"{config.synthesis.eval_threshold * 10}"
            )
            logger.warning(f"Below threshold: {rejection_reason}")
        elif not result.image.path:
            outcome = "below_threshold"
            rejection_reason = "Image generation failed"
            logger.warning("No image generated, skipping post")
        elif args.dry_run:
            outcome = "dry_run"
            logger.info("Dry run enabled, skipping publish")
            logger.info("Generated visual post:")
            logger.info(pr.final_content)
            logger.info(f"Generated image saved to: {result.image.path}")
        else:
            # Post with media
            logger.info("Posting visual post to X...")
            post_result = x_client.post_with_media(
                text=pr.final_content,
                media_path=result.image.path,
                alt_text=result.image.prompt_used,
            )
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
            batch_id=pr.batch_id,
            content_type="x_visual",
            candidates_generated=len(pr.candidates),
            best_candidate_index=best_idx,
            best_score_before_refine=pr.comparison.best_score,
            best_score_after_refine=pr.refinement.final_score if pr.refinement else None,
            refinement_picked=pr.refinement.picked if pr.refinement else None,
            final_score=pr.final_score,
            content_id=content_id,
            outcome=outcome,
            rejection_reason=rejection_reason,
            filter_stats=pr.filter_stats,
        )

    update_monitoring("run-visual-post")
    logger.info("Done")


if __name__ == "__main__":
    main()
