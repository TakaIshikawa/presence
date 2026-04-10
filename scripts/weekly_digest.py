#!/usr/bin/env python3
"""Generate and publish weekly blog post via multi-stage pipeline."""

import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from ingestion.claude_logs import ClaudeLogParser
from synthesis.pipeline import SynthesisPipeline
from output.blog_writer import BlogWriter
from knowledge.embeddings import VoyageEmbeddings, serialize_embedding

logger = logging.getLogger(__name__)


def main():
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

        pipeline = SynthesisPipeline(
            api_key=config.anthropic.api_key,
            generator_model=config.synthesis.model,
            evaluator_model=config.synthesis.eval_model,
            db=db,
            num_candidates=config.synthesis.num_candidates,
            anthropic_timeout=config.timeouts.anthropic_seconds,
            embedder=embedder,
            semantic_threshold=semantic_threshold,
        )
        blog_writer = BlogWriter(config.paths.static_site)

        # Get this week's date range (last 7 days, UTC)
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        week_ago = today - timedelta(days=7)

        logger.info(f"Generating weekly digest for {week_ago.date()} to {today.date()}")

        # Get this week's commits
        commits = db.get_commits_in_range(week_ago, today)
        if not commits:
            logger.info("No commits this week, skipping digest")
            return

        logger.info(f"Found {len(commits)} commits")

        # Get this week's Claude prompts
        parser = ClaudeLogParser(config.paths.claude_logs)
        prompts = [
            msg for msg in parser.parse_global_history()
            if week_ago <= msg.timestamp < today
        ]
        prompt_texts = [p.prompt_text for p in prompts]

        logger.info(f"Found {len(prompts)} prompts")

        if not prompt_texts:
            logger.info("No prompts found, skipping digest")
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
            if theme_selector.should_inject("blog_post", config.historical.injection_frequency):
                ctx = theme_selector.select(
                    commit_dicts, "blog_post",
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
            content_type="blog_post",
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
            content_type="blog_post",
            source_commits=[c["sha"] for c in commit_dicts],
            source_messages=[p.message_uuid for p in prompts],
            content=result.final_content,
            eval_score=result.final_score,
            eval_feedback=result.comparison.best_feedback,
        )

        # Embed content for future semantic dedup
        if embedder and content_id:
            try:
                vectors = embedder.embed_batch([result.final_content])
                if vectors:
                    db.set_content_embedding(content_id, serialize_embedding(vectors[0]))
            except Exception as e:
                logger.warning(f"Embedding failed (non-fatal): {e}")

        # Determine outcome and publish if passes threshold
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
                logger.warning("Below threshold, not publishing")
            logger.debug("Generated content:")
            logger.debug(result.final_content[:500] + "...")
        else:
            logger.info("Writing blog post...")
            write_result = blog_writer.write_post(result.final_content)

            if write_result.success:
                logger.info(f"Blog post written: {write_result.file_path}")

                # Commit and push
                logger.info("Committing and pushing...")
                title = result.final_content.split("\n")[0].replace("TITLE:", "").strip()
                if blog_writer.commit_and_push(title):
                    db.mark_published(content_id, write_result.url)
                    logger.info(f"Published: {write_result.url}")
                    outcome = "published"
                else:
                    logger.error("Git push failed")
                    outcome = "below_threshold"
                    rejection_reason = "Git push failed"
            else:
                logger.error(f"Write failed: {write_result.error}")
                outcome = "below_threshold"
                rejection_reason = f"Write failed: {write_result.error}"

        # Record pipeline run
        db.insert_pipeline_run(
            batch_id=result.batch_id,
            content_type="blog_post",
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

    update_monitoring("run-weekly")
    logger.info("Done")


if __name__ == "__main__":
    main()
