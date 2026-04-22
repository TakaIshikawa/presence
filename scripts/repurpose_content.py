#!/usr/bin/env python3
"""Repurpose high-performing posts into different content formats."""

import argparse
import sys
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from synthesis.repurposer import ContentRepurposer
from synthesis.evaluator_v2 import CrossModelEvaluator
from output.blog_writer import BlogWriter
from output.platform_adapter import LinkedInPlatformAdapter, count_graphemes
from output.x_client import XClient, parse_thread_content

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Repurpose content or refresh platform-specific text variants.",
    )
    parser.add_argument(
        "--linkedin-variants",
        action="store_true",
        help="Create or refresh durable LinkedIn variants for existing generated content.",
    )
    parser.add_argument(
        "--content-id",
        type=int,
        help="Refresh the LinkedIn variant for one generated_content id.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum generated content rows to refresh when --content-id is omitted.",
    )
    if argv is None:
        args, _unknown = parser.parse_known_args(argv)
        return args
    return parser.parse_args(argv)


def _source_text_for_linkedin(row: dict) -> str:
    content = row.get("content") or ""
    if row.get("content_type") == "x_thread":
        return "\n\n".join(parse_thread_content(content))
    return content


def refresh_linkedin_variants(db, content_id: int | None = None, limit: int = 50) -> int:
    """Create or refresh LinkedIn text variants for generated content."""
    if content_id is not None:
        row = db.get_generated_content(content_id)
        rows = [row] if row else []
    else:
        rows = db.list_generated_content_for_variant_refresh(limit=limit)

    adapter = LinkedInPlatformAdapter()
    refreshed = 0

    for row in rows:
        variant = adapter.adapt(_source_text_for_linkedin(row), row.get("content_type", "x_post"))
        db.upsert_content_variant(
            content_id=row["id"],
            platform="linkedin",
            variant_type="post",
            content=variant,
            metadata={
                "source_content_type": row.get("content_type"),
                "adapter": "LinkedInPlatformAdapter",
                "graphemes": count_graphemes(variant),
            },
        )
        refreshed += 1

    return refreshed


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    args = parse_args(argv)

    with script_context() as (config, db):
        if args.linkedin_variants:
            refreshed = refresh_linkedin_variants(
                db,
                content_id=args.content_id,
                limit=args.limit,
            )
            logger.info(f"Refreshed {refreshed} LinkedIn content variants")
            update_monitoring("repurpose")
            return

        # Check if we've already hit the daily post cap
        daily_posts = db.count_posts_today("x_thread")
        daily_cap = getattr(config.synthesis, "daily_post_cap", 3)
        if daily_posts >= daily_cap:
            logger.info(f"Already posted {daily_posts} threads today (cap: {daily_cap}), skipping repurpose")
            return

        # Initialize repurposer
        repurposer = ContentRepurposer(
            api_key=config.anthropic.api_key,
            model=config.synthesis.model,
            db=db,
            timeout=config.timeouts.anthropic_seconds,
        )

        # Find candidates
        candidates = repurposer.find_candidates(
            min_engagement=10.0,
            max_age_days=14,
        )

        if not candidates:
            logger.info("No repurpose candidates found")
            update_monitoring("repurpose")
            return

        logger.info(f"Found {len(candidates)} repurpose candidates")

        # Take the top candidate
        candidate = candidates[0]
        logger.info(
            f"Repurposing content #{candidate.content_id} "
            f"(engagement: {candidate.engagement_score}, type: {candidate.original_type} -> {candidate.target_type})"
        )

        # Generate repurposed content based on target type
        if candidate.target_type == "x_thread":
            result = repurposer.expand_post_to_thread(candidate)
        elif candidate.target_type == "blog_seed":
            result = repurposer.expand_to_blog_seed(candidate)
        else:
            logger.error(f"Unsupported target type: {candidate.target_type}")
            return

        logger.info("Generated repurposed content, evaluating...")

        # Evaluate the repurposed content
        evaluator = CrossModelEvaluator(
            api_key=config.anthropic.api_key,
            model=config.synthesis.eval_model,
            timeout=config.timeouts.anthropic_seconds,
        )

        # Get reference examples and calibration data
        reference_examples = [p["content"] for p in db.get_top_performing_posts(limit=3)]
        classified = db.get_all_classified_posts()
        engagement_stats = db.get_engagement_calibration_stats()

        # Evaluate as a single candidate
        comparison = evaluator.evaluate(
            candidates=[result.content],
            source_prompts=[f"Repurposed from content #{result.source_id}"],
            source_commits=[],
            reference_examples=reference_examples,
            calibration_resonated=classified.get("resonated", []),
            calibration_low_resonance=classified.get("low_resonance", []),
            engagement_stats=engagement_stats,
        )

        logger.info(f"Evaluation score: {comparison.best_score:.1f}/10")

        # Store the repurposed content
        content_id = db.insert_repurposed_content(
            content_type=result.target_type,
            source_content_id=result.source_id,
            content=result.content,
            eval_score=comparison.best_score,
            eval_feedback=comparison.best_feedback,
        )

        # Determine if we should publish
        threshold = config.synthesis.eval_threshold * 10
        passes = comparison.best_score >= threshold and comparison.reject_reason is None

        if not passes:
            if comparison.reject_reason:
                logger.warning(f"Rejected: {comparison.reject_reason}")
            else:
                logger.warning(f"Below threshold ({comparison.best_score:.1f} < {threshold}), not posting")
            logger.debug(f"Generated content:\n{result.content}")
            update_monitoring("repurpose")
            return

        # Only post threads (blog_seed is for later manual use)
        if result.target_type == "x_thread":
            # Check daily cap again
            daily_posts = db.count_posts_today("x_thread")
            if daily_posts >= daily_cap:
                logger.info(f"Daily cap reached ({daily_posts}/{daily_cap}), storing for later")
                update_monitoring("repurpose")
                return

            logger.info("Posting repurposed thread to X...")
            x_client = XClient(
                config.x.api_key,
                config.x.api_secret,
                config.x.access_token,
                config.x.access_token_secret,
            )

            tweets = parse_thread_content(result.content)
            post_result = x_client.post_thread(tweets)

            if post_result.success:
                db.mark_published(content_id, post_result.url, tweet_id=post_result.tweet_id)
                logger.info(f"Posted repurposed thread: {post_result.url}")
                logger.info(f"Lineage: Repurposed from content #{result.source_id} (engagement: {candidate.engagement_score})")
            else:
                logger.error(f"Post failed: {post_result.error}")
        else:
            blog_writer = BlogWriter(config.paths.static_site)
            draft_result = blog_writer.write_draft(
                result.content,
                source_content_id=result.source_id,
                generated_content_id=content_id,
            )
            if draft_result.success:
                logger.info(
                    f"Stored {result.target_type} for manual review "
                    f"(content_id: {content_id}, draft: {draft_result.file_path})"
                )
            else:
                logger.error(f"Draft write failed: {draft_result.error}")

    update_monitoring("repurpose")
    logger.info("Done")


if __name__ == "__main__":
    main(sys.argv[1:])
