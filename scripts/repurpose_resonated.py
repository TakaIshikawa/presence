#!/usr/bin/env python3
"""Create reviewable blog seed artifacts from resonated published posts."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring  # noqa: E402
from synthesis.post_mortem_repurposer import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MAX_AGE_DAYS,
    DEFAULT_MIN_ENGAGEMENT,
    PostMortemRepurposer,
    PostMortemRepurposerError,
    artifact_filename,
    artifact_to_dict,
    write_artifact,
)

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-engagement", type=float, default=DEFAULT_MIN_ENGAGEMENT)
    parser.add_argument("--max-age-days", type=int, default=DEFAULT_MAX_AGE_DAYS)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print selected artifacts as JSON without writing files or DB variants.",
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        help="Directory for JSON or markdown blog seed artifacts.",
    )
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="json",
        help="Artifact format to write when --artifact-dir is set.",
    )
    target_group = parser.add_mutually_exclusive_group()
    target_group.add_argument(
        "--content-idea",
        action="store_true",
        help="Persist a matching content idea seed in addition to the blog artifact.",
    )
    target_group.add_argument(
        "--planned-topic",
        action="store_true",
        help="Persist a matching planned topic seed in addition to the blog artifact.",
    )
    parser.add_argument(
        "--priority",
        choices=("high", "normal", "low"),
        default="normal",
        help="Priority for --content-idea seeds.",
    )
    parser.add_argument(
        "--target-date",
        help="Target date for --planned-topic seeds (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--topic",
        help="Override the topic used for the downstream seed record.",
    )
    parser.add_argument(
        "--angle",
        help="Optional angle used when creating a planned topic seed.",
    )
    parser.add_argument(
        "--campaign-id",
        type=int,
        help="Optional campaign ID for planned topic seeds.",
    )
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> list[dict]:
    """Select resonated posts and either dry-run or write review artifacts."""
    if not args.dry_run and args.artifact_dir is None:
        raise PostMortemRepurposerError("Use --dry-run or provide --artifact-dir")
    if args.planned_topic and not args.target_date:
        raise PostMortemRepurposerError("--target-date is required with --planned-topic")

    outcomes: list[dict] = []
    with script_context() as (_config, db):
        repurposer = PostMortemRepurposer(db)
        candidates = repurposer.find_eligible_posts(
            min_engagement=args.min_engagement,
            max_age_days=args.max_age_days,
            limit=args.limit,
        )
        if not candidates:
            logger.info("No resonated posts eligible for blog seed repurposing")
            return []

        for candidate in candidates:
            artifact = repurposer.build_seed(candidate)
            outcome = {
                "source_content_id": candidate.content_id,
                "title": artifact.title,
                "engagement_score": candidate.engagement_score,
                "dry_run": bool(args.dry_run),
                "artifact_path": None,
                "variant_id": None,
                "content_idea_id": None,
                "planned_topic_id": None,
            }

            if args.dry_run:
                logger.info(
                    "Dry run blog seed for #%s: %s",
                    candidate.content_id,
                    artifact.title,
                )
                outcomes.append({**outcome, "artifact": artifact_to_dict(artifact)})
                continue

            artifact_path = args.artifact_dir / artifact_filename(
                artifact,
                artifact_format=args.format,
            )
            write_artifact(artifact, artifact_path, artifact_format=args.format)
            variant_id = repurposer.record_seed_variant(artifact)
            logger.info("Wrote blog seed artifact: %s", artifact_path)

            if args.content_idea:
                idea_result = repurposer.record_content_idea(
                    artifact,
                    priority=args.priority,
                    topic=args.topic,
                )
                outcome["content_idea_id"] = idea_result.record_id
                outcome["content_idea_created"] = idea_result.created
                logger.info(
                    "Seeded content idea %s for #%s (%s)",
                    idea_result.record_id,
                    candidate.content_id,
                    "created" if idea_result.created else "existing",
                )
            elif args.planned_topic:
                planned_result = repurposer.record_planned_topic(
                    artifact,
                    target_date=args.target_date,
                    topic=args.topic,
                    angle=args.angle,
                    campaign_id=args.campaign_id,
                )
                outcome["planned_topic_id"] = planned_result.record_id
                outcome["planned_topic_created"] = planned_result.created
                logger.info(
                    "Seeded planned topic %s for #%s (%s)",
                    planned_result.record_id,
                    candidate.content_id,
                    "created" if planned_result.created else "existing",
                )

            outcomes.append(
                {
                    **outcome,
                    "artifact_path": str(artifact_path),
                    "variant_id": variant_id,
                    "artifact": artifact_to_dict(artifact),
                }
            )

    update_monitoring("repurpose-resonated")
    return outcomes


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    try:
        outcomes = run(args)
    except (PostMortemRepurposerError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(json.dumps(outcomes, indent=2, sort_keys=True))
    if not args.dry_run:
        for outcome in outcomes:
            print(
                f"Blog seed artifact: {outcome['artifact_path']} "
                f"(variant {outcome['variant_id']})",
                file=sys.stderr,
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
