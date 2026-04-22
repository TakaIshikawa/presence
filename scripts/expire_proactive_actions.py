#!/usr/bin/env python3
"""Expire pending proactive engagement drafts that are too old to review."""

import argparse
import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context

logger = logging.getLogger(__name__)

DEFAULT_DRAFT_TTL_HOURS = 48


def _draft_ttl_hours(config, override: int | None = None) -> int:
    if override is not None:
        value = override
    else:
        proactive_config = getattr(config, "proactive", None)
        value = getattr(proactive_config, "draft_ttl_hours", DEFAULT_DRAFT_TTL_HOURS)
    if not isinstance(value, int) or value <= 0:
        raise ValueError("proactive.draft_ttl_hours must be a positive integer")
    return value


def _limit(value: int | None) -> int | None:
    if value is not None and value <= 0:
        raise ValueError("--limit must be a positive integer")
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Dismiss pending proactive drafts older than proactive.draft_ttl_hours."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List drafts that would be dismissed without updating the database.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of expired drafts to dismiss.",
    )
    parser.add_argument(
        "--ttl-hours",
        type=int,
        help="Override proactive.draft_ttl_hours for this run.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    args = _build_parser().parse_args(argv)

    with script_context() as (config, db):
        ttl_hours = _draft_ttl_hours(config, args.ttl_hours)
        limit = _limit(args.limit)
        expired = db.get_expired_proactive_drafts(ttl_hours, limit=limit)

        if not expired:
            logger.info("No pending proactive drafts older than %d hours.", ttl_hours)
            return 0

        logger.info(
            "%d pending proactive draft%s older than %d hours:",
            len(expired),
            "" if len(expired) == 1 else "s",
            ttl_hours,
        )
        for row in expired:
            logger.info(
                "  #%s [%s] @%s created_at=%s target_id=%s",
                row["id"],
                row.get("action_type") or "reply",
                row.get("target_author_handle") or "?",
                row.get("created_at"),
                row.get("target_tweet_id"),
            )

        if args.dry_run:
            logger.info("Dry run only. Re-run without --dry-run to dismiss these drafts.")
            return 0

        dismissed = db.dismiss_expired_proactive_drafts(ttl_hours, limit=limit)
        logger.info(
            "Dismissed %d expired proactive draft%s.",
            dismissed,
            "" if dismissed == 1 else "s",
        )
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
