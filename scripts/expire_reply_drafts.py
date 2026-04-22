#!/usr/bin/env python3
"""Expire pending reply drafts that are too old to review usefully."""

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
        replies_config = getattr(config, "replies", None)
        value = getattr(replies_config, "draft_ttl_hours", DEFAULT_DRAFT_TTL_HOURS)
    if not isinstance(value, int) or value <= 0:
        raise ValueError("replies.draft_ttl_hours must be a positive integer")
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Dismiss pending reply drafts older than replies.draft_ttl_hours."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="List drafts that would be dismissed without updating the database.",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="Dismiss expired drafts.",
    )
    parser.add_argument(
        "--ttl-hours",
        type=int,
        help="Override replies.draft_ttl_hours for this run.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    args = _build_parser().parse_args(argv)
    apply_changes = args.apply

    with script_context() as (config, db):
        ttl_hours = _draft_ttl_hours(config, args.ttl_hours)
        expired = db.get_expired_reply_drafts(ttl_hours)

        if not expired:
            logger.info("No pending reply drafts older than %d hours.", ttl_hours)
            return 0

        logger.info(
            "%d pending reply draft%s older than %d hours:",
            len(expired),
            "" if len(expired) == 1 else "s",
            ttl_hours,
        )
        for row in expired:
            platform = row.get("platform") or "x"
            logger.info(
                "  #%s [%s] @%s detected_at=%s inbound_id=%s",
                row["id"],
                platform,
                row.get("inbound_author_handle") or "?",
                row.get("detected_at"),
                row.get("inbound_tweet_id"),
            )

        if not apply_changes:
            logger.info("Dry run only. Re-run with --apply to dismiss these drafts.")
            return 0

        dismissed = db.dismiss_expired_reply_drafts(ttl_hours)
        logger.info("Dismissed %d expired reply draft%s.", dismissed, "" if dismissed == 1 else "s")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
