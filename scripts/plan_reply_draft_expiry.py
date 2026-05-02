#!/usr/bin/env python3
"""Plan regeneration for stale pending reply drafts."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_draft_expiry import (  # noqa: E402
    DEFAULT_MAX_CONTEXT_AGE_HOURS,
    DEFAULT_MAX_DRAFT_AGE_HOURS,
    build_reply_draft_expiry_plan,
    format_reply_draft_expiry_json,
    format_reply_draft_expiry_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-draft-age-hours",
        type=float,
        default=DEFAULT_MAX_DRAFT_AGE_HOURS,
        help=(
            "Draft/source mention age threshold before regeneration "
            f"(default: {DEFAULT_MAX_DRAFT_AGE_HOURS:g})."
        ),
    )
    parser.add_argument(
        "--max-context-age-hours",
        type=float,
        default=DEFAULT_MAX_CONTEXT_AGE_HOURS,
        help=(
            "Context timestamp age threshold before rechecking context "
            f"(default: {DEFAULT_MAX_CONTEXT_AGE_HOURS:g})."
        ),
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--status-filter",
        action="append",
        default=None,
        help="Reply status to include; repeat for multiple statuses or use 'all'.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        with script_context() as (_config, db):
            plan = build_reply_draft_expiry_plan(
                db,
                max_draft_age_hours=args.max_draft_age_hours,
                max_context_age_hours=args.max_context_age_hours,
                status_filter=args.status_filter or ["pending"],
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_draft_expiry_json(plan))
    else:
        print(format_reply_draft_expiry_text(plan))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
