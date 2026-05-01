#!/usr/bin/env python3
"""Report open content ideas whose snoozes are due for review."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.content_idea_snooze_digest import (  # noqa: E402
    DEFAULT_DAYS_AHEAD,
    DEFAULT_LIMIT,
    build_content_idea_snooze_digest,
    format_content_idea_snooze_digest_json,
    format_content_idea_snooze_digest_text,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days-ahead",
        type=int,
        default=DEFAULT_DAYS_AHEAD,
        help=(
            "Include snoozes expiring within this many days "
            f"(default: {DEFAULT_DAYS_AHEAD})"
        ),
    )
    parser.add_argument(
        "--include-unsnoozed",
        action="store_true",
        help="Also include open ideas with no snoozed_until value.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum ideas to return (default: {DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text)",
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
            report = build_content_idea_snooze_digest(
                db,
                days_ahead=args.days_ahead,
                include_unsnoozed=args.include_unsnoozed,
                limit=args.limit,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_content_idea_snooze_digest_json(report))
    else:
        print(format_content_idea_snooze_digest_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
