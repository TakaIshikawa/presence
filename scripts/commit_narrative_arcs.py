#!/usr/bin/env python3
"""Plan narrative arcs from recent commits and Claude sessions."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.commit_narrative_arcs import (  # noqa: E402
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_MIN_ITEMS_PER_ARC,
    build_commit_narrative_arcs,
    format_commit_narrative_arcs_json,
    format_commit_narrative_arcs_text,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"Recent activity window to consider (default: {DEFAULT_LOOKBACK_DAYS}).",
    )
    parser.add_argument(
        "--min-items",
        type=int,
        default=DEFAULT_MIN_ITEMS_PER_ARC,
        help=(
            "Minimum commit/session source items required for an arc "
            f"(default: {DEFAULT_MIN_ITEMS_PER_ARC})."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of arcs to print.",
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
            plan = build_commit_narrative_arcs(
                db,
                lookback_days=args.days,
                min_items_per_arc=args.min_items,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_commit_narrative_arcs_json(plan, limit=args.limit))
    else:
        print(format_commit_narrative_arcs_text(plan, limit=args.limit))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
