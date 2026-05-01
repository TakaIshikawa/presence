#!/usr/bin/env python3
"""Select old high-performing content for safe read-only recirculation."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.content_recirculation import (  # noqa: E402
    DEFAULT_DAYS_OLD,
    DEFAULT_LIMIT,
    DEFAULT_LOOKBACK_DAYS,
    build_content_recirculation_report,
    format_content_recirculation_json,
    format_content_recirculation_text,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days-old",
        type=int,
        default=DEFAULT_DAYS_OLD,
        help=f"Minimum age in days since publication (default: {DEFAULT_DAYS_OLD}).",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=(
            "Recent publication/reuse window to suppress recirculation "
            f"(default: {DEFAULT_LOOKBACK_DAYS})."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum candidates to return (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output deterministic JSON instead of text.",
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
            report = build_content_recirculation_report(
                db,
                days_old=args.days_old,
                lookback_days=args.lookback_days,
                limit=args.limit,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_content_recirculation_json(report))
    else:
        print(format_content_recirculation_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
