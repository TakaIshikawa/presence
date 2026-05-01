#!/usr/bin/env python3
"""Report curated knowledge sources whose trust is decaying."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_trust_decay import (  # noqa: E402
    build_source_trust_decay_report,
    format_json_report,
    format_text_report,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Freshness and citation lookback window in days (default: 90).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum sources to include after decay ranking.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--include-healthy",
        action="store_true",
        help="Include sources that do not currently need attention.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_source_trust_decay_report(
                db,
                days=args.days,
                limit=args.limit,
                include_healthy=args.include_healthy,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_json_report(report))
    else:
        print(format_text_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
