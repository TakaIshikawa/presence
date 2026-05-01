#!/usr/bin/env python3
"""Plan retry order for paused or quarantined curated sources."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_recovery import (  # noqa: E402
    build_source_recovery_plan,
    export_to_json,
    format_text_report,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stale-days",
        type=int,
        default=7,
        help="Minimum days since last failure before retry (default: 7).",
    )
    parser.add_argument(
        "--max-failures",
        type=int,
        default=5,
        help="Maximum consecutive failures eligible for retry (default: 5).",
    )
    parser.add_argument(
        "--source-type",
        choices=("x_account", "blog", "newsletter"),
        help="Limit recovery planning to one source type.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum paused or quarantined sources to inspect (default: 50).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            plan = build_source_recovery_plan(
                db,
                stale_days=args.stale_days,
                max_failures=args.max_failures,
                source_type=args.source_type,
                limit=args.limit,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(export_to_json(plan))
    else:
        print(format_text_report(plan))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
