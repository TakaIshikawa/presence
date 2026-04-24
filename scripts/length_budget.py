#!/usr/bin/env python3
"""Report copy length budgets for generated content and stored variants."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.length_budget import (  # noqa: E402
    LengthBudgetRecordNotFound,
    budget_report_to_json,
    build_length_budget_report,
    format_length_budget_report,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--content-id", type=int, help="generated_content id to inspect")
    target.add_argument("--queue-id", type=int, help="publish_queue id to inspect")
    parser.add_argument(
        "--platform",
        default="all",
        help="Filter to one platform: x, bluesky, linkedin, newsletter, blog, or all",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of text",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    with script_context() as (_config, db):
        try:
            report = build_length_budget_report(
                db,
                content_id=args.content_id,
                queue_id=args.queue_id,
                platform=args.platform,
            )
        except (LengthBudgetRecordNotFound, ValueError) as exc:
            print(str(exc), file=sys.stderr)
            return 1

    print(budget_report_to_json(report) if args.json else format_length_budget_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
