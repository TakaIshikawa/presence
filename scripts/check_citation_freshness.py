#!/usr/bin/env python3
"""Check generated content knowledge citations for freshness and traceability."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.knowledge_citation_freshness_guard import (  # noqa: E402
    build_knowledge_citation_freshness_report,
    export_to_json,
    format_text_report,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--content-id",
        type=int,
        help="Limit the guard to one generated_content.id.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Maximum allowed knowledge source age in days (default: 90).",
    )
    parser.add_argument(
        "--allow-missing-canonical",
        action="store_true",
        help="Do not warn when linked knowledge lacks canonical URL metadata.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--fail-on-blocked",
        action="store_true",
        help="Exit nonzero when blocked citation links exist.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_knowledge_citation_freshness_report(
                db,
                content_id=args.content_id,
                days=args.days,
                require_canonical=not args.allow_missing_canonical,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(export_to_json(report))
    else:
        print(format_text_report(report))
    return 1 if args.fail_on_blocked and report.blocked_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
