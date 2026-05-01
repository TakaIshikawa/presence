#!/usr/bin/env python3
"""Route pending inbound reply drafts into deterministic handling lanes."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_routing import (  # noqa: E402
    apply_reply_routes,
    build_reply_routing_report,
    format_json_report,
    format_text_report,
)
from runner import script_context  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of routed rows to include after urgency ordering.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--min-urgency",
        type=int,
        help="Only include routes with urgency greater than or equal to this value.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist route metadata only when compatible storage already exists.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive")
    if args.min_urgency is not None and args.min_urgency < 0:
        parser.error("--min-urgency must be non-negative")

    logging.basicConfig(
        level=logging.WARNING if args.format == "json" else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        report = build_reply_routing_report(
            db,
            limit=args.limit,
            min_urgency=args.min_urgency,
        )
        if args.apply:
            report["apply"] = apply_reply_routes(db, report["items"])

    if args.format == "json":
        print(format_json_report(report))
    else:
        print(format_text_report(report))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
