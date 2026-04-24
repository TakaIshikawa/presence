#!/usr/bin/env python3
"""Print a remediation digest for failed publication attempts."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_failures import (  # noqa: E402
    build_publication_failure_digest,
    format_publication_failure_digest,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to look back (default: 7)",
    )
    parser.add_argument(
        "--platform",
        choices=["all", "x", "bluesky"],
        default="all",
        help="Platform to include (default: all)",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--include-queued",
        action="store_true",
        help="Include queued rows in addition to failed/retrying rows",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        summary = build_publication_failure_digest(
            db,
            days=args.days,
            platform=args.platform,
            include_queued=args.include_queued,
        )

    if args.format == "json":
        print(json.dumps(summary, indent=2))
    else:
        print(format_publication_failure_digest(summary))


if __name__ == "__main__":
    main()
