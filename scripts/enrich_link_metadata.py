#!/usr/bin/env python3
"""Enrich stored curated link metadata."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.link_metadata_enricher import (  # noqa: E402
    DEFAULT_LIMIT,
    SOURCE_TYPES,
    enrich_link_metadata,
    format_link_metadata_enrichment_json,
    format_link_metadata_enrichment_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-type",
        choices=SOURCE_TYPES,
        default="all",
        help="Rows to enrich (default: all)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum URLs to fetch (default: {DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Update missing metadata instead of reporting a dry run",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="HTTP fetch timeout in seconds (default: 10)",
    )
    parser.add_argument(
        "--format",
        choices=["json", "text"],
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
            report = enrich_link_metadata(
                db,
                source_type=args.source_type,
                limit=args.limit,
                apply=args.apply,
                timeout=args.timeout,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_link_metadata_enrichment_json(report))
    else:
        print(format_link_metadata_enrichment_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
