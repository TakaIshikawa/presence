#!/usr/bin/env python3
"""Backfill missing canonical publication URLs."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publication_url_backfill import (  # noqa: E402
    PublicationAccountHandles,
    backfill_publication_urls,
    format_backfill_table,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--platform",
        default="all",
        choices=["all", "x", "bluesky"],
        help="Platform to backfill (default: all)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to look back (default: 30)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report updates without writing to the database",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON",
    )
    return parser.parse_args(argv)


def _handles_from_config(config) -> PublicationAccountHandles:
    x_username = (
        getattr(config.x, "username", "")
        or getattr(config.github, "username", "")
    )
    bluesky_handle = config.bluesky.handle if config.bluesky else ""
    return PublicationAccountHandles(
        x_username=x_username,
        bluesky_handle=bluesky_handle,
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (config, db):
        report = backfill_publication_urls(
            db,
            handles=_handles_from_config(config),
            days=args.days,
            platform=args.platform,
            dry_run=args.dry_run,
        )

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(format_backfill_table(report))


if __name__ == "__main__":
    main()
