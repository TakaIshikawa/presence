#!/usr/bin/env python3
"""Export a machine-readable archive manifest for sent newsletters."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_archive_manifest import (  # noqa: E402
    DEFAULT_DAYS,
    build_newsletter_archive_manifest,
    format_newsletter_archive_manifest_json,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to include by sent_at (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--issue-id",
        default=None,
        help="Export one newsletter issue_id instead of the filtered archive.",
    )
    parser.add_argument(
        "--output",
        help="Write JSON to this path instead of stdout.",
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
            manifest = build_newsletter_archive_manifest(
                db,
                days=args.days,
                issue_id=args.issue_id,
            )
        payload = format_newsletter_archive_manifest_json(manifest)
        if args.output:
            Path(args.output).write_text(payload + "\n")
        else:
            print(payload)
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
