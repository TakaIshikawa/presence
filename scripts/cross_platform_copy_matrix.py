#!/usr/bin/env python3
"""Report selected copy variants across campaign platforms."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.cross_platform_copy_matrix import (  # noqa: E402
    DEFAULT_PLATFORMS,
    build_cross_platform_copy_matrix_report,
    format_cross_platform_copy_matrix_json,
    format_cross_platform_copy_matrix_markdown,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--campaign",
        help="Content campaign ID or exact campaign name to report.",
    )
    parser.add_argument(
        "--content-id",
        type=int,
        action="append",
        default=None,
        help="Generated content ID to include; repeat for multiple IDs.",
    )
    parser.add_argument(
        "--platform",
        action="append",
        default=None,
        help=(
            "Platform column to include; repeat for multiple platforms or use 'all' "
            f"(default: {', '.join(DEFAULT_PLATFORMS)})."
        ),
    )
    parser.add_argument(
        "--format",
        choices=["markdown", "json"],
        default="markdown",
        help="Output format (default: markdown).",
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
            report = build_cross_platform_copy_matrix_report(
                db,
                campaign=args.campaign,
                content_ids=args.content_id,
                platforms=args.platform or DEFAULT_PLATFORMS,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_cross_platform_copy_matrix_json(report))
    else:
        print(format_cross_platform_copy_matrix_markdown(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
