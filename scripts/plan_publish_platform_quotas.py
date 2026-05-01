#!/usr/bin/env python3
"""Plan deferrals for queued publish items that exceed platform daily quotas."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publish_platform_quotas import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    SUPPORTED_PLATFORMS,
    format_publish_platform_quotas_json,
    format_publish_platform_quotas_text,
    parse_quota_options,
    plan_publish_platform_quotas,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--platform",
        choices=("all", *SUPPORTED_PLATFORMS),
        default="all",
        help="Restrict quota planning to one platform (default: all).",
    )
    parser.add_argument(
        "--quota",
        action="append",
        default=[],
        metavar="PLATFORM=N",
        help="Daily quota for a platform; repeat for multiple platforms.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to scan from now (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum deferral suggestions to include (default: {DEFAULT_LIMIT}).",
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
        quotas = parse_quota_options(args.quota)
        with script_context() as (_config, db):
            report = plan_publish_platform_quotas(
                db,
                platform=args.platform,
                quotas=quotas,
                days=args.days,
                limit=args.limit,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_publish_platform_quotas_json(report))
    else:
        print(format_publish_platform_quotas_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
