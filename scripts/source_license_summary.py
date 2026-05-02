#!/usr/bin/env python3
"""Summarize curated source license and reuse posture."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_license_summary import (  # noqa: E402
    DEFAULT_STALE_AFTER_DAYS,
    build_source_license_summary_report,
    format_source_license_summary_json,
    format_source_license_summary_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-type",
        help="Only include one curated source type, such as x_account, blog, or newsletter.",
    )
    parser.add_argument(
        "--stale-after-days",
        type=_positive_int,
        default=DEFAULT_STALE_AFTER_DAYS,
        help=f"Mark license checks stale after this many days (default: {DEFAULT_STALE_AFTER_DAYS}).",
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
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        with script_context() as (_config, db):
            report = build_source_license_summary_report(
                db,
                source_type=args.source_type,
                stale_after_days=args.stale_after_days,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_source_license_summary_json(report))
    else:
        print(format_source_license_summary_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
