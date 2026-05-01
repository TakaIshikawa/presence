#!/usr/bin/env python3
"""Report content variant A/B outcomes."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_variant_outcomes import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_SAMPLE,
    build_content_variant_outcome_report,
    format_content_variant_outcome_json,
    format_content_variant_outcome_text,
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
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for variants (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--platform",
        help="Only include variants for one platform.",
    )
    parser.add_argument(
        "--variant-type",
        help="Only include variants of one type.",
    )
    parser.add_argument(
        "--min-sample",
        type=_positive_int,
        default=DEFAULT_MIN_SAMPLE,
        help=(
            "Minimum variants in a platform/type/selection group before outcome "
            f"recommendations are made (default: {DEFAULT_MIN_SAMPLE})."
        ),
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
            report = build_content_variant_outcome_report(
                db,
                days=args.days,
                platform=args.platform,
                variant_type=args.variant_type,
                min_sample=args.min_sample,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_content_variant_outcome_json(report))
    else:
        print(format_content_variant_outcome_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
