#!/usr/bin/env python3
"""Export reply quality calibration reports."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.reply_quality_calibration import (  # noqa: E402
    ReplyQualityCalibrator,
    format_reply_quality_calibration_json,
    format_reply_quality_calibration_markdown,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Look back this many days (default: 30)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output stable machine-readable JSON instead of Markdown",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=5,
        help="Minimum samples needed before recommending threshold action (default: 5)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the report to this file instead of stdout",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.days <= 0:
        raise ValueError("--days must be positive")
    if args.min_samples <= 0:
        raise ValueError("--min-samples must be positive")

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        report = ReplyQualityCalibrator(db).build_report(
            days=args.days,
            min_samples=args.min_samples,
        )

    rendered = (
        format_reply_quality_calibration_json(report)
        if args.json
        else format_reply_quality_calibration_markdown(report)
    )
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
