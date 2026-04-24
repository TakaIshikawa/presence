#!/usr/bin/env python3
"""Export reply quality calibration reports."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.reply_quality_calibration import (  # noqa: E402
    DEFAULT_THRESHOLD,
    ReplyQualityCalibrator,
    build_reply_quality_calibration_report,
    format_reply_quality_calibration_json,
    format_reply_quality_calibration_markdown,
    format_text_report,
)
from runner import script_context  # noqa: E402


logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare reply_queue quality signals against operator review outcomes "
            "and export either the legacy calibration report or the score-threshold "
            "analysis view."
        )
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Look back this many days (default: 30).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output the legacy calibration report as stable machine-readable JSON.",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=5,
        help="Minimum samples needed before recommending threshold action (default: 5).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the rendered output to this file instead of stdout.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help=f"Evaluator pass threshold to calibrate (default: {DEFAULT_THRESHOLD:.1f}).",
    )
    parser.add_argument(
        "--format",
        choices=["markdown", "text", "json"],
        default="markdown",
        help="Output format for threshold analysis (default: markdown).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.days <= 0:
        raise ValueError("--days must be positive")
    if args.min_samples <= 0:
        raise ValueError("--min-samples must be positive")
    if args.threshold < 0 or args.threshold > 10:
        raise ValueError("--threshold must be between 0 and 10")

    selected_format = "json" if args.json else args.format
    logging.basicConfig(
        level=logging.WARNING if selected_format == "json" else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        if selected_format == "text":
            logger.info(
                "Building reply quality threshold analysis for %d days at threshold %.1f.",
                args.days,
                args.threshold,
            )
            rendered = _render_threshold_report(
                db,
                days=args.days,
                threshold=args.threshold,
                output_format=selected_format,
            )
        else:
            logger.info(
                "Building reply quality calibration report for %d days with min_samples=%d.",
                args.days,
                args.min_samples,
            )
            report = ReplyQualityCalibrator(db).build_report(
                days=args.days,
                min_samples=args.min_samples,
            )
            rendered = (
                format_reply_quality_calibration_json(report)
                if selected_format == "json"
                else format_reply_quality_calibration_markdown(report)
            )

    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0


def _render_threshold_report(db, *, days: int, threshold: float, output_format: str) -> str:
    report = build_reply_quality_calibration_report(
        db,
        days=days,
        threshold=threshold,
    )
    if output_format == "json":
        return json.dumps(report, indent=2, sort_keys=True)
    return format_text_report(report)


if __name__ == "__main__":
    raise SystemExit(main())
