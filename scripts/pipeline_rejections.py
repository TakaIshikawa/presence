#!/usr/bin/env python3
"""Report normalized pipeline rejection causes."""

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.pipeline_rejections import (  # noqa: E402
    PipelineRejectionAnalytics,
    PipelineRejectionReport,
)
from runner import script_context  # noqa: E402


def format_text_report(report: PipelineRejectionReport) -> str:
    """Format a human-readable rejection taxonomy report."""
    content_type = report.content_type or "all"
    lines = [
        "",
        "=" * 70,
        f"Pipeline Rejections (last {report.days} days)",
        "=" * 70,
        "",
        f"Content type:  {content_type}",
        f"Total runs:    {report.total_runs}",
        f"Rejected runs: {report.rejected_runs}",
        "",
    ]

    if not report.categories:
        lines.append("No rejection categories found.")
    else:
        lines.append("Top Rejection Causes:")
        for category in report.categories:
            type_counts = ", ".join(
                f"{name}={count}"
                for name, count in sorted(category.content_types.items())
            )
            lines.append(f"  {category.category:30s} {category.count:5d}  {type_counts}")
            if category.raw_examples:
                lines.append(f"    e.g. {category.raw_examples[0]}")

    if report.parse_warnings:
        lines.append("")
        lines.append("Parse Warnings:")
        for warning in report.parse_warnings:
            run_label = warning.batch_id or warning.run_id or "unknown"
            lines.append(f"  {run_label}: {warning.message}")

    return "\n".join(lines).rstrip()


def format_json_report(report: PipelineRejectionReport) -> str:
    """Format a machine-readable rejection taxonomy report."""
    data = asdict(report)
    data["period_start"] = report.period_start.isoformat() if report.period_start else None
    data["period_end"] = report.period_end.isoformat()
    return json.dumps(data, indent=2)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report normalized pipeline rejection causes."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to look back (default: 30)",
    )
    parser.add_argument(
        "--content-type",
        default=None,
        help="Restrict to one content type, such as x_post or x_thread",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON instead of text",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=1,
        help="Minimum category count to include (default: 1)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv or [])
    with script_context() as (config, db):
        report = PipelineRejectionAnalytics(db).report(
            days=args.days,
            content_type=args.content_type,
            min_count=args.min_count,
        )

    if args.json:
        print(format_json_report(report))
    else:
        print(format_text_report(report))


if __name__ == "__main__":
    main(sys.argv[1:])
