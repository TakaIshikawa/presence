#!/usr/bin/env python3
"""Report failed and held publish queue items by diagnosis."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publish_failure_triage import build_publish_failure_triage
from runner import script_context


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _shorten(value: Any, width: int) -> str:
    if value is None:
        return "-"
    text = str(value).replace("\n", " ")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def format_triage_json(report: dict) -> str:
    """Format triage report as stable machine-readable JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_triage_table(report: dict) -> str:
    """Format triage groups as a compact operator table."""
    groups = report["groups"]
    if not groups:
        return "No failed or held publish queue items found."

    columns = [
        ("platform", "PLATFORM", 8),
        ("category", "CATEGORY", 10),
        ("recommended_action", "ACTION", 18),
        ("count", "COUNT", 5),
        ("queue_ids", "QUEUE_IDS", 18),
        ("sample_errors", "SAMPLE_ERROR", 32),
        ("recommendation", "RECOMMENDATION", 54),
    ]
    lines = [
        "  ".join(label.ljust(width) for _, label, width in columns),
        "  ".join("-" * width for _, _, width in columns),
    ]
    for group in groups:
        row = {
            **group,
            "queue_ids": ",".join(str(value) for value in group["queue_ids"]),
            "sample_errors": group["sample_errors"][0]
            if group["sample_errors"]
            else None,
        }
        lines.append(
            "  ".join(
                _shorten(row.get(key), width).ljust(width)
                for key, _, width in columns
            )
        )

    if report.get("include_content"):
        lines.extend(["", _format_item_table(report["items"])])
    return "\n".join(lines)


def _format_item_table(items: list[dict]) -> str:
    columns = [
        ("queue_id", "QID", 5),
        ("content_id", "CID", 5),
        ("platform", "PLATFORM", 8),
        ("status", "STATUS", 7),
        ("category", "CATEGORY", 10),
        ("recommended_action", "ACTION", 18),
        ("content", "CONTENT", 48),
    ]
    lines = [
        "  ".join(label.ljust(width) for _, label, width in columns),
        "  ".join("-" * width for _, _, width in columns),
    ]
    for item in items:
        lines.append(
            "  ".join(
                _shorten(item.get(key), width).ljust(width)
                for key, _, width in columns
            )
        )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=30,
        help="Number of days to look back (default: 30)",
    )
    parser.add_argument(
        "--platform",
        choices=["all", "x", "bluesky"],
        default="all",
        help="Platform to include (default: all)",
    )
    parser.add_argument(
        "--status",
        choices=["failed", "held"],
        help="Filter by blocked queue status",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON",
    )
    parser.add_argument(
        "--include-content",
        action="store_true",
        help="Include generated content in item details",
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
            report = build_publish_failure_triage(
                db,
                days=args.days,
                platform=args.platform,
                status=args.status,
                include_content=args.include_content,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_triage_json(report))
    else:
        print(format_triage_table(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
