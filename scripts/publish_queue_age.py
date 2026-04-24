#!/usr/bin/env python3
"""Report aging and stale items in the publish queue."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publish_queue_age import build_publish_queue_age_report
from runner import script_context


def _shorten(value: Any, width: int) -> str:
    if value is None:
        return "-"
    text = str(value).replace("\n", " ")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _format_item(item: dict | None) -> str:
    if item is None:
        return "-"
    return (
        f"queue={item['queue_id']} content={item['content_id']} "
        f"type={item['content_type']} platform={item['platform']} "
        f"status={item['status']} scheduled={item['scheduled_at']} "
        f"age={item['age_hours']}h"
    )


def format_age_report(report: dict) -> str:
    """Format the queue aging report for terminal review."""
    lines = [
        f"Publish queue age report generated at {report['generated_at']}",
        f"Stale threshold: {report['stale_after_hours']}h",
        f"Total queued/failed items: {report['total']}",
        f"Oldest item: {_format_item(report['oldest_item'])}",
        "",
    ]

    if not report["platforms"]:
        lines.append("No queued or failed publish queue items found.")
        return "\n".join(lines)

    bucket_names = ["future", "0-1h", "1-6h", "6-24h", "24-72h", "72h+"]
    header = ["PLATFORM", "TOTAL", "QUEUED", "FAILED", *bucket_names]
    widths = [10, 5, 6, 6, 6, 5, 5, 6, 6, 5]
    lines.append("  ".join(label.ljust(width) for label, width in zip(header, widths)))
    lines.append("  ".join("-" * width for width in widths))
    for platform, data in sorted(report["platforms"].items()):
        values = [
            platform,
            data["total"],
            data["statuses"]["queued"],
            data["statuses"]["failed"],
            *[data["age_buckets"][bucket] for bucket in bucket_names],
        ]
        lines.append(
            "  ".join(str(value).ljust(width) for value, width in zip(values, widths))
        )

    lines.extend(["", "Stale items:"])
    if not report["stale_items"]:
        lines.append("No stale queued or failed items found.")
        return "\n".join(lines)

    columns = [
        ("queue_id", "QUEUE", 5),
        ("content_id", "CID", 5),
        ("content_type", "TYPE", 12),
        ("platform", "PLATFORM", 8),
        ("status", "STATUS", 7),
        ("scheduled_at", "SCHEDULED", 25),
        ("age_hours", "AGE_H", 7),
        ("retry_state", "RETRY_STATE", 36),
    ]
    lines.append("  ".join(label.ljust(width) for _, label, width in columns))
    lines.append("  ".join("-" * width for _, _, width in columns))
    for item in report["stale_items"]:
        rendered = dict(item)
        rendered["retry_state"] = json.dumps(item["retry_state"], sort_keys=True)
        lines.append(
            "  ".join(
                _shorten(rendered.get(key), width).ljust(width)
                for key, _, width in columns
            )
        )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stale-after-hours",
        type=float,
        default=24.0,
        help="Flag items stale after this many hours past scheduled time (default: 24)",
    )
    parser.add_argument(
        "--platform",
        choices=["x", "bluesky", "all"],
        help="Filter by queued platform target",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of a human-readable report",
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
            report = build_publish_queue_age_report(
                db,
                stale_after_hours=args.stale_after_hours,
                platform=args.platform,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(format_age_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
